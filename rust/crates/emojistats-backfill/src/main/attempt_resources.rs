use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

use emojistats_backfill::{
    archive::{ArchiveCommitContext, ArchiveStorageConfig},
    ledger::{HostOverride, RepoLedgerEntry},
    parse::ParseConfig,
    scheduler::{ClaimScope, SharedHostPacer},
    transport::FetchByteBudget,
};
use tokio::sync::Semaphore;

use super::{
    super::{
        cli::HttpProtocol,
        failure::FetchOneFailure,
        fleet::{
            DEFAULT_HOST_CONCURRENCY_CAP, HostConcurrencyLimiter, HostConcurrencyPermit,
            SharedBlockingLedger,
        },
    },
    archive_host::{ArchiveClaimCheck, PreparedFetchHost},
};

pub struct LocalFetchOneAttemptConfig<'a> {
    pub(crate) did_str: &'a str,
    pub(crate) spool_dir: PathBuf,
    pub(crate) max_bytes: u64,
    pub(crate) archive_dir: PathBuf,
    pub(crate) archive_context: ArchiveCommitContext,
    pub(crate) archive_storage: ArchiveStorageConfig,
    pub(crate) parse_config: ParseConfig,
    pub(crate) http_protocol: HttpProtocol,
}

pub struct FetchOneAttemptConfig<'a> {
    pub(crate) did_str: &'a str,
    pub(crate) spool_dir: PathBuf,
    pub(crate) max_bytes: u64,
    pub(crate) archive_dir: PathBuf,
    pub(crate) archive_context: ArchiveCommitContext,
    pub(crate) archive_storage: ArchiveStorageConfig,
    pub(crate) resources: AttemptResources<'a>,
    pub(crate) parse_config: ParseConfig,
    pub(crate) http_protocol: HttpProtocol,
}

pub enum AttemptResources<'a> {
    Local {
        claim_scope: ClaimScope,
    },
    Fleet {
        host_pacer: SharedHostPacer,
        host_limiter: HostConcurrencyLimiter,
        parse_permits: Arc<Semaphore>,
        byte_budget: FetchByteBudget,
        ledger: SharedBlockingLedger,
        claimed: Box<RepoLedgerEntry>,
        claim_scope: &'a ClaimScope,
        host_override_cache: HostOverrideCache,
    },
}

impl AttemptResources<'_> {
    pub(crate) const fn claim_scope(&self) -> &ClaimScope {
        match self {
            Self::Local { claim_scope } => claim_scope,
            Self::Fleet { claim_scope, .. } => claim_scope,
        }
    }

    pub(crate) fn shared_ledger(&self) -> Option<SharedBlockingLedger> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { ledger, .. } => Some(ledger.clone()),
        }
    }

    pub(crate) fn host_override_cache(&self) -> Option<HostOverrideCache> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet {
                host_override_cache,
                ..
            } => Some(host_override_cache.clone()),
        }
    }

    pub(crate) const fn host_pacer(&self) -> Option<&SharedHostPacer> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { host_pacer, .. } => Some(host_pacer),
        }
    }

    pub(crate) const fn parse_permits(&self) -> Option<&Arc<Semaphore>> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { parse_permits, .. } => Some(parse_permits),
        }
    }

    pub(crate) fn byte_budget(&self) -> Option<FetchByteBudget> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { byte_budget, .. } => Some(byte_budget.clone()),
        }
    }

    pub(crate) fn archive_claim_check(&self) -> Option<ArchiveClaimCheck> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet {
                claimed, ledger, ..
            } => Some(ArchiveClaimCheck {
                ledger: ledger.clone(),
                claimed: (**claimed).clone(),
            }),
        }
    }

    const fn host_limiter(&self) -> Option<&HostConcurrencyLimiter> {
        match self {
            Self::Local { .. } => None,
            Self::Fleet { host_limiter, .. } => Some(host_limiter),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HostOverrideCache {
    pub(crate) entries: Arc<Mutex<HashMap<String, HostOverrideCacheEntry>>>,
}

#[derive(Debug, Clone)]
pub struct HostOverrideCacheEntry {
    pub(crate) loaded_at: Instant,
    pub(crate) value: Option<HostOverride>,
}

pub async fn acquire_host_fetch_permit(
    resources: &AttemptResources<'_>,
    prepared_host: &PreparedFetchHost,
) -> Result<Option<HostConcurrencyPermit>, FetchOneFailure> {
    let Some(limiter) = resources.host_limiter() else {
        return Ok(None);
    };
    limiter
        .acquire(
            prepared_host.host.as_str(),
            prepared_host
                .host_override
                .as_ref()
                .and_then(|override_record| override_record.concurrency_cap)
                .or(Some(DEFAULT_HOST_CONCURRENCY_CAP)),
        )
        .await
}
