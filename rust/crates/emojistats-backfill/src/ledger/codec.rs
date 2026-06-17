use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{OptionalExtension, Transaction, params};

use super::{
    AttemptId, ForcedFetchMode, HostOverride, LedgerStoreError, RepoLedgerEntry, RepoLedgerStatus,
};
use crate::transport::AccountState;

pub(super) struct StoredStatus {
    pub(super) status: &'static str,
    pub(super) terminal_account_state: Option<&'static str>,
}

impl From<&RepoLedgerStatus> for StoredStatus {
    fn from(status: &RepoLedgerStatus) -> Self {
        match status {
            RepoLedgerStatus::Pending => Self {
                status: "pending",
                terminal_account_state: None,
            },
            RepoLedgerStatus::Claimed => Self {
                status: "claimed",
                terminal_account_state: None,
            },
            RepoLedgerStatus::Succeeded => Self {
                status: "succeeded",
                terminal_account_state: None,
            },
            RepoLedgerStatus::RetryableFailure => Self {
                status: "retryable_failure",
                terminal_account_state: None,
            },
            RepoLedgerStatus::Throttled => Self {
                status: "throttled",
                terminal_account_state: None,
            },
            RepoLedgerStatus::OperatorDeferred => Self {
                status: "operator_deferred",
                terminal_account_state: None,
            },
            RepoLedgerStatus::ResourceLimited => Self {
                status: "resource_limited",
                terminal_account_state: None,
            },
            RepoLedgerStatus::TerminalAccount(state) => Self {
                status: "terminal_account",
                terminal_account_state: Some(account_state_name(*state)),
            },
            RepoLedgerStatus::PermanentFailure => Self {
                status: "permanent_failure",
                terminal_account_state: None,
            },
        }
    }
}

pub(super) fn row_to_entry(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<RepoLedgerEntry, LedgerStoreError>> {
    let status: String = row.get(1)?;
    let terminal_account_state: Option<String> = row.get(2)?;
    let attempts: i64 = row.get(3)?;
    let next_attempt_after_ms: Option<i64> = row.get(4)?;
    let last_attempt_run_id: Option<String> = row.get(5)?;
    let last_attempt_did: Option<String> = row.get(6)?;
    let last_attempt_sequence: Option<i64> = row.get(7)?;
    let worker_id: Option<String> = row.get(9)?;
    let claimed_at_ms: Option<i64> = row.get(10)?;
    let lease_until_ms: Option<i64> = row.get(11)?;

    Ok(build_entry(
        row.get(0)?,
        &status,
        terminal_account_state.as_deref(),
        attempts,
        next_attempt_after_ms,
        last_attempt_run_id,
        last_attempt_did,
        last_attempt_sequence,
        row.get(8)?,
        worker_id,
        claimed_at_ms,
        lease_until_ms,
    ))
}

pub(super) fn load_entry_in_transaction(
    transaction: &Transaction<'_>,
    did: &str,
) -> Result<Option<RepoLedgerEntry>, LedgerStoreError> {
    transaction
        .query_row(
            "
            SELECT
                did,
                status,
                terminal_account_state,
                attempts,
                next_attempt_after_ms,
                last_attempt_run_id,
                last_attempt_did,
                last_attempt_sequence,
                last_error,
                worker_id,
                claimed_at_ms,
                lease_until_ms
            FROM repo_ledger
            WHERE did = ?1
            ",
            params![did],
            row_to_entry,
        )
        .optional()
        .map_err(Into::into)
        .and_then(Option::transpose)
}

pub(super) fn update_entry_if_owned(
    transaction: &Transaction<'_>,
    entry: &RepoLedgerEntry,
    worker_id: &str,
    attempt: &AttemptId,
) -> Result<usize, LedgerStoreError> {
    let status = StoredStatus::from(&entry.status);
    let next_attempt_after_ms = optional_time_to_millis(entry.next_attempt_after)?;
    let last_attempt_sequence = entry
        .last_attempt
        .as_ref()
        .map(|attempt| i64::try_from(attempt.sequence))
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let claimed_at_ms = optional_time_to_millis(entry.claimed_at)?;
    let lease_until_ms = optional_time_to_millis(entry.lease_until)?;
    let owned_attempt_sequence =
        i64::try_from(attempt.sequence).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    transaction
        .execute(
            "
            UPDATE repo_ledger
            SET
                status = ?2,
                terminal_account_state = ?3,
                attempts = ?4,
                next_attempt_after_ms = ?5,
                last_attempt_run_id = ?6,
                last_attempt_did = ?7,
                last_attempt_sequence = ?8,
                last_error = ?9,
                worker_id = ?10,
                claimed_at_ms = ?11,
                lease_until_ms = ?12
            WHERE
                did = ?1
                AND status = 'claimed'
                AND worker_id = ?13
                AND last_attempt_run_id = ?14
                AND last_attempt_did = ?15
                AND last_attempt_sequence = ?16
            ",
            params![
                entry.did.as_str(),
                status.status,
                status.terminal_account_state,
                i64::from(entry.attempts),
                next_attempt_after_ms,
                entry
                    .last_attempt
                    .as_ref()
                    .map(|attempt| attempt.run_id.as_str()),
                entry
                    .last_attempt
                    .as_ref()
                    .map(|attempt| attempt.did.as_str()),
                last_attempt_sequence,
                entry.last_error.as_deref(),
                entry.worker_id.as_deref(),
                claimed_at_ms,
                lease_until_ms,
                worker_id,
                attempt.run_id.as_str(),
                attempt.did.as_str(),
                owned_attempt_sequence,
            ],
        )
        .map_err(Into::into)
}

pub(super) fn update_expired_claim(
    transaction: &Transaction<'_>,
    entry: &RepoLedgerEntry,
    now: SystemTime,
) -> Result<usize, LedgerStoreError> {
    let status = StoredStatus::from(&entry.status);
    let next_attempt_after_ms = optional_time_to_millis(entry.next_attempt_after)?;
    let last_attempt_sequence = entry
        .last_attempt
        .as_ref()
        .map(|attempt| i64::try_from(attempt.sequence))
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let claimed_at_ms = optional_time_to_millis(entry.claimed_at)?;
    let lease_until_ms = optional_time_to_millis(entry.lease_until)?;
    transaction
        .execute(
            "
            UPDATE repo_ledger
            SET
                status = ?2,
                terminal_account_state = ?3,
                attempts = ?4,
                next_attempt_after_ms = ?5,
                last_attempt_run_id = ?6,
                last_attempt_did = ?7,
                last_attempt_sequence = ?8,
                last_error = ?9,
                worker_id = ?10,
                claimed_at_ms = ?11,
                lease_until_ms = ?12
            WHERE
                did = ?1
                AND status = 'claimed'
                AND lease_until_ms IS NOT NULL
                AND lease_until_ms <= ?13
            ",
            params![
                entry.did.as_str(),
                status.status,
                status.terminal_account_state,
                i64::from(entry.attempts),
                next_attempt_after_ms,
                entry
                    .last_attempt
                    .as_ref()
                    .map(|attempt| attempt.run_id.as_str()),
                entry
                    .last_attempt
                    .as_ref()
                    .map(|attempt| attempt.did.as_str()),
                last_attempt_sequence,
                entry.last_error.as_deref(),
                entry.worker_id.as_deref(),
                claimed_at_ms,
                lease_until_ms,
                time_to_millis(now)?,
            ],
        )
        .map_err(Into::into)
}

pub(super) fn row_to_host_override(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<Result<HostOverride, LedgerStoreError>> {
    let host: String = row.get(0)?;
    let disabled: i64 = row.get(1)?;
    let concurrency_cap: Option<i64> = row.get(2)?;
    let min_interval_ms: Option<i64> = row.get(3)?;
    let revive_after_ms: Option<i64> = row.get(4)?;
    let force_mode: Option<String> = row.get(5)?;
    let never_diff: i64 = row.get(6)?;

    Ok(build_host_override(
        host,
        disabled,
        concurrency_cap,
        min_interval_ms,
        revive_after_ms,
        force_mode,
        never_diff,
    ))
}

fn build_host_override(
    host: String,
    disabled: i64,
    concurrency_cap: Option<i64>,
    min_interval_ms: Option<i64>,
    revive_after_ms: Option<i64>,
    force_mode: Option<String>,
    never_diff: i64,
) -> Result<HostOverride, LedgerStoreError> {
    let disabled = match disabled {
        0 => false,
        1 => true,
        value => return Err(LedgerStoreError::InvalidHostOverrideDisabled { value }),
    };
    let never_diff = match never_diff {
        0 => false,
        1 => true,
        value => return Err(LedgerStoreError::InvalidHostOverrideDisabled { value }),
    };
    let concurrency_cap = concurrency_cap
        .map(u32::try_from)
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let min_interval = min_interval_ms
        .map(u64::try_from)
        .transpose()
        .map_err(|_err| LedgerStoreError::IntegerOverflow)?
        .map(Duration::from_millis);
    let revive_after = revive_after_ms.map(time_from_millis).transpose()?;
    let force_mode = force_mode.map(|mode| parse_force_mode(&mode)).transpose()?;
    let record = HostOverride {
        host,
        disabled,
        concurrency_cap,
        min_interval,
        revive_after,
        force_mode,
        never_diff,
    };
    validate_host_override(&record)?;
    Ok(record)
}

#[allow(clippy::too_many_arguments)]
fn build_entry(
    did: String,
    status: &str,
    terminal_account_state: Option<&str>,
    attempts: i64,
    next_attempt_after_ms: Option<i64>,
    last_attempt_run_id: Option<String>,
    last_attempt_did: Option<String>,
    last_attempt_sequence: Option<i64>,
    last_error: Option<String>,
    worker_id: Option<String>,
    claimed_at_ms: Option<i64>,
    lease_until_ms: Option<i64>,
) -> Result<RepoLedgerEntry, LedgerStoreError> {
    let status = parse_status(status, terminal_account_state)?;
    let attempts = u32::try_from(attempts).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    let next_attempt_after = next_attempt_after_ms.map(time_from_millis).transpose()?;
    let last_attempt = match (last_attempt_run_id, last_attempt_did, last_attempt_sequence) {
        (None, None, None) => None,
        (Some(run_id), Some(did), Some(sequence)) => Some(AttemptId {
            run_id,
            did,
            sequence: u64::try_from(sequence).map_err(|_err| LedgerStoreError::IntegerOverflow)?,
        }),
        _ => return Err(LedgerStoreError::InconsistentAttemptIdentity),
    };

    Ok(RepoLedgerEntry {
        did,
        status,
        attempts,
        next_attempt_after,
        last_attempt,
        last_error,
        worker_id,
        claimed_at: claimed_at_ms.map(time_from_millis).transpose()?,
        lease_until: lease_until_ms.map(time_from_millis).transpose()?,
    })
}

fn parse_status(
    status: &str,
    terminal_account_state: Option<&str>,
) -> Result<RepoLedgerStatus, LedgerStoreError> {
    let parsed = match status {
        "pending" => RepoLedgerStatus::Pending,
        "claimed" => RepoLedgerStatus::Claimed,
        "succeeded" => RepoLedgerStatus::Succeeded,
        "retryable_failure" => RepoLedgerStatus::RetryableFailure,
        "throttled" => RepoLedgerStatus::Throttled,
        "operator_deferred" => RepoLedgerStatus::OperatorDeferred,
        "resource_limited" => RepoLedgerStatus::ResourceLimited,
        "terminal_account" => {
            let state =
                terminal_account_state.ok_or(LedgerStoreError::InconsistentTerminalStatus)?;
            RepoLedgerStatus::TerminalAccount(parse_account_state(state)?)
        }
        "permanent_failure" => RepoLedgerStatus::PermanentFailure,
        _ => {
            return Err(LedgerStoreError::UnknownStatus {
                status: status.to_owned(),
            });
        }
    };
    if !matches!(parsed, RepoLedgerStatus::TerminalAccount(_)) && terminal_account_state.is_some() {
        return Err(LedgerStoreError::InconsistentTerminalStatus);
    }
    Ok(parsed)
}

fn parse_account_state(state: &str) -> Result<AccountState, LedgerStoreError> {
    match state {
        "RepoNotFound" => Ok(AccountState::RepoNotFound),
        "RepoTakendown" => Ok(AccountState::RepoTakendown),
        "RepoSuspended" => Ok(AccountState::RepoSuspended),
        "RepoDeactivated" => Ok(AccountState::RepoDeactivated),
        _ => Err(LedgerStoreError::InvalidTerminalAccountState {
            state: state.to_owned(),
        }),
    }
}

const fn account_state_name(state: AccountState) -> &'static str {
    match state {
        AccountState::RepoNotFound => "RepoNotFound",
        AccountState::RepoTakendown => "RepoTakendown",
        AccountState::RepoSuspended => "RepoSuspended",
        AccountState::RepoDeactivated => "RepoDeactivated",
    }
}

pub(super) const fn force_mode_name(mode: ForcedFetchMode) -> &'static str {
    match mode {
        ForcedFetchMode::GetRepo => "get_repo",
        ForcedFetchMode::ListRecords => "list_records",
    }
}

fn parse_force_mode(mode: &str) -> Result<ForcedFetchMode, LedgerStoreError> {
    match mode {
        "get_repo" => Ok(ForcedFetchMode::GetRepo),
        "list_records" => Ok(ForcedFetchMode::ListRecords),
        _ => Err(LedgerStoreError::InvalidForcedFetchMode {
            mode: mode.to_owned(),
        }),
    }
}

pub(super) fn validate_host_override(record: &HostOverride) -> Result<(), LedgerStoreError> {
    if record.host.trim().is_empty() {
        return Err(LedgerStoreError::InvalidHostOverride {
            message: "host must not be blank".to_owned(),
        });
    }
    if record.concurrency_cap == Some(0) {
        return Err(LedgerStoreError::InvalidHostOverride {
            message: "concurrency cap must be greater than zero".to_owned(),
        });
    }
    if record.min_interval == Some(Duration::ZERO) {
        return Err(LedgerStoreError::InvalidHostOverride {
            message: "min interval must be greater than zero".to_owned(),
        });
    }
    Ok(())
}

pub(super) const fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

pub(super) fn shard_bucket_to_i64(bucket: u64) -> Result<i64, LedgerStoreError> {
    i64::try_from(bucket).map_err(|_err| LedgerStoreError::IntegerOverflow)
}

pub(super) fn optional_time_to_millis(
    time: Option<SystemTime>,
) -> Result<Option<i64>, LedgerStoreError> {
    time.map(time_to_millis).transpose()
}

pub(super) fn optional_duration_to_millis(
    duration: Option<Duration>,
) -> Result<Option<i64>, LedgerStoreError> {
    duration
        .map(|value| {
            i64::try_from(value.as_millis()).map_err(|_err| LedgerStoreError::IntegerOverflow)
        })
        .transpose()
}

pub(super) fn time_to_millis(time: SystemTime) -> Result<i64, LedgerStoreError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_err| LedgerStoreError::TimeBeforeUnixEpoch)?;
    let millis = duration
        .as_secs()
        .checked_mul(1_000)
        .and_then(|seconds| seconds.checked_add(u64::from(duration.subsec_millis())))
        .ok_or(LedgerStoreError::IntegerOverflow)?;
    i64::try_from(millis).map_err(|_err| LedgerStoreError::IntegerOverflow)
}

fn time_from_millis(millis: i64) -> Result<SystemTime, LedgerStoreError> {
    let millis = u64::try_from(millis).map_err(|_err| LedgerStoreError::IntegerOverflow)?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or(LedgerStoreError::IntegerOverflow)
}
