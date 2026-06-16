use super::{
    super::{
        ArchiveCommitContext, ArchiveError, AttemptOutcome, ClaimScope, CompletenessClass,
        FetchMethod, FetchOneFailure, ForcedFetchMode, HOST_OVERRIDE_CACHE_TTL, HostOverride,
        HostPacer, Instant, NormalizerVersion, ParseConfig, ParseVisitError, ParsedRepoSummary,
        Path, SharedHostPacer, SqliteLedger, StreamingArchiveSink, StreamingReceiptInput,
        SystemTime, Uri, archive_row_from_owned_post_observed_at, classify_archive_error,
        classify_parse_error, elapsed_ms, hash_profile_record, parse_repo_for_did_with_state,
        retryable_failure,
    },
    fetch_attempt::{
        GetRepoProcessed, GetRepoTimings, HostOverrideCache, HostOverrideCacheEntry, ProcessedRepo,
        ProcessedRepoArtifacts, ProcessedRepoCounts,
    },
};

struct ArchiveRunState {
    sink: StreamingArchiveSink,
    archive_row_ns: u128,
    sink_push_ns: u128,
    profiled_posts: u64,
}

pub fn parse_and_archive_spooled_repo(
    did_str: &str,
    car_path: &Path,
    archive_dir: &Path,
    archive_context: ArchiveCommitContext,
    parse_config: ParseConfig,
) -> Result<ProcessedRepo, FetchOneFailure> {
    let parse_started = Instant::now();
    let sink = StreamingArchiveSink::new(archive_dir, did_str, archive_context).map_err(|err| {
        classify_archive_error(&format!("open streaming archive sink for {did_str}"), &err)
    })?;

    let normalizer = sink.normalizer().clone();
    let did = did_str.to_owned();
    let state = ArchiveRunState {
        sink,
        archive_row_ns: 0,
        sink_push_ns: 0,
        profiled_posts: 0,
    };
    let (parsed, state) = if std::env::var_os("EMOJISTATS_PROFILE_STAGES").is_some() {
        parse_repo_streaming_archive_profiled(
            car_path,
            did_str,
            parse_config,
            state,
            did,
            normalizer,
        )
    } else {
        parse_repo_streaming_archive_unprofiled(
            car_path,
            did_str,
            parse_config,
            state,
            did,
            normalizer,
        )
    }
    .map_err(|err| match err {
        ParseVisitError::Parse(err) => classify_parse_error(did_str, &err),
        ParseVisitError::Visit(err) => {
            classify_archive_error(&format!("stream archive row for {did_str}"), &err)
        }
    })?;
    let parse_ms = elapsed_ms(parse_started);
    let sink = state.sink;
    let archive_started = Instant::now();
    let profile_row_hash = hash_profile_record(parsed.profile.as_ref())
        .map_err(|err| classify_archive_error(&format!("hash profile row for {did_str}"), &err))?;
    let (receipt, artifacts) = sink
        .finish(
            StreamingReceiptInput {
                fetch_method: FetchMethod::GetRepo,
                completeness_class: CompletenessClass::SnapshotComplete,
                reachable_records_count: parsed.rkey_digest.all_records_count,
                reachable_post_records_count: parsed.rkey_digest.post_records_count,
                post_decode_error_count: parsed.post_decode_error_count,
                profile_row_hash,
                mst_root_cid: Some(parsed.commit.data.clone()),
                commit_cid: Some(parsed.commit.cid.clone()),
            },
            parsed.profile.as_ref(),
        )
        .map_err(|err| {
            classify_archive_error(&format!("finish archive artifacts for {did_str}"), &err)
        })?;
    Ok(ProcessedRepo::GetRepo(GetRepoProcessed {
        counts: ProcessedRepoCounts {
            records: parsed.rkey_digest.all_records_count,
            archived_posts: receipt.archived_post_rows_count,
            decode_errors: parsed.record_decode_error_count,
            emoji_rows: artifacts.emoji_rows,
        },
        artifacts: ProcessedRepoArtifacts {
            receipt_hash: receipt.post_rows_hash,
            parquet_path: artifacts.parquet_path,
            receipt_path: artifacts.receipt_path,
            manifest_path: artifacts.manifest_path,
            emoji_projection_path: artifacts.emoji_projection_path,
        },
        timings: GetRepoTimings {
            fetch_ms: None,
            bytes: None,
            parse_ms,
            parse_index_ms: parsed.timings.index_ms,
            parse_commit_ms: parsed.timings.commit_ms,
            parse_walk_ms: parsed.timings.walk_ms,
            archive_ms: elapsed_ms(archive_started),
        },
    }))
}

fn parse_repo_streaming_archive_unprofiled(
    car_path: &Path,
    did_str: &str,
    parse_config: ParseConfig,
    state: ArchiveRunState,
    did: String,
    normalizer: NormalizerVersion,
) -> Result<(ParsedRepoSummary, ArchiveRunState), ParseVisitError<ArchiveError>> {
    let observed_at = state.sink.observed_at();
    parse_repo_for_did_with_state(
        car_path,
        did_str,
        parse_config,
        state,
        move |state, post| {
            let row =
                archive_row_from_owned_post_observed_at(&did, post, &normalizer, observed_at)?;
            state.sink.push_row(row)
        },
    )
}

fn parse_repo_streaming_archive_profiled(
    car_path: &Path,
    did_str: &str,
    parse_config: ParseConfig,
    state: ArchiveRunState,
    did: String,
    normalizer: NormalizerVersion,
) -> Result<(ParsedRepoSummary, ArchiveRunState), ParseVisitError<ArchiveError>> {
    let observed_at = state.sink.observed_at();
    let (summary, state) = parse_repo_for_did_with_state(
        car_path,
        did_str,
        parse_config,
        state,
        move |state, post| {
            let archive_row_started = Instant::now();
            let row =
                archive_row_from_owned_post_observed_at(&did, post, &normalizer, observed_at)?;
            state.archive_row_ns = state
                .archive_row_ns
                .saturating_add(archive_row_started.elapsed().as_nanos());
            let sink_push_started = Instant::now();
            let result = state.sink.push_row(row);
            state.sink_push_ns = state
                .sink_push_ns
                .saturating_add(sink_push_started.elapsed().as_nanos());
            state.profiled_posts = state.profiled_posts.saturating_add(1);
            result
        },
    )?;
    eprintln!(
        "stage_profile posts={} archive_row_ms={} sink_push_ms={}",
        state.profiled_posts,
        state.archive_row_ns / 1_000_000,
        state.sink_push_ns / 1_000_000
    );
    Ok((summary, state))
}

pub async fn prepare_fetch_host(
    did_str: &str,
    pds: &Uri<String>,
    claim_scope: &ClaimScope,
    host_override_ledger_path: Option<&Path>,
    host_override_cache: Option<HostOverrideCache>,
    host_pacer: Option<&SharedHostPacer>,
) -> Result<PreparedFetchHost, FetchOneFailure> {
    if !claim_scope.includes_did(did_str) {
        return Err(retryable_failure(format!(
            "DID {did_str} is outside configured shard scope"
        )));
    }
    let host = pds_host_key(pds);
    let now = SystemTime::now();
    let host_override =
        load_host_override(host_override_ledger_path, host_override_cache, &host, now)?;
    let fetch_mode = fetch_mode_for_host(&host, host_override.as_ref(), now)?;
    if let Some(pacer) = host_pacer {
        HostPacer::wait_until_ready(pacer, &host)
            .await
            .map_err(|err| retryable_failure(format!("host pacing for {host}: {err}")))?;
    }
    Ok(PreparedFetchHost {
        host,
        host_override,
        fetch_mode,
    })
}

#[derive(Debug)]
pub struct PreparedFetchHost {
    pub host: String,
    pub host_override: Option<HostOverride>,
    pub fetch_mode: ForcedFetchMode,
}

pub fn pds_host_key(pds: &Uri<String>) -> String {
    pds.authority().map_or_else(
        || pds.as_str().to_owned(),
        |authority| authority.host().to_owned(),
    )
}

pub fn load_host_override(
    ledger_path: Option<&Path>,
    cache: Option<HostOverrideCache>,
    host: &str,
    now: SystemTime,
) -> Result<Option<HostOverride>, FetchOneFailure> {
    let Some(ledger_path) = ledger_path else {
        return Ok(None);
    };
    if let Some(ref cache) = cache {
        match load_cached_host_override(cache, host)? {
            CachedHostOverride::Hit(cached) => return Ok(cached),
            CachedHostOverride::Miss => {}
        }
    }
    let ledger = SqliteLedger::open(ledger_path)
        .map_err(|err| retryable_failure(format!("open ledger for host override {host}: {err}")))?;
    let override_record = ledger
        .load_host_override(host)
        .map_err(|err| retryable_failure(format!("load host override for {host}: {err}")))?
        .map(|record| normalize_host_override(&ledger, host, record, now))
        .transpose()?;
    if let Some(cache) = cache {
        store_cached_host_override(&cache, host, override_record.clone())?;
    }
    Ok(override_record)
}

enum CachedHostOverride {
    Hit(Option<HostOverride>),
    Miss,
}

fn load_cached_host_override(
    cache: &HostOverrideCache,
    host: &str,
) -> Result<CachedHostOverride, FetchOneFailure> {
    let entries = cache
        .entries
        .lock()
        .map_err(|_err| retryable_failure("host override cache lock poisoned".to_owned()))?;
    Ok(entries
        .get(host)
        .filter(|entry| entry.loaded_at.elapsed() <= HOST_OVERRIDE_CACHE_TTL)
        .map_or(CachedHostOverride::Miss, |entry| {
            CachedHostOverride::Hit(entry.value.clone())
        }))
}

fn store_cached_host_override(
    cache: &HostOverrideCache,
    host: &str,
    value: Option<HostOverride>,
) -> Result<(), FetchOneFailure> {
    cache
        .entries
        .lock()
        .map_err(|_err| retryable_failure("host override cache lock poisoned".to_owned()))?
        .insert(
            host.to_owned(),
            HostOverrideCacheEntry {
                loaded_at: Instant::now(),
                value,
            },
        );
    Ok(())
}

fn normalize_host_override(
    ledger: &SqliteLedger,
    host: &str,
    mut record: HostOverride,
    now: SystemTime,
) -> Result<HostOverride, FetchOneFailure> {
    if record.disabled
        && record
            .revive_after
            .is_some_and(|revive_after| revive_after <= now)
    {
        record.disabled = false;
        record.revive_after = None;
        ledger.upsert_host_override(&record).map_err(|err| {
            retryable_failure(format!(
                "clear expired disabled host override for {host}: {err}"
            ))
        })?;
    }
    Ok(record)
}

pub fn fetch_mode_for_host(
    host: &str,
    host_override: Option<&HostOverride>,
    now: SystemTime,
) -> Result<ForcedFetchMode, FetchOneFailure> {
    let Some(host_override) = host_override else {
        return Ok(ForcedFetchMode::GetRepo);
    };
    if host_override.disabled {
        if let Some(revive_after) = host_override.revive_after
            && let Ok(retry_after) = revive_after.duration_since(now)
        {
            return Err(FetchOneFailure {
                outcome: AttemptOutcome::OperatorDeferred {
                    retry_after: Some(retry_after),
                    message: format!("host {host} disabled by override until {revive_after:?}"),
                },
                error: anyhow::anyhow!("host {host} disabled by override until {revive_after:?}"),
            });
        }
        if host_override.revive_after.is_none() {
            return Err(FetchOneFailure {
                outcome: AttemptOutcome::OperatorDeferred {
                    retry_after: None,
                    message: format!("host {host} disabled by override"),
                },
                error: anyhow::anyhow!("host {host} disabled by override"),
            });
        }
    }
    Ok(host_override.force_mode.unwrap_or(ForcedFetchMode::GetRepo))
}
