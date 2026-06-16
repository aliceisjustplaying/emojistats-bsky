use std::{path::Path, time::Instant};

use emojistats_backfill::{
    archive::ArchiveCommitContext,
    parse::{ParseVisitError, PostRecordBody, parse_repo_for_did_with_state},
};

use super::{
    failure::{current_rss_kb, elapsed_ms, outcome_name},
    parse_and_archive_spooled_repo, parse_config_for_threads,
};

pub fn run(
    did: &str,
    car_path: &Path,
    archive_dir: &Path,
    parse_only: bool,
    cid_verification_threads: usize,
) -> anyhow::Result<()> {
    if parse_only {
        return profile_car_parse_only(did, car_path, cid_verification_threads);
    }
    let started = Instant::now();
    let processed = parse_and_archive_spooled_repo(
        did,
        car_path,
        archive_dir,
        ArchiveCommitContext::fetch_one_local(),
        parse_config_for_threads(cid_verification_threads),
        None,
    )
    .map_err(|failure| {
        anyhow::anyhow!(
            "profile-car failed with {}: {}",
            outcome_name(&failure.outcome),
            failure.error
        )
    })?;
    let counts = processed.counts();
    let artifacts = processed.artifacts();
    let timings = processed
        .get_repo_timings()
        .ok_or_else(|| anyhow::anyhow!("profile-car expected getRepo timings"))?;
    println!(
        "profile-car parsed {} records, {} posts, {} decode errors, {} emoji rows",
        counts.records, counts.archived_posts, counts.decode_errors, counts.emoji_rows
    );
    println!(
        "timings total={}ms parse={}ms index={}ms commit={}ms walk={}ms archive={}ms rss_kb={}",
        elapsed_ms(started),
        timings.parse_ms,
        timings.parse_index_ms,
        timings.parse_commit_ms,
        timings.parse_walk_ms,
        timings.archive_ms,
        current_rss_kb().unwrap_or_default()
    );
    println!(
        "wrote archive {}, receipt {}, manifest {}, emoji projection {}",
        artifacts.parquet_path.display(),
        artifacts.receipt_path.display(),
        artifacts.manifest_path.display(),
        artifacts.emoji_projection_path.display()
    );
    Ok(())
}

#[derive(Debug, Default)]
struct ParseOnlyCounters {
    posts: u64,
    raw_partial_posts: u64,
}

fn profile_car_parse_only(
    did: &str,
    car_path: &Path,
    cid_verification_threads: usize,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let (summary, counters) = parse_repo_for_did_with_state(
        car_path,
        did,
        parse_config_for_threads(cid_verification_threads),
        ParseOnlyCounters::default(),
        |state, post| {
            state.posts = state
                .posts
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("profile parse-only post counter overflow"))?;
            if matches!(
                post.body,
                PostRecordBody::RawPartial(record) if record.typed_decode_failed
            ) {
                state.raw_partial_posts =
                    state.raw_partial_posts.checked_add(1).ok_or_else(|| {
                        anyhow::anyhow!("profile parse-only partial post counter overflow")
                    })?;
            }
            Ok::<(), anyhow::Error>(())
        },
    )
    .map_err(|error| match error {
        ParseVisitError::Parse(error) => anyhow::anyhow!("profile parse-only failed: {error}"),
        ParseVisitError::Visit(error) => error,
    })?;
    println!(
        "profile-car parse-only parsed {} records, {} posts, {} raw partial posts, {} decode errors",
        summary.rkey_digest.all_records_count,
        counters.posts,
        counters.raw_partial_posts,
        summary.post_decode_error_count
    );
    println!(
        "timings total={}ms index={}ms commit={}ms walk={}ms rss_kb={}",
        elapsed_ms(started),
        summary.timings.index_ms,
        summary.timings.commit_ms,
        summary.timings.walk_ms,
        current_rss_kb().unwrap_or_default()
    );
    Ok(())
}
