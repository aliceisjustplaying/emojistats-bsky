#![allow(clippy::redundant_pub_crate)]

use std::path::PathBuf;

#[derive(Debug)]
pub(crate) struct FetchedRepo {
    pub(crate) spooled: emojistats_backfill::transport::SpooledRepo,
    pub(crate) fetch_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessedRepoCounts {
    pub(crate) records: u64,
    pub(crate) archived_posts: u64,
    pub(crate) decode_errors: u64,
    pub(crate) emoji_rows: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ProcessedRepoArtifacts {
    pub(crate) post_rows_hash: String,
    pub(crate) parquet_path: PathBuf,
    pub(crate) receipt_path: PathBuf,
    pub(crate) manifest_path: PathBuf,
    pub(crate) emoji_projection_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct GetRepoTimings {
    pub(crate) fetch_ms: Option<u64>,
    pub(crate) bytes: Option<u64>,
    pub(crate) parse_ms: u64,
    pub(crate) parse_index_ms: u64,
    pub(crate) parse_commit_ms: u64,
    pub(crate) parse_walk_ms: u64,
    pub(crate) archive_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ListRecordsTimings {
    pub(crate) fetch_ms: u64,
    pub(crate) archive_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct GetRepoProcessed {
    pub(crate) counts: ProcessedRepoCounts,
    pub(crate) artifacts: ProcessedRepoArtifacts,
    pub(crate) timings: GetRepoTimings,
}

#[derive(Debug, Clone)]
pub(crate) struct ListRecordsProcessed {
    pub(crate) counts: ProcessedRepoCounts,
    pub(crate) artifacts: ProcessedRepoArtifacts,
    pub(crate) timings: ListRecordsTimings,
}

#[derive(Debug, Clone)]
pub(crate) enum ProcessedRepo {
    GetRepo(GetRepoProcessed),
    ListRecords(ListRecordsProcessed),
}

impl ProcessedRepo {
    pub(crate) const fn counts(&self) -> &ProcessedRepoCounts {
        match self {
            Self::GetRepo(processed) => &processed.counts,
            Self::ListRecords(processed) => &processed.counts,
        }
    }

    pub(crate) const fn artifacts(&self) -> &ProcessedRepoArtifacts {
        match self {
            Self::GetRepo(processed) => &processed.artifacts,
            Self::ListRecords(processed) => &processed.artifacts,
        }
    }

    pub(crate) const fn fetch_ms_opt(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => processed.timings.fetch_ms,
            Self::ListRecords(processed) => Some(processed.timings.fetch_ms),
        }
    }

    pub(crate) const fn bytes(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => processed.timings.bytes,
            Self::ListRecords(_) => None,
        }
    }

    pub(crate) const fn parse_ms(&self) -> Option<u64> {
        match self {
            Self::GetRepo(processed) => Some(processed.timings.parse_ms),
            Self::ListRecords(_) => None,
        }
    }

    pub(crate) const fn archive_ms(&self) -> u64 {
        match self {
            Self::GetRepo(processed) => processed.timings.archive_ms,
            Self::ListRecords(processed) => processed.timings.archive_ms,
        }
    }

    pub(crate) const fn get_repo_timings(&self) -> Option<&GetRepoTimings> {
        match self {
            Self::GetRepo(processed) => Some(&processed.timings),
            Self::ListRecords(_) => None,
        }
    }

    pub(crate) const fn with_get_repo_fetch(mut self, fetch_ms: u64, bytes: u64) -> Self {
        if let Self::GetRepo(processed) = &mut self {
            processed.timings.fetch_ms = Some(fetch_ms);
            processed.timings.bytes = Some(bytes);
        }
        self
    }
}
