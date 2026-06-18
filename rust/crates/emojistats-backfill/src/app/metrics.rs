use std::path::Path;

use emojistats_backfill::metrics::{
    SharedMetricsRecorder, jsonl_metrics_recorder, noop_metrics_recorder,
};

pub(super) fn metrics_recorder(path: Option<&Path>) -> anyhow::Result<SharedMetricsRecorder> {
    path.map_or_else(|| Ok(noop_metrics_recorder()), jsonl_metrics_recorder)
}
