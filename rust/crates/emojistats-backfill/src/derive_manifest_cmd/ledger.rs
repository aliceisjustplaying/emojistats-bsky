use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::Serialize;

use crate::{
    clickhouse::{ClickHouseInsertPayload, ClickHouseInsertReceipt},
    derive::DeriveCheckpointRecord,
    manifest_derive::VerifiedLoaderInput,
};

#[derive(Debug)]
pub(super) struct DeriveLedger {
    path: Option<PathBuf>,
    completed: HashSet<DeriveCheckpointRecord>,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct DeriveLedgerRecord {
    checkpoint: DeriveCheckpointRecord,
    run_id: String,
    shard: String,
    file_sequence: u64,
    dataset: String,
    schema_version: u16,
    object_path: String,
    clickhouse_status: u16,
}

impl DeriveLedger {
    pub(super) fn new(path: Option<&Path>) -> anyhow::Result<Self> {
        if let Some(path) = path
            && let Some(parent) = path.parent()
        {
            fs::create_dir_all(parent)?;
        }
        let completed = match path {
            Some(path) if path.try_exists()? => Self::read_completed(path)?,
            Some(_) | None => HashSet::new(),
        };
        Ok(Self {
            path: path.map(Path::to_path_buf),
            completed,
        })
    }

    pub(super) fn is_completed(
        &self,
        _verified: &VerifiedLoaderInput,
        payload: &ClickHouseInsertPayload,
    ) -> anyhow::Result<bool> {
        Ok(self.completed.contains(&Self::checkpoint(payload)?))
    }

    pub(super) fn append_success(
        &mut self,
        verified: &VerifiedLoaderInput,
        payload: &ClickHouseInsertPayload,
        receipt: &ClickHouseInsertReceipt,
    ) -> anyhow::Result<()> {
        let checkpoint = Self::checkpoint(payload)?;
        let Some(path) = &self.path else {
            self.completed.insert(checkpoint);
            return Ok(());
        };
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let record = DeriveLedgerRecord {
            checkpoint: checkpoint.clone(),
            run_id: verified.manifest.run_id.clone(),
            shard: verified.manifest.shard.clone(),
            file_sequence: verified.manifest.file_sequence,
            dataset: verified.manifest.dataset.clone(),
            schema_version: verified.manifest.schema_version,
            object_path: verified.object_path.to_string_lossy().into_owned(),
            clickhouse_status: receipt.status,
        };
        serde_json::to_writer(&mut file, &record)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        self.completed.insert(checkpoint);
        Ok(())
    }

    fn read_completed(path: &Path) -> anyhow::Result<HashSet<DeriveCheckpointRecord>> {
        let file = File::open(path)?;
        let mut completed = HashSet::new();
        for (line_index, line) in BufReader::new(file).lines().enumerate() {
            let line = line?;
            let line_number = line_index
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("derive ledger line number overflow"))?;
            if line.trim().is_empty() {
                continue;
            }
            let record: DeriveLedgerRecord = serde_json::from_str(&line).map_err(|source| {
                anyhow::anyhow!(
                    "parse derive ledger {} line {}: {source}",
                    path.display(),
                    line_number
                )
            })?;
            completed.insert(record.checkpoint);
        }
        Ok(completed)
    }

    fn checkpoint(payload: &ClickHouseInsertPayload) -> anyhow::Result<DeriveCheckpointRecord> {
        Ok(DeriveCheckpointRecord::from_payload_body(
            payload.checkpoint_key.clone(),
            payload.dedupe_token.clone(),
            payload.row_count,
            &payload.body,
        )?)
    }
}
