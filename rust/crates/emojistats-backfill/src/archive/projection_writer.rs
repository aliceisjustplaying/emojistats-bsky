use sha2::Digest as _;

use super::{
    ArchiveError, ArchivePostRow, File, Sha256, Write,
    archive_io::{hash_field_bytes, json_bytes},
    borrowed_emoji_projection_rows_for_post,
};

pub(super) struct StreamingProjectionWriter {
    file: File,
    hash: Sha256,
    rows: u64,
}

impl StreamingProjectionWriter {
    pub(super) fn new(file: File) -> Self {
        Self {
            file,
            hash: Sha256::new(),
            rows: 0,
        }
    }

    pub(super) fn write_row(&mut self, row: &ArchivePostRow) -> Result<(), ArchiveError> {
        for projection_row in borrowed_emoji_projection_rows_for_post(row)? {
            let json = json_bytes(&projection_row)?;
            hash_field_bytes(&mut self.hash, &json)?;
            self.file.write_all(&json)?;
            self.file.write_all(b"\n")?;
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or(ArchiveError::CountOverflow {
                    field: "emoji_rows",
                })?;
        }
        Ok(())
    }

    pub(super) fn sync(&self) -> Result<(), ArchiveError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub(super) const fn rows(&self) -> u64 {
        self.rows
    }

    pub(super) fn hash(&self) -> String {
        hex::encode(self.hash.clone().finalize())
    }
}
