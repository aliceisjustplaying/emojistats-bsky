use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use jacquard_common::types::did::Did;
use tokio::sync::Notify;

use super::FetchError;

/// Shared cap for bytes currently held by in-flight streamed `CAR` files.
#[derive(Debug, Clone)]
pub struct FetchByteBudget {
    inner: Arc<FetchByteBudgetInner>,
}

#[derive(Debug)]
struct FetchByteBudgetInner {
    max_bytes: u64,
    charged_bytes: Mutex<u64>,
    notify: Notify,
}

impl FetchByteBudget {
    /// Build a shared in-flight byte budget.
    #[must_use]
    pub fn new(max_bytes: u64) -> Self {
        Self {
            inner: Arc::new(FetchByteBudgetInner {
                max_bytes,
                charged_bytes: Mutex::new(0),
                notify: Notify::new(),
            }),
        }
    }

    pub(super) fn reservation(&self) -> FetchByteBudgetReservation {
        FetchByteBudgetReservation {
            budget: self.clone(),
            charged_bytes: 0,
        }
    }

    async fn reserve_charged_delta(&self, delta: u64) -> Result<(), FetchError> {
        if delta == 0 || self.inner.max_bytes == 0 {
            return Ok(());
        }
        loop {
            let notified = self.inner.notify.notified();
            {
                let mut charged = self
                    .inner
                    .charged_bytes
                    .lock()
                    .map_err(|_error| FetchError::ByteBudgetPoisoned)?;
                let next = charged
                    .checked_add(delta)
                    .ok_or(FetchError::InFlightBytesExceeded {
                        max_bytes: self.inner.max_bytes,
                        observed_bytes: u64::MAX,
                    })?;
                if next <= self.inner.max_bytes {
                    *charged = next;
                    drop(charged);
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn release_charged(&self, bytes: u64) {
        if bytes == 0 || self.inner.max_bytes == 0 {
            return;
        }
        if let Ok(mut charged) = self.inner.charged_bytes.lock() {
            *charged = charged.saturating_sub(bytes);
        }
        self.inner.notify.notify_waiters();
    }
}

/// Held with a spooled repo until parse/archive no longer needs the local `CAR`.
#[derive(Debug)]
pub(super) struct FetchByteBudgetReservation {
    budget: FetchByteBudget,
    charged_bytes: u64,
}

impl FetchByteBudgetReservation {
    pub(super) async fn reserve_capacity(&mut self, bytes: u64) -> Result<(), FetchError> {
        if bytes > self.budget.inner.max_bytes {
            return Err(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            });
        }
        if bytes <= self.charged_bytes {
            return Ok(());
        }
        let charged_target = bytes;
        let delta = charged_target.checked_sub(self.charged_bytes).ok_or(
            FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            },
        )?;
        self.budget.reserve_charged_delta(delta).await?;
        self.charged_bytes = charged_target;
        Ok(())
    }

    pub(super) fn try_reserve_capacity(&mut self, bytes: u64) -> Result<(), FetchError> {
        if bytes > self.budget.inner.max_bytes {
            return Err(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            });
        }
        if bytes <= self.charged_bytes {
            return Ok(());
        }
        let charged_target = bytes;
        let delta = charged_target.checked_sub(self.charged_bytes).ok_or(
            FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            },
        )?;
        if delta == 0 || self.budget.inner.max_bytes == 0 {
            self.charged_bytes = charged_target;
            return Ok(());
        }
        let mut charged = self
            .budget
            .inner
            .charged_bytes
            .lock()
            .map_err(|_error| FetchError::ByteBudgetPoisoned)?;
        let next = charged
            .checked_add(delta)
            .ok_or(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: u64::MAX,
            })?;
        if next > self.budget.inner.max_bytes {
            return Err(FetchError::InFlightBytesUnavailable {
                max_bytes: self.budget.inner.max_bytes,
                requested_bytes: charged_target,
            });
        }
        *charged = next;
        drop(charged);
        self.charged_bytes = charged_target;
        Ok(())
    }

    pub(super) fn shrink_to_actual(&mut self, bytes: u64) -> Result<(), FetchError> {
        if bytes > self.charged_bytes {
            return Err(FetchError::InFlightBytesExceeded {
                max_bytes: self.budget.inner.max_bytes,
                observed_bytes: bytes,
            });
        }
        let delta =
            self.charged_bytes
                .checked_sub(bytes)
                .ok_or(FetchError::InFlightBytesExceeded {
                    max_bytes: self.budget.inner.max_bytes,
                    observed_bytes: bytes,
                })?;
        self.charged_bytes = bytes;
        self.budget.release_charged(delta);
        Ok(())
    }

    #[cfg(test)]
    pub(super) const fn charged_bytes(&self) -> u64 {
        self.charged_bytes
    }
}

impl Drop for FetchByteBudgetReservation {
    fn drop(&mut self) {
        self.budget.release_charged(self.charged_bytes);
    }
}

/// A successfully spooled repo `CAR`.
#[derive(Debug)]
pub struct SpooledRepo {
    /// Path to the local spooled `CAR`.
    pub car_path: PathBuf,
    /// HTTP status returned by `getRepo`.
    pub http_status: u16,
    /// Rate-limit headers observed on the response.
    pub rate_limit: super::RateLimitSnapshot,
    /// Bytes written to `car_path`.
    pub bytes: u64,
    pub(super) _byte_budget_reservation: Option<FetchByteBudgetReservation>,
}

impl Drop for SpooledRepo {
    fn drop(&mut self) {
        let _ignored = fs::remove_file(&self.car_path);
    }
}

pub(super) fn sync_parent_dir(path: &Path) -> Result<(), FetchError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(super) fn spool_path(spool_dir: &Path, did: &Did) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut file_name = String::from("repo-");
    for ch in did.as_str().chars() {
        if ch.is_ascii_alphanumeric() {
            file_name.push(ch);
        } else {
            file_name.push('_');
        }
    }
    file_name.push('.');
    file_name.push_str(&std::process::id().to_string());
    file_name.push('.');
    file_name.push_str(&timestamp.to_string());
    file_name.push_str(".car");
    spool_dir.join(file_name)
}
