use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::Notify;

use super::super::failure::{FetchOneFailure, retryable_failure};

#[derive(Debug, Clone, Default)]
pub struct HostConcurrencyLimiter {
    hosts: Arc<Mutex<HashMap<String, Arc<HostConcurrencyState>>>>,
}

#[derive(Debug)]
struct HostConcurrencyState {
    inner: Mutex<HostConcurrencyInner>,
    notify: Notify,
}

#[derive(Debug)]
struct HostConcurrencyInner {
    cap: usize,
    in_use: usize,
}

#[derive(Debug)]
pub struct HostConcurrencyPermit {
    state: Arc<HostConcurrencyState>,
}

impl HostConcurrencyLimiter {
    pub async fn acquire(
        &self,
        host: &str,
        concurrency_cap: Option<u32>,
    ) -> Result<Option<HostConcurrencyPermit>, FetchOneFailure> {
        let Some(concurrency_cap) = concurrency_cap else {
            return Ok(None);
        };
        let cap = usize::try_from(concurrency_cap)
            .map_err(|_err| retryable_failure(format!("host cap overflows usize for {host}")))?;
        let state = {
            let mut hosts = self
                .hosts
                .lock()
                .map_err(|_err| retryable_failure("host limiter lock poisoned".to_owned()))?;
            Arc::clone(hosts.entry(host.to_owned()).or_insert_with(|| {
                Arc::new(HostConcurrencyState {
                    inner: Mutex::new(HostConcurrencyInner { cap, in_use: 0 }),
                    notify: Notify::new(),
                })
            }))
        };
        state.acquire(host, cap).await.map(Some)
    }
}

impl HostConcurrencyState {
    async fn acquire(
        self: Arc<Self>,
        host: &str,
        cap: usize,
    ) -> Result<HostConcurrencyPermit, FetchOneFailure> {
        loop {
            let notified = self.notify.notified();
            {
                let mut inner = self.inner.lock().map_err(|_err| {
                    retryable_failure(format!("host limiter lock poisoned for {host}"))
                })?;
                inner.cap = cap;
                if inner.in_use < inner.cap {
                    inner.in_use = inner.in_use.checked_add(1).ok_or_else(|| {
                        retryable_failure(format!("host limiter in-use count overflow for {host}"))
                    })?;
                    let state = Arc::clone(&self);
                    drop(inner);
                    return Ok(HostConcurrencyPermit { state });
                }
            }
            notified.await;
        }
    }
}

impl Drop for HostConcurrencyPermit {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.state.inner.lock() {
            inner.in_use = inner.in_use.saturating_sub(1);
        }
        self.state.notify.notify_waiters();
    }
}
