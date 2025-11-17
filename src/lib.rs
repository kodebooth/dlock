// SPDX-FileCopyrightText: 2025 Abe Kohandel <abe@kodebooth.com>
// SPDX-License-Identifier: MIT

//! A lease based distributed lock with support for fencing tokens
//!
//! DLock uses a lease based algorithm to provide locking mechanisms for
//! distributed clients. DLock also provides a fencing token that can be
//! used to prevent a stale lock from erroneously being use by a client.
//!
//! # Examples
//!
//! You can use the automatic lease renewal mechanism built into `DLock` or manually manage the lease yourself:
//!
//! ## Automatic
//! [DLock::with] will acquire, automatically renew, and release the lease. For more information see the function documentation.
//! ```rust,no_run
//! use dlock::{DLock, DynamodbProvider};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! const TABLE_NAME: &str = "dynamodb_table";
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = aws_sdk_dynamodb::config::Builder::new().build();
//!     let client = aws_sdk_dynamodb::Client::from_conf(config);
//!     let provider = DynamodbProvider::builder()
//!         .client(Arc::new(client))
//!         .table_name(TABLE_NAME.to_string())
//!         .build();
//!
//!     let lock = DLock::builder()
//!         .name("test_lock".to_string())
//!         .duration(Duration::from_secs(1))
//!         .provider(provider)
//!         .build();
//!
//!     let result = lock.with(async |token| {
//!         // do synchronized work!
//!     }).await;
//! }
//! ```
//!
//! ## Manually
//! You are responsible for acquiring, renewing, and releasing the lease.
//! ```rust,no_run
//! use dlock::{DLock, DynamodbProvider, Lease};
//! use std::sync::Arc;
//! use std::time::Duration;
//! use std::error::Error;
//!
//! const TABLE_NAME: &str = "dynamodb_table";
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = aws_sdk_dynamodb::config::Builder::new().build();
//!     let client = aws_sdk_dynamodb::Client::from_conf(config);
//!     let provider = DynamodbProvider::builder()
//!         .client(Arc::new(client))
//!         .table_name(TABLE_NAME.to_string())
//!         .build();
//!
//!     let lock = DLock::builder()
//!         .name("test_lock".to_string())
//!         .duration(Duration::from_secs(1))
//!         .provider(provider)
//!         .build();
//!
//!     let lease = lock.acquire().await.unwrap();
//!     // do synchronized work!
//!     lease.release().await.unwrap();
//! }
//! ```

use std::{sync::Arc, time::Duration};
pub mod error;
pub mod providers;

use bon::Builder;
use tokio::{join, select, sync::Notify, time::sleep};

use crate::error::DLockError;

#[cfg(feature = "dynamodb")]
pub use providers::dynamodb::{DynamodbLease, DynamodbProvider, DynamodbRetry};
pub use providers::{Lease, Provider};

#[derive(Builder)]
pub struct DLock<P>
where
    P: Provider,
{
    name: String,
    duration: Duration,
    backoff: Option<Duration>,

    provider: P,
}

impl<A> DLock<A>
where
    A: Provider,
{
    /// Attempt to acquire a lock.
    ///
    /// Upon success a lease is returned that can be renewed or can be released
    /// when the lock is no longer needed.
    ///
    /// If lock acquisition fails due to lock contention, a retry context is
    /// returned that should be used with [DLock::retry].
    pub async fn acquire(&self) -> Result<A::L, DLockError<A::R>> {
        self.provider.acquire(&self.name, self.duration, None).await
    }

    /// Retry acquiring a lock.
    ///
    /// Upon success a lease is returned that can be renewed or can be released
    /// when the lock is no longer needed.
    ///
    /// An initial acquisition attempt should be always made using [DLock::acquire].
    pub async fn retry(&self, retry: A::R) -> Result<A::L, DLockError<A::R>> {
        self.provider
            .acquire(&self.name, self.duration, Some(retry))
            .await
    }

    /// Execute a closure while holding the lock.
    ///
    /// The closure will be only called after the lock has been acquired and is
    /// given the fencing token for the current lease.
    ///
    /// <div class="warning">
    /// The lease is renewed using futures just like the closure passed in. If
    /// the closure blocks execution for a long time then the lease duration may
    /// expire and the lease may become invalid. This is equivalent to a
    /// scenario where the lease holder is paused for a long time and can still
    /// be detected using the fencing token.
    /// </div>
    ///
    pub async fn with<R>(&self, mut f: impl AsyncFnMut(A::T) -> R) -> Result<R, DLockError> {
        let mut retry = None;
        let lease = loop {
            match self.acquire_or_retry(retry).await {
                Ok(lease) => break lease,
                Err(error) => match error {
                    DLockError::AlreadyAcquired(r) => retry = Some(r),
                    DLockError::ProviderError(error) => {
                        return Err(DLockError::ProviderError(error));
                    }
                    DLockError::AlreadyReleased => retry = None,
                },
            }

            sleep(self.backoff.unwrap_or_default()).await
        };

        let token = lease.token();
        let notify = Arc::new(Notify::new());

        let wrapper = async || {
            let result = f(token);
            notify.notify_one();
            result.await
        };

        let (result, lease) = join! {
            wrapper(),
            self.renew(lease, Arc::clone(&notify)),
        };

        lease?.release().await?;

        Ok(result)
    }

    async fn acquire_or_retry(&self, retry: Option<A::R>) -> Result<A::L, DLockError<A::R>> {
        match retry {
            Some(retry) => self.retry(retry).await,
            None => self.acquire().await,
        }
    }

    async fn renew(&self, mut lease: A::L, notify: Arc<Notify>) -> Result<A::L, DLockError> {
        Ok(loop {
            let stop = select! {
                _ = sleep(self.duration / 2) => false,
                _ = notify.notified() => true,
            };

            if !stop {
                lease = lease.renew().await?;
            } else {
                break lease;
            }
        })
    }
}
