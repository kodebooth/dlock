// SPDX-FileCopyrightText: 2025 Abe Kohandel <abe@kodebooth.com>
// SPDX-License-Identifier: MIT

use std::{fmt::Debug, time::Duration};

use crate::error::DLockError;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

/// `Provider` is a trait that abstracts the backend specific details of the
/// lock acquisition mechanism.
pub trait Provider {
    type T;
    type L: Lease<Self::L, Self::T>;
    type R;

    #[allow(async_fn_in_trait)]
    async fn acquire(
        &self,
        name: impl Into<String>,
        duration: impl Into<Duration>,
        retry: Option<Self::R>,
    ) -> Result<Self::L, DLockError<Self::R>>;
}

/// `Lease` is a trait that abstracts the backend specific details of the
/// lock renewal and release mechanism.
pub trait Lease<L, T>: Debug {
    #[allow(async_fn_in_trait)]
    async fn release(&self) -> Result<(), DLockError>;
    #[allow(async_fn_in_trait)]
    async fn renew(&self) -> Result<L, DLockError>;

    fn token(&self) -> T;
}
