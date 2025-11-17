// SPDX-FileCopyrightText: 2025 Abe Kohandel <abe@kodebooth.com>
// SPDX-License-Identifier: MIT

use std::error::Error;

use thiserror::Error as ThisError;
#[derive(ThisError, Debug)]
pub enum DLockError<R = ()> {
    #[error("provider error: {0}")]
    ProviderError(Box<dyn Error>),

    #[error("lock already acquired")]
    AlreadyAcquired(R),

    #[error("lock already released")]
    AlreadyReleased,
}
