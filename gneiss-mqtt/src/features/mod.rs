/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module that encompasses feature-specific logic (primarily TLS and Async runtime).
 */

#[cfg(feature = "tokio-rustls")]
pub mod rustls;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "tokio-native-tls")]
pub mod native_tls;

#[cfg(feature = "threaded")]
pub mod threaded;
