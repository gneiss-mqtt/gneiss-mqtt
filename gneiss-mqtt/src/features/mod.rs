/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module that encompasses feature-specific logic (primarily TLS and Async runtime).
 */

#[cfg(feature = "tokio-rustls")]
pub mod gneiss_rustls;

#[cfg(feature = "tokio")]
pub mod gneiss_tokio;

#[cfg(feature = "tokio-native-tls")]
mod gneiss_native_tls;
