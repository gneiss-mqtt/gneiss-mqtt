/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module that encompasses feature-specific logic (primarily TLS and Async runtime).
 */

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
pub mod rustls;

#[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
pub mod native_tls;

