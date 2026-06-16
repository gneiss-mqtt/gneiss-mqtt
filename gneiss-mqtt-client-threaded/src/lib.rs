/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Implementation of an MQTT client that uses one or more background threads for processing.
 */

pub mod config;

/// A builder for creating thread-based MQTT clients.
pub struct ThreadedClientBuilder {
    endpoint: String,
    port: u16,
    
    threaded_options: Option<ThreadedOptions>
}