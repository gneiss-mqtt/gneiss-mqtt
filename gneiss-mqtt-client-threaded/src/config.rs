/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::time::Duration;

/// Threaded client specific configuration
#[derive(Default, Clone)]
pub struct ThreadedOptions {
    pub(crate) idle_service_sleep: Option<Duration>,
}

impl ThreadedOptions {

    /// Creates a new builder for ThreadedClientOptions instances
    pub fn builder() -> ThreadedOptionsBuilder {
        ThreadedOptionsBuilder::new()
    }
}

/// Builder type for threaded client configuration
pub struct ThreadedOptionsBuilder {
    config: ThreadedOptions
}

impl ThreadedOptionsBuilder {

    pub(crate) fn new() -> Self {
        ThreadedOptionsBuilder {
            config: ThreadedOptions {
                idle_service_sleep: None,
            }
        }
    }

    /// Configures the time interval to sleep the thread the client runs on between io
    /// processing events.
    ///
    /// Only used if no events occurred on the previous iteration.  If the
    /// client is handling significant work, it will not sleep, but if there's nothing
    /// happening, it will.
    ///
    /// If not set, defaults to 20 milliseconds.
    pub fn with_idle_service_sleep(&mut self, duration: Duration) {
        self.config.idle_service_sleep = Some(duration);
    }

    /// Builds a new set of threaded client configuration options
    pub fn build(self) -> ThreadedOptions {
        self.config
    }
}