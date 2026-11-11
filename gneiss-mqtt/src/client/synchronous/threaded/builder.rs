/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::client::synchronous::*;
use crate::client::config::*;
use crate::error::*;
use super::*;

/// A builder for creating thread-based MQTT clients.
pub struct ThreadedClientBuilder {
    connect_options: Option<ConnectOptions>,
    client_options: Option<MqttClientOptions>,
    threaded_options: Option<ThreadedOptions>
}

impl ThreadedClientBuilder {

    /// Creates a new ThreadedClientBuilder instance that will construct clients that connect
    /// to the specified endpoint and port.
    pub fn new(endpoint: &str, port: u16) -> Self {
        ThreadedClientBuilder {
            connect_options: None,
            client_options: None,
            threaded_options: None,
        }
    }

    /// Configures the Connect packet related options for created clients.
    ///
    /// If not specified, default values will be used.
    pub fn with_connect_options(&mut self, connect_options: ConnectOptions) -> &mut Self {
        self.connect_options = Some(connect_options);
        self
    }

    /// Configures client MQTT behavioral options.
    ///
    /// If not specified, default values will be used.
    pub fn with_client_options(&mut self, client_options: MqttClientOptions) -> &mut Self {
        self.client_options = Some(client_options);
        self
    }

    /// Configures thread-related options that created clients should use.
    pub fn with_threaded_options(&mut self, threaded_options: ThreadedOptions) -> &mut Self {
        self.threaded_options = Some(threaded_options);
        self
    }

    /// Builds a new thread-based MQTT client according to all the configuration options that have
    /// been given to the builder.
    pub fn build<T>(&self, connection_factory: ThreadedConnectionFactory<T>) -> GneissResult<SyncClientHandle> where T : Read + Write + Send + Sync + 'static {
        let connect_options =
            if let Some(options) = &self.connect_options {
                options.clone()
            } else {
                ConnectOptions::builder().build()
            };

        let client_options =
            if let Some(options) = &self.client_options {
                options.clone()
            } else {
                MqttClientOptionsBuilder::new().build()
            };

        let threaded_options =
            if let Some(options) = &self.threaded_options {
                options.clone()
            } else {
                ThreadedOptions::builder().build()
            };

        Ok(new_threaded_client(client_options, connect_options, threaded_options, connection_factory))
    }
}