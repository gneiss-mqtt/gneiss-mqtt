/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::client::config::*;
use crate::error::*;
use super::*;

use ::tokio::runtime::Handle;

/// A builder for creating tokio-based MQTT clients.
#[derive(Default)]
pub struct TokioClientBuilder {
    connect_options: Option<ConnectOptions>,
    client_options: Option<MqttClientOptions>,
    tokio_options: Option<TokioOptions>
}

impl TokioClientBuilder {

    /// Creates a new TokioClientBuilder instance that will construct clients that connect
    /// to the specified endpoint and port.
    pub fn new() -> Self {
        Self::default()
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

    /// Configures tokio-related options that created clients should use.
    pub fn with_tokio_options(&mut self, tokio_options: TokioOptions) -> &mut Self {
        self.tokio_options = Some(tokio_options);
        self
    }

    /// Builds a new tokio-based MQTT client according to all the configuration options that have
    /// been given to the builder.
    pub fn build<T>(&self, connection_factory: TokioConnectionFactory<T>) -> GneissResult<AsyncClientHandle> where T : AsyncRead + AsyncWrite + Send + Sync + 'static {
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

        let tokio_options =
            if let Some(options) = &self.tokio_options {
                options.clone()
            } else {
                TokioOptions::builder(Handle::current()).build()
            };

        Ok(new_tokio_client(client_options, connect_options, tokio_options, connection_factory))
    }
}