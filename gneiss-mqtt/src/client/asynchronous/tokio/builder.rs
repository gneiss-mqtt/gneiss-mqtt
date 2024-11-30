/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::client::config::*;
use crate::error::*;
use super::*;

use ::tokio::runtime::Handle;

/// A builder for creating tokio-based MQTT clients.
pub struct TokioClientBuilder {
    endpoint: String,
    port: u16,

    tls_options: Option<TlsOptions>,
    connect_options: Option<ConnectOptions>,
    client_options: Option<MqttClientOptions>,
    http_proxy_options: Option<HttpProxyOptions>,
    websocket_options: Option<AsyncWebsocketOptions>,
    tokio_options: Option<TokioOptions>
}

impl TokioClientBuilder {

    /// Creates a new TokioClientBuilder instance that will construct clients that connect
    /// to the specified endpoint and port.
    pub fn new(endpoint: &str, port: u16) -> Self {
        TokioClientBuilder {
            endpoint: endpoint.to_string(),
            port,
            tls_options: None,
            connect_options: None,
            client_options: None,
            http_proxy_options: None,
            websocket_options: None,
            tokio_options: None,
        }
    }

    /// Configures what TLS options to use for the connection to the broker.
    ///
    /// If not specified, then TLS will not be used.
    pub fn with_tls_options(&mut self, tls_options: TlsOptions) -> &mut Self {
        self.tls_options = Some(tls_options);
        self
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

    /// Configures created clients to connect through an http proxy.
    pub fn with_http_proxy_options(&mut self, http_proxy_options: HttpProxyOptions) -> &mut Self {
        self.http_proxy_options = Some(http_proxy_options);
        self
    }

    /// Configures created clients to use websockets as transport.
    pub fn with_websocket_options(&mut self, websocket_options: AsyncWebsocketOptions) -> &mut Self {
        self.websocket_options = Some(websocket_options);
        self
    }

    //#[cfg_attr(not(feature = "tokio-websockets"), allow(dead_code))]
    #[cfg(all(feature = "tokio-websockets", feature = "testing"))]
    pub(crate) fn clear_websocket_options(&mut self) {
        self.websocket_options = None;
    }

    /// Configures tokio-related options that created clients should use.
    pub fn with_tokio_options(&mut self, tokio_options: TokioOptions) -> &mut Self {
        self.tokio_options = Some(tokio_options);
        self
    }

    /// Builds a new tokio-based MQTT client according to all the configuration options that have
    /// been given to the builder.
    pub fn build(&self) -> GneissResult<AsyncClientHandle> {
        let tls_impl = self.get_tls_impl();
        if tls_impl == TlsConfiguration::Mixed {
            return Err(GneissError::new_tls_error("Cannot mix two different tls implementations in one client"));
        }

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

        let http_proxy_options = self.http_proxy_options.clone();
        let tls_options = self.tls_options.clone();
        let ws_options = self.websocket_options.clone();
        let endpoint = self.endpoint.clone();

        make_client_tokio(tls_impl, endpoint, self.port, tls_options, client_options, connect_options, http_proxy_options, ws_options, tokio_options)
    }

    fn get_tls_impl(&self) -> TlsConfiguration {
        let to_broker_tls = get_tls_impl_from_options(self.tls_options.as_ref());
        let mut to_proxy_tls = TlsConfiguration::None;
        if let Some(http_proxy_options) = &self.http_proxy_options {
            to_proxy_tls = get_tls_impl_from_options(http_proxy_options.tls_options.as_ref());
        }

        if to_broker_tls == to_proxy_tls {
            return to_broker_tls;
        }

        if to_broker_tls != TlsConfiguration::None {
            if to_proxy_tls != TlsConfiguration::None {
                TlsConfiguration::Mixed
            } else {
                to_broker_tls
            }
        } else {
            to_proxy_tls
        }
    }
}