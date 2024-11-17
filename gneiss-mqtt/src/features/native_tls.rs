/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Functionality for using [`rustls`](https://crates.io/crates/native-tls) as an MQTT client's TLS
implementation.
 */

use std::sync::Arc;
use crate::client::config::*;
use crate::error::MqttError;

impl TlsOptionsBuilder {

    /// Builds client TLS options using the `native-tls` crate
    ///
    /// If using MTLS, native-tls only supports pkcs8 format private keys.  If your private key is in a different
    /// format, you must first convert it to pkcs8 and instead use that.
    pub fn build_native_tls(&self) -> Result<TlsOptions, MqttError> {
        let mut builder = native_tls::TlsConnector::builder();

        if let Some(root_ca_bytes) = &self.root_ca_bytes {
            builder.disable_built_in_roots(true);

            let root = native_tls::Certificate::from_pem(root_ca_bytes.as_slice())?;
            builder.add_root_certificate(root);
        }

        if self.mode == TlsMode::Mtls {
            let identity = native_tls::Identity::from_pkcs8(self.certificate_bytes.as_ref().unwrap(), self.private_key_bytes.as_ref().unwrap())?;
            builder.identity(identity);
        }

        if let Some(alpn) = &self.alpn {
            let protocols = vec!(alpn.as_str());
            builder.request_alpns(protocols.as_slice());
        }

        builder.use_sni(self.verify_peer);

        Ok(TlsOptions {
            options: TlsData::NativeTls(Arc::new(builder))
        })
    }

    /// Builds client TLS options directly from a native-tls TlsConnectorBuilder instance
    ///
    /// This factory is useful when you need TLS properties that TlsOptionsBuilder does not support.
    pub fn build_native_tls_from_tls_connector_builder(builder: native_tls::TlsConnectorBuilder) -> TlsOptions {
        TlsOptions {
            options: TlsData::NativeTls(Arc::new(builder))
        }
    }
}
