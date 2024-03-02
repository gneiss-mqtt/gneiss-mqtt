/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use std::sync::Arc;
use crate::config::{TlsData, TlsMode, TlsOptions, TlsOptionsBuilder};
use crate::error::MqttError;

impl TlsOptionsBuilder {

    /// Builds client TLS options using the `native-tls` crate
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
            options: TlsData::NativeTls(self.mode, Arc::new(builder))
        })
    }
}
