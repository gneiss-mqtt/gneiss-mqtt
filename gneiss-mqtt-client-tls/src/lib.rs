/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![cfg_attr(feature = "strict", deny(warnings))]

/*!
TLS configuration data types for Gneiss MQTT clients
 */

use std::fs::File;
use std::io::Read;
use gneiss_mqtt::error::{GneissError, GneissResult};
use gneiss_mqtt::client::synchronous::network::SyncStreamTransform;

#[derive(Eq, PartialEq, Clone, Copy)]
pub enum TlsMode {
    Standard,
    Mtls
}

fn load_file(filename: &str) -> std::io::Result<Vec<u8>> {
    let mut bytes_vec = Vec::new();
    let mut bytes_file = File::open(filename)?;
    bytes_file.read_to_end(&mut bytes_vec)?;
    Ok(bytes_vec)
}


/// Builder type for constructing TLS configuration.
#[derive(Clone)]
pub struct TlsOptions {
    mode: TlsMode,
    root_ca_bytes: Option<Vec<u8>>,
    certificate_bytes: Option<Vec<u8>>,
    private_key_bytes: Option<Vec<u8>>,
    verify_peer: bool,
    alpn: Option<String>, // one protocol only for now
    server_name_override: Option<String>
}

impl TlsOptions {
    pub(crate) fn new() -> TlsOptions {
        TlsOptions {
            mode: TlsMode::Standard,
            root_ca_bytes: None,
            certificate_bytes: None,
            private_key_bytes: None,
            verify_peer: true,
            alpn: None,
            server_name_override: None
        }
    }

    pub(crate) fn new_mtls(certificate_bytes: Vec<u8>, private_key_bytes: Vec<u8>) -> TlsOptions {
        TlsOptions {
            mode: TlsMode::Mtls,
            root_ca_bytes: None,
            certificate_bytes: Some(certificate_bytes),
            private_key_bytes: Some(private_key_bytes),
            verify_peer: true,
            alpn: None,
            server_name_override: None
        }
    }

    pub(crate) fn with_root_ca(&mut self, root_ca_bytes: Vec<u8>) -> &mut TlsOptions {
        self.root_ca_bytes = Some(root_ca_bytes);
        self
    }

    pub(crate) fn with_verify_peer(&mut self, verify_peer: bool) -> &mut TlsOptions {
        self.verify_peer = verify_peer;
        self
    }

    pub(crate) fn with_alpn(&mut self, alpn: String) -> &mut TlsOptions {
        self.alpn = Some(alpn);
        self
    }

    pub(crate) fn with_server_name_override(&mut self, server_name_override: &str) -> &mut TlsOptions {
        self.server_name_override = Some(server_name_override.to_string());
        self
    }

    pub fn mode(&self) -> TlsMode { self.mode }
    pub fn root_ca_bytes(&self) -> &Option<Vec<u8>> { &self.root_ca_bytes }
    pub fn certificate_bytes(&self) -> &Option<Vec<u8>> { &self.certificate_bytes }
    pub fn private_key_bytes(&self) -> &Option<Vec<u8>> { &self.private_key_bytes }
    pub fn verify_peer(&self) -> bool { self.verify_peer }
    pub fn alpn(&self) -> &Option<String> { &self.alpn }
    pub fn server_name_override(&self) -> &Option<String> { &self.server_name_override }
}

pub struct TlsOptionsBuilder {
    options: TlsOptions
}

impl TlsOptionsBuilder {

    pub(crate) fn new() -> Self {
        TlsOptionsBuilder {
            options: TlsOptions::new()
        }
    }

    pub(crate) fn new_with_mtls_from_path(certificate_path: &str, private_key_path: &str) -> GneissResult<Self> {
        let certificate_bytes = load_file(certificate_path)?;
        let private_key_bytes = load_file(private_key_path)?;

        Ok(TlsOptionsBuilder {
            options: TlsOptions::new_mtls(certificate_bytes, private_key_bytes)
        })
    }

    pub(crate) fn new_with_mtls_from_memory(certificate_bytes: &[u8], private_key_bytes: &[u8]) -> Self {
        TlsOptionsBuilder {
            options: TlsOptions::new_mtls(certificate_bytes.to_vec(), private_key_bytes.to_vec())
        }
    }

    /// Configures the builder to use a trust store that *only* contains a single root certificate,
    /// supplied by file path.
    pub fn with_root_ca_from_path(&mut self, root_ca_path: &str) -> GneissResult<&mut Self> {
        self.options.with_root_ca(load_file(root_ca_path)?);
        Ok(self)
    }

    /// Configures the builder to use a trust store that *only* contains a single root certificate,
    /// supplied from memory.
    pub fn with_root_ca_from_memory(&mut self, root_ca_bytes: &[u8]) -> &mut Self {
        self.options.with_root_ca(root_ca_bytes.to_vec());
        self
    }

    /// Controls whether or not SNI is used during the TLS handshake.
    ///
    /// It is highly recommended to set this value to false only in testing environments.
    pub fn with_verify_peer(&mut self, verify_peer: bool) -> &mut Self {
        self.options.with_verify_peer(verify_peer);
        self
    }

    /// Sets an ALPN protocol to negotiate during the TLS handshake.
    ///
    /// Should multiple protocols become a valid use case, new APIs will be added to manipulate
    /// the set of protocols.
    pub fn with_alpn(&mut self, alpn: &str) -> &mut Self {
        self.options.with_alpn(alpn.to_string());
        self
    }

    pub fn with_server_name_override(&mut self, server_name_override: &str) -> &mut Self {
        self.options.with_server_name_override(server_name_override);
        self
    }

   pub fn build(&self) -> TlsOptions {
        self.options.clone()
   }
}

pub trait TlsSyncConnectionStepFactory {
    fn build_connection_step(&self, options: &TlsOptions, host: &str) -> GneissResult<SyncStreamTransform>;
}
