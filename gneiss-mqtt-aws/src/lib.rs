/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate gneiss_mqtt;
extern crate tokio;

use gneiss_mqtt::client::builder::{ClientBuilder};
use gneiss_mqtt::{Mqtt5Client, Mqtt5ClientOptions, Mqtt5ClientOptionsBuilder};
use gneiss_mqtt::{Mqtt5Result};
use tokio::runtime::Handle;

const CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-name";
const CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-signature";

pub struct AwsCustomAuthUnsignedOptions {
    authorizer_name: String,
    username: Option<String>,
    password: Option<Vec<u8>>
}

impl AwsCustomAuthUnsignedOptions {
    pub fn new(authorizer_name: &str, username: Option<&str>, password: Option<&[u8]>) -> Self {
        AwsCustomAuthUnsignedOptions {
            authorizer_name: authorizer_name.to_string(),
            username: username.map(str::to_string),
            password: password.map(|p| p.to_vec())
        }
    }

    pub(crate) fn build_username(&self) -> Option<String> {
        let username = format!("{}?{}={}", self.username.as_deref().unwrap_or(""), CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME, self.authorizer_name);
        Some(username)
    }

    pub(crate) fn build_password(&self) -> Option<Vec<u8>> {
        self.password.as_ref().map(|p| p.to_vec())
    }
}

pub struct AwsCustomAuthSignedOptions {
    authorizer_name: String,
    username: Option<String>,
    password: Option<Vec<u8>>,
    authorizer_signature: String,
    authorizer_token_key_name: String,
    authorizer_token_key_value: String
}

impl AwsCustomAuthSignedOptions {
    pub fn new(authorizer_name: &str, authorizer_signature: &str, authorizer_token_key_name: &str, authorizer_token_key_value: &str, username: Option<&str>, password: Option<&[u8]>) -> Self {
        AwsCustomAuthSignedOptions {
            authorizer_name: authorizer_name.to_string(),
            authorizer_signature: authorizer_signature.to_string(),
            authorizer_token_key_name: authorizer_token_key_name.to_string(),
            authorizer_token_key_value: authorizer_token_key_value.to_string(),
            username: username.map(str::to_string),
            password: password.map(|p| p.to_vec())
        }
    }

    pub(crate) fn build_username(&self) -> Option<String> {
        let username =
            format!("{}?{}={}&{}={}&{}={}",
                self.username.as_deref().unwrap_or(""),
                CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME,
                self.authorizer_name,
                CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME,
                self.authorizer_signature,
                self.authorizer_token_key_name,
                self.authorizer_token_key_value);

        Some(username)
    }

    pub(crate) fn build_password(&self) -> Option<Vec<u8>> {
        self.password.as_ref().map(|p| p.to_vec())
    }
}

enum AwsCustomAuthOptions {
    Unsigned(AwsCustomAuthUnsignedOptions),
    Signed(AwsCustomAuthSignedOptions)
}

#[derive(PartialEq, Eq)]
enum AuthType {
    Mtls,
    //Sigv4Websockets,
    CustomAuth
}

pub struct AwsClientBuilder {
    auth_type: AuthType,
    custom_auth_options: Option<AwsCustomAuthOptions>,
    client_options: Option<Mqtt5ClientOptions>,
    inner_builder: ClientBuilder
}

const ALPN_PORT : u16 = 443;
const DEFAULT_PORT : u16 = ALPN_PORT;
const DIRECT_ALPN_PROTOCOL : &[u8] = b"x-amzn-mqtt-ca";
const CUSTOM_AUTH_ALPN_PROTOCOL : &[u8] = b"mqtt";

impl AwsClientBuilder {
    pub fn new_direct_with_mtls_from_fs(endpoint: &str, certificate_path: &str, private_key_path: &str, root_ca_path: Option<&str>) -> Mqtt5Result<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            client_options: None,
            inner_builder: ClientBuilder::new_with_mtls_from_fs(endpoint, DEFAULT_PORT, certificate_path, private_key_path, root_ca_path)?
        };

        builder.set_alpn(DIRECT_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn new_direct_with_mtls_from_memory(endpoint: &str, certificate_bytes: &[u8], private_key_bytes: &[u8], root_ca_bytes: Option<&[u8]>) -> Mqtt5Result<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            client_options: None,
            inner_builder: ClientBuilder::new_with_mtls_from_memory(endpoint, DEFAULT_PORT, certificate_bytes, private_key_bytes, root_ca_bytes)?
        };

        builder.set_alpn(DIRECT_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn new_direct_with_unsigned_custom_auth(endpoint: &str, custom_auth_options: AwsCustomAuthUnsignedOptions, root_ca_path: Option<&str>) -> Mqtt5Result<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::CustomAuth,
            custom_auth_options: Some(AwsCustomAuthOptions::Unsigned(custom_auth_options)),
            client_options: None,
            inner_builder: ClientBuilder::new_with_tls(endpoint, DEFAULT_PORT, root_ca_path)?
        };

        builder.set_alpn(CUSTOM_AUTH_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn new_direct_with_signed_custom_auth(endpoint: &str, custom_auth_options: AwsCustomAuthSignedOptions, root_ca_path: Option<&str>) -> Mqtt5Result<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::CustomAuth,
            custom_auth_options: Some(AwsCustomAuthOptions::Signed(custom_auth_options)),
            client_options: None,
            inner_builder: ClientBuilder::new_with_tls(endpoint, DEFAULT_PORT, root_ca_path)?
        };

        builder.set_alpn(CUSTOM_AUTH_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn with_client_options(mut self, client_options: Mqtt5ClientOptions) -> Self {
        self.client_options = Some(client_options);
        self
    }

    pub fn build(mut self, runtime: &Handle) -> Mqtt5Result<Mqtt5Client> {
        if self.client_options.is_none() {
            self.client_options = Some(Mqtt5ClientOptionsBuilder::new().build());
        }

        let mut client_options = self.client_options.take().unwrap();
        self.apply_custom_auth_options_to_client_options(&mut client_options);

        self.inner_builder = self.inner_builder.with_client_options(client_options);
        self.inner_builder.build(runtime)
    }

    fn apply_custom_auth_options_to_client_options(&self, client_options: &mut Mqtt5ClientOptions) {
        if self.auth_type != AuthType::CustomAuth {
            return;
        }

        match &self.custom_auth_options {
            Some(AwsCustomAuthOptions::Unsigned(options)) => {
                client_options.connect_options.username = options.build_username();
                client_options.connect_options.password = options.build_password();
            }
            Some(AwsCustomAuthOptions::Signed(options)) => {
                client_options.connect_options.username = options.build_username();
                client_options.connect_options.password = options.build_password();
            }
            _ => {}
        }
    }

    fn set_alpn(&mut self, protocol_name: &[u8]) {
        self.clear_alpn();
        self.inner_builder.get_mut_tls_config().unwrap().alpn_protocols.push(protocol_name.to_vec());
    }

    fn clear_alpn(&mut self) {
        self.inner_builder.get_mut_tls_config().unwrap().alpn_protocols.clear();
    }
}