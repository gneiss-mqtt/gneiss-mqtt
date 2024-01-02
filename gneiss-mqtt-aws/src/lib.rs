/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate gneiss_mqtt;
extern crate tokio;

use gneiss_mqtt::client::builder::{ClientBuilder};
use gneiss_mqtt::{ConnectOptions, ConnectOptionsBuilder, Mqtt5Client, Mqtt5ClientOptions, Mqtt5ClientOptionsBuilder};
use gneiss_mqtt::{MqttResult};
use tokio::runtime::Handle;

const CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-name";
const CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-signature";

pub struct AwsCustomAuthOptions {
    username: String,
    password: Option<Vec<u8>>
}

impl AwsCustomAuthOptions {
    pub fn new_unsigned(authorizer_name: &str, username: Option<&str>, password: Option<&[u8]>) -> Self {
        AwsCustomAuthOptions {
            username: format!("{}?{}={}", username.unwrap_or(""), CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME, authorizer_name),
            password: password.map(|p| p.to_vec())
        }
    }

    pub fn new_signed(authorizer_name: &str, authorizer_signature: &str, authorizer_token_key_name: &str, authorizer_token_key_value: &str, username: Option<&str>, password: Option<&[u8]>) -> Self {
        AwsCustomAuthOptions {
            username: format!("{}?{}={}&{}={}&{}={}",
                username.unwrap_or(""),
                CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME,
                authorizer_name,
                CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME,
                authorizer_signature,
                authorizer_token_key_name,
                authorizer_token_key_value),
            password: password.map(|p| p.to_vec())
        }
    }

    pub(crate) fn get_username(&self) -> &str {
        self.username.as_str()
    }

    pub(crate) fn get_password(&self) -> Option<&[u8]> {
        self.password.as_deref()
    }
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
    connect_options: Option<ConnectOptions>,
    client_options: Option<Mqtt5ClientOptions>,
    inner_builder: ClientBuilder
}

const ALPN_PORT : u16 = 443;
const DEFAULT_PORT : u16 = ALPN_PORT;
const DIRECT_ALPN_PROTOCOL : &[u8] = b"x-amzn-mqtt-ca";
const CUSTOM_AUTH_ALPN_PROTOCOL : &[u8] = b"mqtt";

impl AwsClientBuilder {
    pub fn new_direct_with_mtls_from_fs(endpoint: &str, certificate_path: &str, private_key_path: &str, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            inner_builder: ClientBuilder::new_with_mtls_from_fs(endpoint, DEFAULT_PORT, certificate_path, private_key_path, root_ca_path)?
        };

        builder.set_alpn(DIRECT_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn new_direct_with_mtls_from_memory(endpoint: &str, certificate_bytes: &[u8], private_key_bytes: &[u8], root_ca_bytes: Option<&[u8]>) -> MqttResult<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            inner_builder: ClientBuilder::new_with_mtls_from_memory(endpoint, DEFAULT_PORT, certificate_bytes, private_key_bytes, root_ca_bytes)?
        };

        builder.set_alpn(DIRECT_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn new_direct_with_unsigned_custom_auth(endpoint: &str, custom_auth_options: AwsCustomAuthOptions, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::CustomAuth,
            custom_auth_options: Some(custom_auth_options),
            connect_options: None,
            client_options: None,
            inner_builder: ClientBuilder::new_with_tls(endpoint, DEFAULT_PORT, root_ca_path)?
        };

        builder.set_alpn(CUSTOM_AUTH_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn new_direct_with_signed_custom_auth(endpoint: &str, custom_auth_options: AwsCustomAuthOptions, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let mut builder =  AwsClientBuilder {
            auth_type: AuthType::CustomAuth,
            custom_auth_options: Some(custom_auth_options),
            connect_options: None,
            client_options: None,
            inner_builder: ClientBuilder::new_with_tls(endpoint, DEFAULT_PORT, root_ca_path)?
        };

        builder.set_alpn(CUSTOM_AUTH_ALPN_PROTOCOL);

        Ok(builder)
    }

    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.connect_options = Some(connect_options);
        self
    }

    pub fn with_client_options(mut self, client_options: Mqtt5ClientOptions) -> Self {
        self.client_options = Some(client_options);
        self
    }

    pub fn build(mut self, runtime: &Handle) -> MqttResult<Mqtt5Client> {
        if self.connect_options.is_none() {
            self.connect_options = Some(ConnectOptionsBuilder::new().build());
        }
        if self.client_options.is_none() {
            self.client_options = Some(Mqtt5ClientOptionsBuilder::new().build());
        }

        let mut connect_options = self.connect_options.take().unwrap();
        self.apply_custom_auth_options_to_connect_options(&mut connect_options);

        self.inner_builder = self.inner_builder.with_connect_options(connect_options);
        self.inner_builder = self.inner_builder.with_client_options(self.client_options.take().unwrap());
        self.inner_builder.build(runtime)
    }

    fn apply_custom_auth_options_to_connect_options(&self, connect_options: &mut ConnectOptions) {
        if self.auth_type != AuthType::CustomAuth {
            return;
        }

        if let Some(options) = &self.custom_auth_options {
            connect_options.set_username(Some(options.get_username()));
            connect_options.set_password(options.get_password());
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