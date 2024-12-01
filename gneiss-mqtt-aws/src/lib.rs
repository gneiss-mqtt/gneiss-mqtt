/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides a builder API for creating MQTT clients that connect to AWS IoT Core, an
AWS-managed message broker that supports both MQTT5 and MQTT311.  This crate depends on
[`gneiss-mqtt`](https://crates.io/crates/gneiss-mqtt),
which contains the MQTT client implementations.

IoT Core supports three different ways to securely establish and authenticate an MQTT connection:
* MQTT over mTLS - via an X509 certificate (registered with AWS IoT Core) and its associated private key
* MQTT over Websockets - sign the websocket upgrade request with AWS credentials using the Sigv4 signing algorithm
* MQTT with Custom Authentication - invoke an AWS Lambda with data fields passed via the MQTT username and password fields of the Connect packet

This crate's builder does all the dirty work for each of these connection methods, letting you
just supply the required data.

# Feature Flags

Gneiss-mqtt supports two different MQTT clients:
* `tokio` - An asynchronous client that executes on a Tokio async runtime.  Intended for users
comfortable with async programming in Rust.
* `threaded` - A callback-based client that executes on a background thread.  Intended for users
that do not wish to deal with the additional complexity (or dependencies) of async programming in
Rust.

Client support is controlled by enabling one or more of these crate features:

- `tokio-rustls`: Enables the tokio client and establishing connections using the `rustls` TLS implementation.
- `tokio-native-tls`: Enables the tokio client and establishing connections using the `native-tls` TLS implementation.
- `tokio-websockets`: Enables the tokio client and transport over websockets.
- `threaded-rustls`: Enables the thread-based client and establishing connections using the `rustls` TLS implementation.
- `threaded-native-tls`: Enables the thread-based client and establishing connections using the `native-tls` TLS implementation.

AWS IoT Core requires TLS to connect and the crate will not build unless at least one TLS feature
is enabled.

# Usage

To use this crate, you'll first need to add it and [`gneiss-mqtt`](https://crates.io/crates/gneiss-mqtt) to your project's Cargo.toml,
enabling a TLS implementation feature as well.

Enabling the tokio client:

```toml
[dependencies]
gneiss-mqtt = { version = "...", features = [ "tokio-rustls" ] }
gneiss-mqtt-aws = { version = "...", features = [ "tokio-rustls" ] }
tokio = { version = "1", features = ["full"] }
```

Enabling the thread-based client:

```toml
[dependencies]
gneiss-mqtt = { version = "...", features = [ "threaded-rustls" ] }
gneiss-mqtt-aws = { version = "...", features = [ "threaded-rustls" ] }
```

Standalone examples for each connection method can be found in the project repository
[examples folder](https://github.com/gneiss-mqtt/gneiss-mqtt/blob/main/gneiss-mqtt-aws/examples/README.md).

*/

#![cfg_attr(any(feature = "tokio-rustls", feature = "tokio-native-tls"), doc = r##"
## Example: Connect to AWS IoT Core via mTLS with a tokio-based client

You'll need to create and register an X509 device certificate with IoT Core and associate an IAM
permission policy that allows IoT Core connections.  See
[X509 Certificates and AWS IoT Core](https://docs.aws.amazon.com/iot/latest/developerguide/x509-client-certs.html)
for guidance on this process.

To create a client and connect:

```no_run
use gneiss_mqtt::client::AsyncClient;
use gneiss_mqtt_aws::AwsClientBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = "<your AWS IoT Core endpoint>";
    let cert_path = "<path to your X509 certificate>";
    let key_path = "<path to the certificate's private key>";

    let client =
        AwsClientBuilder::new_direct_with_mtls_from_fs(endpoint, cert_path, key_path, None)?
            .build_tokio()?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start(None)?;

    // <do stuff with the client>

    Ok(())
}
```"##)]

#![cfg_attr(all(any(feature = "tokio-rustls", feature = "tokio-native-tls"), feature = "tokio-websockets"), doc = r##"
## Example: Connect to AWS IoT Core via Websockets with a tokio-based client
You'll need to configure your runtime environment to source AWS credentials whose IAM policy allows
IoT usage.  This crate uses the AWS SDK for Rust to source the credentials necessary
to sign the websocket upgrade request.  Consult
[AWS documentation](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html) for more
details.

To create a client and connect:

```no_run
use gneiss_mqtt::client::AsyncClient;
use gneiss_mqtt_aws::{AwsClientBuilder, WebsocketSigv4OptionsBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use gneiss_mqtt_aws::WebsocketSigv4Options;let endpoint = "<your AWS IoT Core endpoint>";
    let signing_region = "<AWS region for endpoint>";

    // Creating a default credentials provider chain is an async operation
    let sigv4_options = WebsocketSigv4OptionsBuilder::new(signing_region).await.build();

    let client =
        AwsClientBuilder::new_websockets_with_sigv4(endpoint, sigv4_options, None)?
            .build_tokio()?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start(None)?;

    // <do stuff with the client>

    Ok(())
}
```"##)]

/*!
## Example: Connect to AWS IoT Core via AWS IoT Custom Authentication with the thread-based client

Custom authentication is an AWS IoT Core specific way to perform authentication without using
certificates or http request signing.  Instead, an AWS Lambda is invoked to decide whether or
not a connection is allowed.  The lambda function can access CONNECT packet fields (username
and password) to determine whether the connection should be allowed and what permissions it
should have. See the
[custom authentication documentation](https://docs.aws.amazon.com/iot/latest/developerguide/custom-authentication.html)
for step-by-step instructions in how to set up the AWS resources (authorizer, Lambda, etc...) to
perform custom authentication.

Once the necessary AWS resources have been set up, you can easily create clients for each of the two
supported custom authentication modes:

* Unsigned Custom Authentication - Anyone can invoke the authorizer's lambda if they know its ARN.  This is not recommended for production since it is not protected from external abuse that may run up your AWS bill.
* Signed Custom Authentication - Your Lambda function will only be invoked (and billed) if the Connect packet includes the cryptographic signature (based on an IoT Core registered public key) of a controllable value.  Recommended for production.
*/
#![cfg_attr(any(feature = "threaded-rustls", feature = "threaded-native-tls"), doc = r##"
### Unsigned Custom Authentication

For an unsigned custom authorizer (for testing/internal purposes only, not recommended for production):

```no_run
use gneiss_mqtt::client::SyncClient;
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = "<your AWS IoT Core endpoint>";
    let authorizer_name = "<name of the authorizer you want to invoke>";
    let username = "<username value to pass to the authorizer>"; // only necessary if the authorizer's lambda uses it
    let password = "<password value to pass to the authorizer>".as_bytes(); // only necessary if the authorizer's lambda uses it

    let mut custom_auth_options_builder = AwsCustomAuthOptions::builder_unsigned(
        Some(authorizer_name)
    );

    custom_auth_options_builder.with_username(username);
    custom_auth_options_builder.with_password(password);

    // In the common case, you will not need a root CA certificate
    let client =
        AwsClientBuilder::new_direct_with_custom_auth(endpoint, custom_auth_options_builder.build(), None)?
            .build_threaded()?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start(None)?;

    // <do stuff with the client>

    Ok(())
}
```"##)]
#![cfg_attr(any(feature = "threaded-rustls", feature = "threaded-native-tls"), doc = r##"
### Signed Custom Authentication

For a signed custom authorizer (recommended for production):

```no_run
use gneiss_mqtt::client::SyncClient;
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = "<your AWS IoT Core endpoint>";
    let authorizer_name = "<name of the authorizer you want to invoke>";
    let authorizer_token_key_name = "<key name registered with the signing authorizer that indicates the name of the field whose value will contain the `authorizer_token_key_value`>";
    let authorizer_token_key_value = "<An arbitrary value.  The (Base64-encoded) signature of this value (using the private key of the public key associated with the authorizer) must be included as a separate field>";
    let authorizer_signature = "<URI-encoded Base64-encoded signature for `authorizer_token_key_value` signed by the private key of the public key associated with the authorizer>";
    let username = "<username value to pass to the authorizer>"; // only necessary if the authorizer's lambda uses it
    let password = "<password value to pass to the authorizer>".as_bytes(); // only necessary if the authorizer's lambda uses it

    let mut custom_auth_options_builder = AwsCustomAuthOptions::builder_signed(
        Some(authorizer_name),
        authorizer_signature,
        authorizer_token_key_name,
        authorizer_token_key_value
    );

    custom_auth_options_builder.with_username(username);
    custom_auth_options_builder.with_password(password);

    // In the common case, you will not need a root CA certificate
    let client =
        AwsClientBuilder::new_direct_with_custom_auth(endpoint, custom_auth_options_builder.build(), None)?
            .build_threaded()?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start(None)?;

    // <do stuff with the client>

    Ok(())
}
```"##)]
/*!
You must be careful with the encodings of `authorizer`, `authorizer_signature`, and
`authorizer_token_key_name`.  Because
custom authentication is supported over HTTP, these values must be URI-safe.  It is up to
you to URI encode them if necessary.  In general, `authorizer` and `authorizer_token_key_name` are
fixed when you create
the authorizer resource and so it is
straightforward to determine if you need to encode them or not.  `authorizer_signature` should
always be URI encoded.

# MQTT Client Configuration

The above examples skip all client configuration in favor of defaults.  There are many configuration
details that may be of interest depending on your use case.  These options are controlled by
structures in the `gneiss-mqtt` crate, via the `with_client_options` and `with_connect_options`
methods on the [`AwsClientBuilder`](https://docs.rs/gneiss-mqtt-aws/latest/gneiss_mqtt-aws/struct.AwsClientBuilder.html).
Further details can be found in the relevant sections of the `gneiss-mqtt` docs:
* [`MqttClientOptions`](https://docs.rs/gneiss-mqtt/latest/gneiss_mqtt/config/struct.MqttClientOptionsBuilder.html)
* [`ConnectOptions`](https://docs.rs/gneiss-mqtt/latest/gneiss_mqtt/config/struct.ConnectOptionsBuilder.html)

 */

#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(feature = "strict", deny(warnings))]
#![warn(missing_docs)]

use gneiss_mqtt::client::*;
use gneiss_mqtt::client::config::*;
#[allow(unused_imports)]
use gneiss_mqtt::error::{GneissError, GneissResult};
use std::fmt::Write;

#[cfg(feature = "tokio-websockets")]
use aws_credential_types::provider::ProvideCredentials;
#[cfg(feature = "tokio-websockets")]
use aws_sigv4::http_request::{SessionTokenMode, sign, SignableBody, SignableRequest, SignatureLocation};
#[cfg(feature = "tokio-websockets")]
use aws_sigv4::sign::v4;
#[cfg(feature = "tokio-websockets")]
use aws_smithy_runtime_api::client::identity::Identity;
#[cfg(feature = "tokio-websockets")]
use std::time::{Duration, SystemTime};

/// Struct holding all configuration relevant to connecting an MQTT client to AWS IoT Core
/// over websockets using a Sigv4-signed websocket handshake for authentication
#[derive(Clone, Debug)]
#[cfg(feature = "tokio-websockets")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-websockets")))]
pub struct WebsocketSigv4Options {
    signing_region: String,
    credentials_provider: std::sync::Arc<dyn ProvideCredentials>
}

/// A builder type that configures all relevant AWS signing options for connecting over websockets
/// using Sigv4 request signing.
#[cfg(feature = "tokio-websockets")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio-websockets")))]
pub struct WebsocketSigv4OptionsBuilder {
    options: WebsocketSigv4Options
}

#[cfg(feature = "tokio-websockets")]
impl WebsocketSigv4OptionsBuilder {

    /// Creates a new builder.
    ///
    /// AWS credentials will be sourced from the default credentials provider chain as
    /// implemented by the [AWS SDK for Rust](https://aws.amazon.com/sdk-for-rust/).  Construction
    /// of this provider chain is asynchronous, hence this factory function is also asynchronous.
    ///
    /// `signing_region` - the AWS region to sign the websocket handshake with.
    pub async fn new(signing_region: &str) -> Self {
        let region = aws_config::Region::new(signing_region.to_string());
        let mut provider_builder = aws_config::default_provider::credentials::Builder::default();
        provider_builder.set_region(Some(region));

        let default_provider_chain = std::sync::Arc::new(provider_builder.build().await);

        WebsocketSigv4OptionsBuilder {
            options: WebsocketSigv4Options {
                signing_region: signing_region.to_string(),
                credentials_provider: default_provider_chain
            }
        }
    }

    /// Creates a new builder.
    ///
    /// `credentials_provider` - credentials provider to
    /// source the AWS credentials needed to sign the websocket handshake upgrade request.
    ///
    /// `signing_region` - the AWS region to sign the websocket handshake with.
    pub fn new_with_credentials_provider(credentials_provider: Box<dyn ProvideCredentials>, signing_region: &str) -> Self {
        WebsocketSigv4OptionsBuilder {
            options: WebsocketSigv4Options {
                signing_region: signing_region.to_string(),
                credentials_provider: std::sync::Arc::from(credentials_provider)
            }
        }
    }

    /// Creates a new instance of WebsocketSigv4Options based on the builder's configuration.
    pub fn build(&self) -> WebsocketSigv4Options {
        self.options.clone()
    }
}

const CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-name";
const CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-signature";

///  A struct that holds all relevant details needed to perform custom authentication with
/// AWS IoT Core.
pub struct AwsCustomAuthOptions {
    pub(crate) username: String,
    pub(crate) password: Option<Vec<u8>>
}

impl AwsCustomAuthOptions {

    /// Creates a new custom authentication options builder to use an unsigned authorizer.
    ///
    /// `authorizer_name` - name of the AWS IoT authorizer to use.  This value must be URI-encoded if necessary.  A
    /// value is required unless the AWS account has a default authorizer configured for it.
    pub fn builder_unsigned(authorizer_name: Option<&str>) -> AwsCustomAuthOptionsBuilder {
        AwsCustomAuthOptionsBuilder::new_unsigned(authorizer_name)
    }

    /// Creates a new custom authentication options builder to use a signed authorizer. See
    /// [AWS IoT Custom Authentication](https://docs.aws.amazon.com/iot/latest/developerguide/custom-authentication.html)
    /// for more details.  The authenticator's Lambda will not be invoked unless `authorizer_signature`
    /// is the URI-encoded Base64-encoded signature of `authorizer_token_key_value` via the private key
    /// associated with the public key that was registered with the authorizer on creation.
    ///
    /// `authorizer_name` - name of the AWS IoT authorizer to use.  This value must be URI-encoded if necessary. A
    /// value is required unless the AWS account has a default authorizer configured for it.
    ///
    /// `authorizer_signature` - The URI-encoded, Base64-encoded cryptographic signature of the value contained in `authorizer_token_key_value`.  The signature must be
    /// made with the private key associated with the public key that was registered with the authorizer.
    ///
    /// `authorizer_token_key_name` - key name registered with the signing authorizer that indicates the name of the field whose value will contain the `authorizer_token_key_value`
    ///
    /// `authorizer_token_key_value` - arbitrary, developer-selected value whose digital signature must be provided in the `authorizer_signature`
    ///
    /// Both `authorizer_token_key_name` and `authorizer_name` must be valid URI-encoded values.
    pub fn builder_signed(authorizer_name: Option<&str>, authorizer_signature: &str, authorizer_token_key_name: &str, authorizer_token_key_value: &str) -> AwsCustomAuthOptionsBuilder {
        AwsCustomAuthOptionsBuilder::new_signed(authorizer_name, authorizer_signature, authorizer_token_key_name, authorizer_token_key_value)
    }
}

/// Builder type for AwsCustomAuthOptions
pub struct AwsCustomAuthOptionsBuilder {
    authorizer_name: Option<String>,
    authorizer_signature: Option<String>,
    authorizer_token_key_name: Option<String>,
    authorizer_token_key_value: Option<String>,
    username: Option<String>,
    password: Option<Vec<u8>>
}

impl AwsCustomAuthOptionsBuilder {

    pub(crate) fn new_unsigned(authorizer_name: Option<&str>) -> Self {
        AwsCustomAuthOptionsBuilder {
            authorizer_name: authorizer_name.map(|name| { name.to_string() }),
            authorizer_signature: None,
            authorizer_token_key_name: None,
            authorizer_token_key_value: None,
            username: None,
            password: None
        }
    }

    pub(crate) fn new_signed(authorizer_name: Option<&str>, authorizer_signature: &str, authorizer_token_key_name: &str, authorizer_token_key_value: &str) -> Self {
        AwsCustomAuthOptionsBuilder {
            authorizer_name: authorizer_name.map(|name| { name.to_string() }),
            authorizer_signature: Some(authorizer_signature.to_string()),
            authorizer_token_key_name: Some(authorizer_token_key_name.to_string()),
            authorizer_token_key_value: Some(authorizer_token_key_value.to_string()),
            username: None,
            password: None
        }
    }

    /// Sets additional data to pass to the authorizer's Lambda function via the `protocolData.mqtt.username` field.
    ///
    /// `username` - text data to pass to the authorizer's Lambda via the `protocolData.mqtt.username` field.
    ///
    /// It is strongly advised to either URI encode this value or make sure it does not contain characters that
    /// need to be URI-encoded.
    pub fn with_username(&mut self, username: &str) -> &mut Self {
        self.username = Some(username.to_string());
        self
    }

    /// Sets additional data to pass to the authorizer's Lambda function via the `protocolData.mqtt.password` field.
    ///
    /// `password` - binary data to pass to the authorizer's Lambda function via the `protocolData.mqtt.password` field
    pub fn with_password(&mut self, password: &[u8]) -> &mut Self {
        self.password = Some(password.to_vec());
        self
    }

    fn build_query_params(&self) -> Vec<String> {
        let mut params = Vec::new();

        if let Some(authorizer_name) = &self.authorizer_name {
            params.push(format!("{}={}", CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME, authorizer_name.clone()));
        }

        if let Some(authorizer_signature) = &self.authorizer_signature {
            let final_signature =
                if !authorizer_signature.contains('%') {
                    urlencoding::encode(authorizer_signature).to_string()
                } else {
                    authorizer_signature.clone()
                };

            params.push(format!("{}={}", CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME, final_signature));
        }

        if let Some(authorizer_token_key_name) = &self.authorizer_token_key_name {
            params.push(format!("{}={}", authorizer_token_key_name.clone(), self.authorizer_token_key_value.as_ref().unwrap().clone()));
        }

        params
    }

    /// Builds a new set of custom auth options from the builder's configuration.
    pub fn build(&self) -> AwsCustomAuthOptions {
        let mut final_username : String = "".to_string();

        if let Some(username) = &self.username {
            write!(&mut final_username, "{}", username).ok();
        }
        write!(&mut final_username, "?").ok();

        let query_params = self.build_query_params();
        write!(&mut final_username, "{}", query_params.join("&")).ok();

        AwsCustomAuthOptions {
            username: final_username,
            password: self.password.clone(),
        }
    }
}

/// This enumeration allows the user to override the default TLS implementation in the unfortunate case
/// that they are forced to build the crate with multiple TLS implementations enabled.
///
/// Rustls, if enabled, is the default TLS implementation.
///
/// You will probably never need to set this.
#[non_exhaustive]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TlsImplementation {
    /// Use the default TLS implementation, rustls
    Default,

    /// Use rustls as the TLS implementation
    #[cfg(any(feature = "tokio-rustls", feature="threaded-rustls"))]
    Rustls,

    /// Use native-tls as the TLS implementation
    #[cfg(any(feature = "tokio-native-tls", feature="threaded-native-tls"))]
    Nativetls,
}

#[derive(PartialEq, Eq)]
enum AuthType {
    Mtls,
    #[cfg(feature = "tokio-websockets")]
    Sigv4Websockets,
    CustomAuth
}

/// A builder object that allows for easy MQTT client construction for all supported
/// connection methods to AWS IoT Core.
pub struct AwsClientBuilder {
    #[allow(dead_code)]
    auth_type: AuthType,
    custom_auth_options: Option<AwsCustomAuthOptions>,
    connect_options: Option<ConnectOptions>,
    client_options: Option<MqttClientOptions>,
    tls_options_builder: TlsOptionsBuilder,
    #[cfg(feature = "tokio")]
    tokio_options: Option<TokioOptions>,
    #[cfg(feature = "tokio-websockets")]
    websocket_sigv4_options: Option<WebsocketSigv4Options>,
    #[cfg(feature = "threaded")]
    threaded_options: Option<ThreadedOptions>,
    endpoint: String,
    tls_impl: TlsImplementation
}

const ALPN_PORT : u16 = 443;
const DEFAULT_PORT : u16 = ALPN_PORT;
const DIRECT_ALPN_PROTOCOL : &str = "x-amzn-mqtt-ca";
const CUSTOM_AUTH_ALPN_PROTOCOL : &str = "mqtt";

impl AwsClientBuilder {

    /// Creates a new builder that will construct an MQTT client that connects to AWS IoT Core
    /// using mutual TLS where the certificate and private key are read from the filesystem.
    ///
    /// `certificate_path` - path to a PEM-encoded file of the X509 certificate to use in mTLS
    ///
    /// `private_key_path` - path to a PEM-encoded file of the private key associated with the X509 certificate in `certificate_path`
    ///
    /// `root_ca_path` - path to a root CA to use in the TLS context of the connection.  Usually
    /// not needed unless a custom domain is involved.
    pub fn new_direct_with_mtls_from_fs(endpoint: &str, certificate_path: &str, private_key_path: &str, root_ca_path: Option<&str>) -> GneissResult<Self> {
        let mut tls_options_builder = TlsOptions::builder_with_mtls_from_path(certificate_path, private_key_path)?;
        if let Some(root_ca) = root_ca_path {
            tls_options_builder.with_root_ca_from_path(root_ca)?;
        }
        tls_options_builder.with_alpn(DIRECT_ALPN_PROTOCOL);

        let builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            tls_options_builder,
            #[cfg(feature = "tokio")]
            tokio_options: None,
            #[cfg(feature = "tokio-websockets")]
            websocket_sigv4_options: None,
            #[cfg(feature = "threaded")]
            threaded_options: None,
            endpoint: endpoint.to_string(),
            tls_impl: TlsImplementation::Default,
        };

        Ok(builder)
    }

    /// Creates a new builder that will construct an MQTT client that connects to AWS IoT Core
    /// using mutual TLS where the certificate and private key are read from memory.
    ///
    /// `certificate_bytes` - PEM-encoded bytes of the X509 certificate to use in mTLS
    ///
    /// `private_key_bytes` - PEM-encoded bytes of the private key associated with the X509 certificate in `certificate_bytes`
    ///
    /// `root_ca_bytes` - root CA PEM data to use in the TLS context of the connection.  Usually
    /// not needed unless a custom domain is involved.
    pub fn new_direct_with_mtls_from_memory(endpoint: &str, certificate_bytes: &[u8], private_key_bytes: &[u8], root_ca_bytes: Option<&[u8]>) -> GneissResult<Self> {
        let mut tls_options_builder = TlsOptions::builder_with_mtls_from_memory(certificate_bytes, private_key_bytes);
        if let Some(root_ca) = root_ca_bytes {
            tls_options_builder.with_root_ca_from_memory(root_ca);
        }
        tls_options_builder.with_alpn(DIRECT_ALPN_PROTOCOL);

        let builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            tls_options_builder,
            #[cfg(feature = "tokio")]
            tokio_options: None,
            #[cfg(feature = "tokio-websockets")]
            websocket_sigv4_options: None,
            #[cfg(feature = "threaded")]
            threaded_options: None,
            endpoint: endpoint.to_string(),
            tls_impl: TlsImplementation::Default,
        };

        Ok(builder)
    }

    /// Creates a new builder that will construct an MQTT client that connects to AWS IoT Core
    /// using custom authentication.
    ///
    /// `custom_auth_options` - custom authentication options to use while connecting
    ///
    /// `root_ca_path` - path to a root CA to use in the TLS context of the connection.  Usually
    /// not needed unless a custom domain is involved.
    pub fn new_direct_with_custom_auth(endpoint: &str, custom_auth_options: AwsCustomAuthOptions, root_ca_path: Option<&str>) -> GneissResult<Self> {
        let mut tls_options_builder = TlsOptions::builder();
        if let Some(root_ca) = root_ca_path {
            tls_options_builder.with_root_ca_from_path(root_ca)?;
        }
        tls_options_builder.with_alpn(CUSTOM_AUTH_ALPN_PROTOCOL);

        let builder =  AwsClientBuilder {
            auth_type: AuthType::CustomAuth,
            custom_auth_options: Some(custom_auth_options),
            connect_options: None,
            client_options: None,
            tls_options_builder,
            #[cfg(feature = "tokio")]
            tokio_options: None,
            #[cfg(feature = "tokio-websockets")]
            websocket_sigv4_options: None,
            #[cfg(feature = "threaded")]
            threaded_options: None,
            endpoint: endpoint.to_string(),
            tls_impl: TlsImplementation::Default,
        };

        Ok(builder)
    }

    /// Creates a new builder that will construct an MQTT client that will connect to AWS IoT Core
    /// over websockets using AWS Sigv4 request signing for authentication.
    ///
    /// `sigv4_options` - sigv4 signing options to use while connecting
    ///
    /// `root_ca_path` - path to a root CA to use in the TLS context of the connection.  Usually
    /// not needed unless a custom domain is involved.
    #[cfg(feature = "tokio-websockets")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio-websockets")))]
    pub fn new_websockets_with_sigv4(endpoint: &str, sigv4_options: WebsocketSigv4Options, root_ca_path: Option<&str>) -> GneissResult<Self> {
        let mut tls_options_builder = TlsOptions::builder();
        if let Some(root_ca) = root_ca_path {
            tls_options_builder.with_root_ca_from_path(root_ca)?;
        }

        let builder =  AwsClientBuilder {
            auth_type: AuthType::Sigv4Websockets,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            tls_options_builder,
            #[cfg(feature = "tokio")]
            tokio_options: None,
            #[cfg(feature = "tokio-websockets")]
            websocket_sigv4_options: Some(sigv4_options),
            #[cfg(feature = "threaded")]
            threaded_options: None,
            endpoint: endpoint.to_string(),
            tls_impl: TlsImplementation::Default,
        };

        Ok(builder)
    }

    /// Sets configuration options that control the MQTT Connect packet sent by the client on
    /// every connection attempt.
    ///
    /// `connect_options` - CONNECT packet configuration options
    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.connect_options = Some(connect_options);
        self
    }

    /// Sets behavioral configuration options for the client.
    ///
    /// `client_options` - behavioral configuration options
    pub fn with_client_options(mut self, client_options: MqttClientOptions) -> Self {
        self.client_options = Some(client_options);
        self
    }

    /// Overrides the default TLS implementation (rustls) used by the client.
    ///
    /// `tls_impl` - what TLS implementation to use when establishing connections
    ///
    /// Only useful if multiple TLS implementations are enabled.
    pub fn with_default_tls_implementation(mut self, tls_impl: TlsImplementation) -> Self {
        self.tls_impl = tls_impl;
        self
    }

    /// Sets tokio-related configuration options for the client.
    ///
    /// `tokio_options` - client Tokio options (primarily what runtime instance to use)
    ///
    /// Only relevant to tokio-based clients.  If left unspecified, created clients will use the
    /// tokio runtime that was current at time of creation.
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio-websockets", feature = "tokio-rustls", feature = "tokio-native-tls"))))]
    pub fn with_tokio_options(mut self, tokio_options: TokioOptions) -> Self {
        self.tokio_options = Some(tokio_options);
        self
    }

    /// Sets thread-related configuration options for the client.
    ///
    /// `threaded_options` - threaded client configuration options
    ///
    /// Only relevant to thread-based clients.
    #[cfg(feature = "threaded")]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "threaded-rustls", feature = "threaded-native-tls"))))]
    pub fn with_threaded_options(mut self, threaded_options: ThreadedOptions) -> Self {
        self.threaded_options = Some(threaded_options);
        self
    }

    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio-websockets", feature = "tokio-rustls", feature = "tokio-native-tls"))))]
    /// Creates a new tokio-based MQTT client from the configuration options registered with the
    /// builder.
    pub fn build_tokio(&self) -> GneissResult<AsyncClientHandle> {
        let user_connect_options =
            if let Some(options) = &self.connect_options {
                options.clone()
            } else {
                ConnectOptions::builder().build()
            };

        let final_connect_options = self.build_final_connect_options(user_connect_options);

        let client_options =
            if let Some(options) = &self.client_options {
                options.clone()
            } else {
                MqttClientOptions::builder().build()
            };

        let tls_options = self.build_tls_options()?;

        let mut builder = TokioClientBuilder::new(self.endpoint.as_str(), DEFAULT_PORT);
        builder.with_connect_options(final_connect_options)
            .with_client_options(client_options)
            .with_tls_options(tls_options);

        #[cfg(feature = "tokio-websockets")]
        if self.auth_type == AuthType::Sigv4Websockets {
            let sigv4_options = self.websocket_sigv4_options.as_ref().unwrap().clone();

            let signing_region = sigv4_options.signing_region.clone();
            let credentials_provider = sigv4_options.credentials_provider.clone();

            let mut websocket_options_builder = AsyncWebsocketOptions::builder();
            websocket_options_builder.with_handshake_transform(Box::new(move |request_builder| {
                Box::pin(sign_websocket_upgrade_sigv4(request_builder, signing_region.clone(), credentials_provider.clone()))
            }));

            let websocket_options = websocket_options_builder.build();

            builder.with_websocket_options(websocket_options);
        }

        if self.tokio_options.is_some() {
            builder.with_tokio_options(self.tokio_options.as_ref().unwrap().clone());
        }

        builder.build()
    }

    #[cfg(feature = "threaded")]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "threaded-rustls", feature = "threaded-native-tls"))))]
    /// Creates a new thread-based MQTT client from the configuration options registered with the
    /// builder.
    pub fn build_threaded(&self) -> GneissResult<SyncClientHandle> {
        let user_connect_options =
            if let Some(options) = &self.connect_options {
                options.clone()
            } else {
                ConnectOptions::builder().build()
            };

        let final_connect_options = self.build_final_connect_options(user_connect_options);

        let client_options =
            if let Some(options) = &self.client_options {
                options.clone()
            } else {
                MqttClientOptions::builder().build()
            };

        let tls_options = self.build_tls_options()?;

        let mut builder = ThreadedClientBuilder::new(self.endpoint.as_str(), DEFAULT_PORT);
        builder.with_connect_options(final_connect_options)
            .with_client_options(client_options)
            .with_tls_options(tls_options);

        if self.threaded_options.is_some() {
            builder.with_threaded_options(self.threaded_options.as_ref().unwrap().clone());
        }

        builder.build()
    }

    fn build_final_connect_options(&self, connect_options: ConnectOptions) -> ConnectOptions {
        let is_auto_assigned_client_id = connect_options.client_id().is_none();
        let mut final_connect_options_builder = ConnectOptions::builder_from_existing(connect_options);

        if let Some(options) = &self.custom_auth_options {
            final_connect_options_builder.with_username(options.username.as_str());
            if let Some(password) = &options.password {
                final_connect_options_builder.with_password(password.as_slice());
            }
        }

        // Until IotCore fixes their client id generation process to make client ids that
        // are valid on reconnect, you cannot use auto-assigned client ids safely.  So if
        // the configuration indicates that, just generate a UUID instead of leaving it empty.
        if is_auto_assigned_client_id {
            let uuid = uuid::Uuid::new_v4();
            final_connect_options_builder.with_client_id(uuid.to_string().as_str());
        }

        final_connect_options_builder.build()
    }

    #[cfg(not(any(feature = "tokio-rustls", feature = "tokio-native-tls", feature = "threaded-rustls", feature = "threaded-native-tls")))]
    fn build_tls_options(&self) -> GneissResult<TlsOptions> {
        compile_error!("gneiss-mqtt-aws must be built with a TLS feature (rustls, native-tls) enabled");
        Err(GneissError::new_tls_error("Connecting to AWS IoT Core requires a TLS implementation feature to be configured"))
    }

    #[cfg(all(any(feature = "tokio-rustls", feature = "threaded-rustls"), any(feature = "tokio-native-tls", feature = "threaded-native-tls")))]
    fn build_tls_options(&self) -> GneissResult<TlsOptions> {
        match self.tls_impl {
            TlsImplementation::Nativetls => {
                self.tls_options_builder.build_native_tls()
            }
            _ => {
                self.tls_options_builder.build_rustls()
            }
        }
    }

    #[cfg(all(any(feature = "tokio-rustls", feature = "threaded-rustls"), not(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))))]
    fn build_tls_options(&self) -> GneissResult<TlsOptions> {
        self.tls_options_builder.build_rustls()
    }

    #[cfg(all(not(any(feature = "tokio-rustls", feature = "threaded-rustls")), any(feature = "tokio-native-tls", feature = "threaded-native-tls")))]
    fn build_tls_options(&self) -> GneissResult<TlsOptions> {
        self.tls_options_builder.build_native_tls()
    }
}

#[cfg(feature = "tokio-websockets")]
async fn sign_websocket_upgrade_sigv4(request_builder: http::request::Builder, signing_region: String, credentials_provider: std::sync::Arc<dyn ProvideCredentials>) -> GneissResult<http::request::Builder> {
    let credentials = credentials_provider.provide_credentials().await.map_err(|e| { GneissError::new_other_error(e) })?;
    let session_token = credentials.session_token().map(|st| { st.to_string() });

    let identity = Identity::from(credentials);

    let mut signing_settings = aws_sigv4::http_request::SigningSettings::default();
    signing_settings.session_token_mode = SessionTokenMode::Exclude;
    signing_settings.signature_location = SignatureLocation::QueryParams;
    signing_settings.expires_in = Some(Duration::from_secs(3600));

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(signing_region.as_str())
        .name("iotdevicegateway")
        .time(SystemTime::now())
        .settings(signing_settings)
        .build()
        .unwrap()
        .into();

    let uri = request_builder.uri_ref().unwrap().clone();
    let uri_string = uri.to_string();

    let headers = vec!(("host", uri.host().unwrap()));
    let signable_request = SignableRequest::new(
        "GET",
        uri_string.clone(),
        headers.into_iter(),
        SignableBody::Bytes(&[])
    ).expect("signable request");

    let (signing_instructions, _signature) = sign(signable_request, &signing_params)
        .map_err(|e| { GneissError::new_other_error(e) })?
        .into_parts();

    let mut signed_request_builder = http::request::Builder::default()
        .method(request_builder.method_ref().unwrap());

    for (header_name, header_value) in request_builder.headers_ref().unwrap().iter() {
        signed_request_builder = signed_request_builder.header(header_name, header_value);
    }

    let mut query_param_list = signing_instructions
        .params()
        .iter()
        .map(|(key, value)| { format!("{}={}", urlencoding::encode(key), urlencoding::encode(value))})
        .collect::<Vec<String>>();

    if let Some(session_token) = session_token {
        query_param_list.push(format!("X-Amz-Security-Token={}", urlencoding::encode(session_token.as_str())));
    }

    let query_params = query_param_list.join("&");
    let final_uri = format!("{}?{}", uri_string, query_params);

    signed_request_builder = signed_request_builder.uri(final_uri);

    Ok(signed_request_builder)
}

#[cfg(feature = "testing")]
#[cfg(test)]
mod testing {
    use gneiss_mqtt::error::GneissResult;
    use std::env;
    #[cfg(feature = "tokio")]
    use std::future::Future;
    #[cfg(feature = "tokio")]
    use std::pin::Pin;
    use gneiss_mqtt::client::*;
    use gneiss_mqtt::client::waiter::*;
    use super::*;

    fn get_iot_core_endpoint() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_ENDPOINT").unwrap()
    }

    fn get_iot_core_cert_path() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_MTLS_CERT_PATH").unwrap()
    }

    fn get_iot_core_key_path() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_MTLS_KEY_PATH").unwrap()
    }

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    fn get_iot_core_cert_path_pkcs8() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_MTLS_CERT_PATH_PKCS8").unwrap()
    }

    #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
    fn get_iot_core_key_path_pkcs8() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_MTLS_KEY_PATH_PKCS8").unwrap()
    }

    #[cfg(feature = "tokio-websockets")]
    fn get_iot_core_sigv4_region() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_SIGV4_REGION").unwrap()
    }

    fn get_iot_core_unsigned_authorizer_name() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_UNSIGNED_AUTHORIZER").unwrap()
    }

    fn get_iot_core_signed_authorizer_name() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_SIGNED_AUTHORIZER").unwrap()
    }

    fn get_iot_core_custom_auth_username() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_CUSTOM_AUTH_USERNAME").unwrap()
    }

    fn get_iot_core_custom_auth_password() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_CUSTOM_AUTH_PASSWORD").unwrap()
    }

    fn get_iot_core_custom_auth_signature() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_CUSTOM_AUTH_SIGNATURE").unwrap()
    }

    fn get_iot_core_custom_auth_signature_unencoded() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_CUSTOM_AUTH_SIGNATURE_UNENCODED").unwrap()
    }

    fn get_iot_core_custom_auth_token_key_name() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_CUSTOM_AUTH_TOKEN_KEY_NAME").unwrap()
    }

    fn get_iot_core_custom_auth_token_key_value() -> String {
        env::var("GNEISS_MQTT_TEST_AWS_IOT_CORE_CUSTOM_AUTH_TOKEN_KEY_VALUE").unwrap()
    }

    fn create_mtls_client_builder(tls_impl: TlsImplementation) -> AwsClientBuilder {
        let endpoint = get_iot_core_endpoint();

        match tls_impl {
            #[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
            TlsImplementation::Nativetls => {
                // native-tls only supports pkcs8/12 private keys
                let cert_path_pkcs8 = get_iot_core_cert_path_pkcs8();
                let key_path_pkcs8 = get_iot_core_key_path_pkcs8();
                AwsClientBuilder::new_direct_with_mtls_from_fs(endpoint.as_str(), cert_path_pkcs8.as_str(), key_path_pkcs8.as_str(), None).unwrap()
            }
            _ => {
                let cert_path = get_iot_core_cert_path();
                let key_path = get_iot_core_key_path();
                AwsClientBuilder::new_direct_with_mtls_from_fs(endpoint.as_str(), cert_path.as_str(), key_path.as_str(), None).unwrap()
            }
        }
    }

    fn create_connect_waiter_options() -> ClientEventWaiterOptions {
        ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Predicate(Box::new(|ev| {
                match &**ev {
                    ClientEvent::ConnectionSuccess(_) | ClientEvent::ConnectionFailure(_) => {
                        true
                    }
                    _ => { false }
                }
            })),
        }
    }

    fn verify_connection_result(events: Vec<ClientEventRecord>) -> GneissResult<()> {
        assert_eq!(1, events.len());

        let succeeded =
            if let ClientEvent::ConnectionSuccess(_) = &*events[0].event {
                true
            } else {
                false
            };

        if succeeded {
            Ok(())
        } else {
            Err(GneissError::new_other_error("connection failed"))
        }
    }

    #[cfg(feature = "threaded")]
    fn do_sync_connect_test(builder: AwsClientBuilder) -> GneissResult<()> {
        let client = builder.build_threaded()?;

        let waiter_config = create_connect_waiter_options();
        let connection_result_waiter = ThreadedClientEventWaiter::new(client.clone(), waiter_config, 1);

        client.start(None)?;

        let connection_result_events = connection_result_waiter.wait()?;
        verify_connection_result(connection_result_events)
    }

    #[cfg(feature = "threaded")]
    fn do_sync_mtls_builder_test(tls_impl: TlsImplementation) -> () {
        let mut builder = create_mtls_client_builder(tls_impl);
        builder = builder.with_default_tls_implementation(tls_impl);

        do_sync_connect_test(builder).unwrap();
    }

    #[cfg(feature = "tokio")]
    type AsyncTestFactoryReturnType = Pin<Box<dyn Future<Output = GneissResult<()>> + Send>>;
    #[cfg(feature = "tokio")]
    type AsyncTestFactory = Box<dyn Fn() -> AsyncTestFactoryReturnType + Send + Sync>;

    #[cfg(feature = "tokio")]
    fn do_async_builder_test(test_factory: AsyncTestFactory) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let test_future = (*test_factory)();

        runtime.block_on(test_future).unwrap();
    }

    #[cfg(feature = "tokio")]
    async fn do_async_connect_test(builder: AwsClientBuilder) -> GneissResult<()> {
        let client = builder.build_tokio()?;

        let waiter_config = create_connect_waiter_options();
        let connection_result_waiter = TokioClientEventWaiter::new(client.clone(), waiter_config, 1);

        client.start(None)?;

        let connection_result_events = connection_result_waiter.wait().await?;
        verify_connection_result(connection_result_events)
    }

    #[cfg(feature = "tokio")]
    async fn do_async_mtls_builder_test(tls_impl: TlsImplementation) -> GneissResult<()> {
        let mut builder = create_mtls_client_builder(tls_impl);
        builder = builder.with_default_tls_implementation(tls_impl);

        do_async_connect_test(builder).await
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn connect_success_aws_iot_core_mtls_rustls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_mtls_builder_test(TlsImplementation::Rustls))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn connect_success_aws_iot_core_mtls_rustls_threaded() {
        do_sync_mtls_builder_test(TlsImplementation::Rustls);
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn connect_success_aws_iot_core_mtls_native_tls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_mtls_builder_test(TlsImplementation::Nativetls))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn connect_success_aws_iot_core_mtls_native_tls_threaded() {
        do_sync_mtls_builder_test(TlsImplementation::Nativetls);
    }

    #[cfg(feature = "tokio-websockets")]
    async fn do_sigv4_builder_test(tls_impl: TlsImplementation) -> GneissResult<()> {
        let signing_region = get_iot_core_sigv4_region();
        let endpoint = get_iot_core_endpoint();

        let sigv4_options = WebsocketSigv4OptionsBuilder::new(signing_region.as_str()).await.build();

        let mut builder =
            AwsClientBuilder::new_websockets_with_sigv4(endpoint.as_str(), sigv4_options, None).unwrap();
        builder = builder.with_default_tls_implementation(tls_impl);

        do_async_connect_test(builder).await
    }

    #[test]
    #[cfg(all(feature = "tokio-rustls", feature = "tokio-websockets"))]
    fn connect_success_aws_iot_core_ws_sigv4_rustls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_sigv4_builder_test(TlsImplementation::Rustls))
        }))
    }

    #[test]
    #[cfg(all(feature = "tokio-native-tls", feature = "tokio-websockets"))]
    fn connect_success_aws_iot_core_ws_sigv4_native_tls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_sigv4_builder_test(TlsImplementation::Nativetls))
        }))
    }

    fn create_unsigned_custom_auth_builder(tls_impl: TlsImplementation) -> AwsClientBuilder {
        let endpoint = get_iot_core_endpoint();
        let authorizer_name = get_iot_core_unsigned_authorizer_name();
        let username = get_iot_core_custom_auth_username();
        let password = get_iot_core_custom_auth_password();

        let mut custom_auth_options_builder = AwsCustomAuthOptionsBuilder::new_unsigned(
            Some(authorizer_name.as_str())
        );

        custom_auth_options_builder.with_username(username.as_str());
        custom_auth_options_builder.with_password(password.as_bytes());

        let mut builder =
            AwsClientBuilder::new_direct_with_custom_auth(endpoint.as_str(), custom_auth_options_builder.build(), None).unwrap();
        builder = builder.with_default_tls_implementation(tls_impl);
        builder
    }

    #[cfg(feature = "threaded")]
    fn do_sync_unsigned_custom_auth_test(tls_impl: TlsImplementation) -> () {
        do_sync_connect_test(create_unsigned_custom_auth_builder(tls_impl)).unwrap()
    }

    #[cfg(feature = "tokio")]
    async fn do_async_unsigned_custom_auth_test(tls_impl: TlsImplementation) -> GneissResult<()> {
        do_async_connect_test(create_unsigned_custom_auth_builder(tls_impl)).await
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn connect_success_aws_iot_core_custom_auth_unsigned_rustls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_unsigned_custom_auth_test(TlsImplementation::Rustls))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn connect_success_aws_iot_core_custom_auth_unsigned_rustls_threaded() {
        do_sync_unsigned_custom_auth_test(TlsImplementation::Rustls)
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn connect_success_aws_iot_core_custom_auth_unsigned_native_tls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_unsigned_custom_auth_test(TlsImplementation::Nativetls))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn connect_success_aws_iot_core_custom_auth_unsigned_native_tls_threaded() {
        do_sync_unsigned_custom_auth_test(TlsImplementation::Nativetls);
    }

    fn create_signed_custom_auth_builder(tls_impl: TlsImplementation, use_unencoded_signature: bool) -> AwsClientBuilder {
        let endpoint = get_iot_core_endpoint();
        let authorizer_name = get_iot_core_signed_authorizer_name();
        let username = get_iot_core_custom_auth_username();
        let password = get_iot_core_custom_auth_password();
        let signature =
            if use_unencoded_signature {
                get_iot_core_custom_auth_signature_unencoded()
            } else {
                get_iot_core_custom_auth_signature()
            };

        let token_key_name = get_iot_core_custom_auth_token_key_name();
        let token_key_value = get_iot_core_custom_auth_token_key_value();

        let mut custom_auth_options_builder = AwsCustomAuthOptionsBuilder::new_signed(
            Some(authorizer_name.as_str()),
            signature.as_str(),
            token_key_name.as_str(),
            token_key_value.as_str()
        );

        custom_auth_options_builder.with_username(username.as_str());
        custom_auth_options_builder.with_password(password.as_bytes());

        let mut builder =
            AwsClientBuilder::new_direct_with_custom_auth(endpoint.as_str(), custom_auth_options_builder.build(), None).unwrap();
        builder = builder.with_default_tls_implementation(tls_impl);
        builder
    }

    #[cfg(feature = "threaded")]
    fn do_sync_signed_custom_auth_test(tls_impl: TlsImplementation, use_unencoded_signature: bool) -> () {
        do_sync_connect_test(create_signed_custom_auth_builder(tls_impl, use_unencoded_signature)).unwrap();
    }

    #[cfg(feature = "tokio")]
    async fn do_async_signed_custom_auth_test(tls_impl: TlsImplementation, use_unencoded_signature: bool) -> GneissResult<()> {
        do_async_connect_test(create_signed_custom_auth_builder(tls_impl, use_unencoded_signature)).await
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn connect_success_aws_iot_core_custom_auth_signed_preencoded_rustls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_signed_custom_auth_test(TlsImplementation::Rustls, false))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn connect_success_aws_iot_core_custom_auth_signed_preencoded_rustls_threaded() {
        do_sync_signed_custom_auth_test(TlsImplementation::Rustls, false)
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn connect_success_aws_iot_core_custom_auth_signed_preencoded_native_tls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_signed_custom_auth_test(TlsImplementation::Nativetls, false))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn connect_success_aws_iot_core_custom_auth_signed_preencoded_native_tls_threaded() {
        do_sync_signed_custom_auth_test(TlsImplementation::Nativetls, false)
    }

    #[test]
    #[cfg(feature = "tokio-rustls")]
    fn connect_success_aws_iot_core_custom_auth_signed_unencoded_rustls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_signed_custom_auth_test(TlsImplementation::Rustls, true))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-rustls")]
    fn connect_success_aws_iot_core_custom_auth_signed_unencoded_rustls_threaded() {
        do_sync_signed_custom_auth_test(TlsImplementation::Rustls, true)
    }

    #[test]
    #[cfg(feature = "tokio-native-tls")]
    fn connect_success_aws_iot_core_custom_auth_signed_unencoded_native_tls_tokio() {
        do_async_builder_test(Box::new(||{
            Box::pin(do_async_signed_custom_auth_test(TlsImplementation::Nativetls, true))
        }))
    }

    #[test]
    #[cfg(feature = "threaded-native-tls")]
    fn connect_success_aws_iot_core_custom_auth_signed_unencoded_native_tls_threaded() {
        do_sync_signed_custom_auth_test(TlsImplementation::Nativetls, true)
    }
}