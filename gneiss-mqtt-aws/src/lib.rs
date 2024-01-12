/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides a builder API for creating MQTT clients that connect to AWS IoT Core, an
AWS-managed message broker that supports both MQTT5 and MQTT311.  This crate depends on
[`gneiss-mqtt`](https://crates.io/crates/gneiss-mqtt),
which contains the MQTT client implementations.

IoT Core supports three different ways to securely establish an MQTT connection:
* MQTT over mTLS - provide an X509 certificate (registered with AWS IoT Core) and its associated private key
* MQTT over Websockets - sign the websocket upgrade request with AWS credentials using the Sigv4 signing algorithm
* MQTT with Custom Authentication - invoke an AWS Lambda with data fields passed via the MQTT username and password fields in the Connect packet

This crate's builder does all the dirty work for each of these connection methods, letting you
just supply the minimal required data.

# Usage

To use this crate, you'll first need to add it and [`gneiss-mqtt`](https://crates.io/crates/gneiss-mqtt) to your project's Cargo.toml:

```toml
[dependencies]
gneiss-mqtt = "0.2"
gneiss-mqtt-aws = "0.2"
```

(Temporary) If your project does not include [`tokio`](https://crates.io/crates/tokio), you will need to add it too:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

Future releases will support other async runtimes as well as a client that runs in a background
thread and does not need an async runtime.  For now, [`tokio`](https://crates.io/crates/tokio) is required.

# Example: Connect to AWS IoT Core via mTLS (with tokio runtime)

You'll need to create and register an X509 device certificate with IoT Core and associate an IAM
permission policy that allows IoT Core connections.  See
[X509 Certificates and AWS IoT Core](https://docs.aws.amazon.com/iot/latest/developerguide/x509-client-certs.html)
for guidance on this process.

To create a client and connect:

```no_run
use gneiss_mqtt_aws::AwsClientBuilder;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = "<your AWS IoT Core endpoint>";
    let cert_path = "<path to your X509 certificate>";
    let key_path = "<path to the certificate's private key>";

    // In the common case, you will not need a root CA certificate
    let client =
        AwsClientBuilder::new_direct_with_mtls_from_fs(endpoint, cert_path, key_path, None)?
            .build(&Handle::current())?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start()?;

    // <do stuff with the client>

    Ok(())
}
```

# Example: Connect to AWS IoT Core via Websockets
Not yet implemented

# Example: Connect to AWS IoT Core via AWS IoT Custom Authentication (with tokio runtime)

Custom authentication is an AWS IoT Core specific way to perform authentication without using
certificates or http request signing.  Instead, an AWS Lambda is invoked to decide whether or
not a connection is allowed.  See the
[custom authentication documentation](https://docs.aws.amazon.com/iot/latest/developerguide/custom-authentication.html)
for step-by-step instructions in how to set up the AWS resources (authorizer, Lambda, etc...) to
perform custom authentication.

Once the necessary AWS resources have been set up, you can easily create clients for each of the two
supported custom authentication modes:

* Unsigned Custom Authentication - Anyone can invoke the authorizer's lambda if they know its ARN.  This is not recommended for production since it is not protected from external abuse that may run up your AWS bill.
* Signed Custom Authentication - Your Lambda function will only be invoked (and billed) if the Connect packet includes the cryptographic signature (based on an IoT Core registered public key) of a controllable value.  Recommended for production.

### Unsigned Custom Authentication

For an unsigned custom authorizer (for testing/internal purposes only, not recommended for production):

```no_run
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptions};
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = "<your AWS IoT Core endpoint>";
    let authorizer_name = "<name of the authorizer you want to invoke>";
    let username = "<username value to pass to the authorizer>"; // only necessary if the authorizer's lambda uses it
    let password = "<password value to pass to the authorizer>".as_bytes(); // only necessary if the authorizer's lambda uses it

    let unsigned_custom_auth_options = AwsCustomAuthOptions::new_unsigned(
        authorizer_name,
        Some(username),
        Some(password)
    );

    // In the common case, you will not need a root CA certificate
    let client =
        AwsClientBuilder::new_direct_with_custom_auth(endpoint, unsigned_custom_auth_options, None)?
            .build(&Handle::current())?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start()?;

    // <do stuff with the client>

    Ok(())
}
```

### Signed Custom Authentication

For a signed custom authorizer (recommended for production):

```no_run
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptions};
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = "<your AWS IoT Core endpoint>";
    let authorizer_name = "<name of the authorizer you want to invoke>";
    let authorizer_token_key_name = "<key name registered with the signing authorizer that indicates the name of the field whose value will contain the `authorizer_token_key_value`>";
    let authorizer_token_key_value = "<An arbitrary value.  The (Base64-encoded) signature of this value (using the private key of the public key associated with the authorizer) must be included as a separate field>";
    let authorizer_signature = "<URI-encoded Base64-encoded signature for `authorizer_token_key_value` signed by the private key of the public key associated with the authorizer>";
    let username = "<username value to pass to the authorizer>"; // only necessary if the authorizer's lambda uses it
    let password = "<password value to pass to the authorizer>".as_bytes(); // only necessary if the authorizer's lambda uses it

    let signed_custom_auth_options = AwsCustomAuthOptions::new_signed(
        authorizer_name,
        authorizer_signature,
        authorizer_token_key_name,
        authorizer_token_key_value,
        Some(username),
        Some(password)
    );

    // In the common case, you will not need a root CA certificate
    let client =
        AwsClientBuilder::new_direct_with_custom_auth(endpoint, signed_custom_auth_options, None)?
            .build(&Handle::current())?;

    // Once started, the client will recurrently maintain a connection to the endpoint until
    // stop() is invoked
    client.start()?;

    // <do stuff with the client>

    Ok(())
}
```

You must be careful with the encodings of `authorizer`, `authorizer_signature`, and
`authorizer_token_key_name`.  Because
custom authentication is supported over HTTP, these values must be URI-safe.  It is up to
you to URI encode them if necessary.  In general, `authorizer` and `authorizer_token_key_name` are
fixed when you create
the authorizer resource and so it is
straightforward to determine if you need to encode them or not.  `authorizer_signature` should
always be URI encoded.

TODO: automatically encode `authorizer_signature` if it needs it.

# MQTT Client Configuration

The above examples skip all client configuration in favor of defaults.  There are many configuration
details that may be of interest depending on your use case.  These options are controlled by
structures in the `gneiss-mqtt` crate, via the `with_client_options` and `with_connect_options`
methods on the [`AwsClientBuilder`](https://docs.rs/gneiss-mqtt-aws/latest/gneiss_mqtt-aws/struct.AwsClientBuilder.html).
Further details can be found in the relevant sections of the `gneiss-mqtt` docs:
* [`Mqtt5ClientOptions`](https://docs.rs/gneiss-mqtt/latest/gneiss_mqtt/config/struct.Mqtt5ClientOptionsBuilder.html)
* [`ConnectOptions`](https://docs.rs/gneiss-mqtt/latest/gneiss_mqtt/config/struct.ConnectOptionsBuilder.html)

# Additional Notes

See the [`gneiss-mqtt`](https://docs.rs/gneiss-mqtt/latest/gneiss_mqtt/) documentation for client
usage details and guidance.

The intention is that this crate will eventually be as agnostic as possible of underlying
implementation details (async runtimes, TLS/transport implementations, etc...)
but at present it has hard dependencies on tokio, rustls, and some
associated helper libraries.  These will get feature-flag-gated before GA, allowing you
to pare the implementation down to your exact connection needs.  In Rust's current state,
there is a fundamental tension between trying to be transport/runtime agnostic and trying
to provide an easy-to-use interface for getting successful clients set up for the many
different combinations expected by users.

 */

#![warn(missing_docs)]

extern crate gneiss_mqtt;
extern crate tokio;

use gneiss_mqtt::config::*;
use gneiss_mqtt::client::Mqtt5Client;
use gneiss_mqtt::{MqttError, MqttResult};
use tokio::runtime::Handle;

const CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-name";
const CUSTOM_AUTH_SIGNATURE_QUERY_PARAM_NAME: &str = "x-amz-customauthorizer-signature";

///  A struct that holds all relevant details needed to perform custom authentication with
/// AWS IoT Core.  Use an appropriate `new_*()` function to create.
pub struct AwsCustomAuthOptions {
    username: String,
    password: Option<Vec<u8>>
}

impl AwsCustomAuthOptions {

    /// Creates a new custom authentication options configuration to use an unsigned authorizer.
    ///
    /// `authorizer_name` - name of the AWS IoT authorizer to use.  This value must be URI-encoded if necessary.
    ///
    /// `username` - specifies additional data to pass to the authorizer's Lambda function via the `protocolData.mqtt.username` field.
    ///
    /// `password` - specifies additional data to pass to the authorizer's Lambda function via the `protocolData.mqtt.password` field.
    pub fn new_unsigned(authorizer_name: &str, username: Option<&str>, password: Option<&[u8]>) -> Self {
        AwsCustomAuthOptions {
            username: format!("{}?{}={}", username.unwrap_or(""), CUSTOM_AUTH_AUTHORIZER_QUERY_PARAM_NAME, authorizer_name),
            password: password.map(|p| p.to_vec())
        }
    }

    /// Creates a new custom authentication options configuration to use a signed authorizer. See
    /// [AWS IoT Custom Authentication](https://docs.aws.amazon.com/iot/latest/developerguide/custom-authentication.html)
    /// for more details.  The authenticator's Lambda will not be invoked unless `authorizer_signature`
    /// is the URI-encoded Base64-encoded signature of `authorizer_token_key_value` via the private key
    /// associated with the public key that was registered with the authorizer on creation.
    ///
    /// `authorizer_name` - name of the AWS IoT authorizer to use.  This value must be URI-encoded if necessary.
    ///
    /// `authorizer_signature` - The URI-encoded, Base64-encoded cryptographic signature of the value contained in `authorizer_token_key_value`.  The signature must be
    /// made with the private key associated with the public key that was registered with the authorizer.
    ///
    /// `authorizer_token_key_name` - key name registered with the signing authorizer that indicates the name of the field whose value will contain the `authorizer_token_key_value`
    ///
    /// `authorizer_token_key_value` - arbitrary value whose digital signature is provided in the `authorizer_signature`
    ///
    /// `username` - specifies additional data to pass to the authorizer's Lambda function via the `protocolData.mqtt.username` field.
    ///
    /// `password` - specifies additional data to pass to the authorizer's Lambda function via the `protocolData.mqtt.password` field.
    ///
    /// `authorizer_token_key_name` and `authorizer_name` must be valid URI-encoded values.
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

/// A builder object that allows for easy MQTT client construction for all supported
/// connection methods to AWS IoT Core.
pub struct AwsClientBuilder {
    auth_type: AuthType,
    custom_auth_options: Option<AwsCustomAuthOptions>,
    connect_options: Option<ConnectOptions>,
    client_options: Option<Mqtt5ClientOptions>,
    tls_options: TlsOptions,
    endpoint: String
}

const ALPN_PORT : u16 = 443;
const DEFAULT_PORT : u16 = ALPN_PORT;
const DIRECT_ALPN_PROTOCOL : &[u8] = b"x-amzn-mqtt-ca";
const CUSTOM_AUTH_ALPN_PROTOCOL : &[u8] = b"mqtt";

impl AwsClientBuilder {

    /// Creates a new builder that will construct an MQTT5 client that connects to AWS IoT Core
    /// using mutual TLS where the certificate and private key are read from files.
    ///
    /// `certificate_path` - path to a PEM-encoded file of the X509 certificate to use in mTLS
    ///
    /// `private_key_path` - path to a PEM-encoded file of the private key associated with the X509 certificate in `certificate_path`
    ///
    /// `root_ca_path` - path to a root CA to use in the TLS context of the connection.  Generally
    /// not needed unless a custom domain is involved.
    pub fn new_direct_with_mtls_from_fs(endpoint: &str, certificate_path: &str, private_key_path: &str, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let mut tls_options_builder = TlsOptionsBuilder::new_with_mtls_from_path(certificate_path, private_key_path)?;
        if let Some(root_ca) = root_ca_path {
            tls_options_builder.with_root_ca_from_path(root_ca)?;
        }
        tls_options_builder.with_alpn(DIRECT_ALPN_PROTOCOL);

        let tls_options_result = tls_options_builder.build_rustls();
        if tls_options_result.is_err() {
            return Err(MqttError::TlsError);
        }

        let builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            tls_options: tls_options_result.unwrap(),
            endpoint: endpoint.to_string()
        };

        Ok(builder)
    }

    /// Creates a new builder that will construct an MQTT5 client that connects to AWS IoT Core
    /// using mutual TLS where the certificate and private key are read from memory.
    ///
    /// `certificate_bytes` - raw PEM-encoded data of the X509 certificate to use in mTLS
    ///
    /// `private_key_bytes` - raw PEM-encoded data of the private key associated with the X509 certificate in `certificate_bytes`
    ///
    /// `root_ca_bytes` - root CA PEM data to use in the TLS context of the connection.  Generally
    /// not needed unless a custom domain is involved.
    pub fn new_direct_with_mtls_from_memory(endpoint: &str, certificate_bytes: &[u8], private_key_bytes: &[u8], root_ca_bytes: Option<&[u8]>) -> MqttResult<Self> {
        let mut tls_options_builder = TlsOptionsBuilder::new_with_mtls_from_memory(certificate_bytes, private_key_bytes);
        if let Some(root_ca) = root_ca_bytes {
            tls_options_builder.with_root_ca_from_memory(root_ca);
        }
        tls_options_builder.with_alpn(DIRECT_ALPN_PROTOCOL);

        let tls_options_result = tls_options_builder.build_rustls();
        if tls_options_result.is_err() {
            return Err(MqttError::TlsError);
        }

        let builder =  AwsClientBuilder {
            auth_type: AuthType::Mtls,
            custom_auth_options: None,
            connect_options: None,
            client_options: None,
            tls_options: tls_options_result.unwrap(),
            endpoint: endpoint.to_string()
        };

        Ok(builder)
    }

    /// Creates a new builder that will construct an MQTT5 client that connects to AWS IoT Core
    /// using custom authentication.
    ///
    /// `custom_auth_options` - custom authentication options to use while connecting
    ///
    /// `root_ca_path` - path to a root CA to use in the TLS context of the connection.  Generally
    /// not needed unless a custom domain is involved.
    pub fn new_direct_with_custom_auth(endpoint: &str, custom_auth_options: AwsCustomAuthOptions, root_ca_path: Option<&str>) -> MqttResult<Self> {
        let mut tls_options_builder = TlsOptionsBuilder::new();
        if let Some(root_ca) = root_ca_path {
            tls_options_builder.with_root_ca_from_path(root_ca)?;
        }
        tls_options_builder.with_alpn(CUSTOM_AUTH_ALPN_PROTOCOL);

        let tls_options_result = tls_options_builder.build_rustls();
        if tls_options_result.is_err() {
            return Err(MqttError::TlsError);
        }

        let builder =  AwsClientBuilder {
            auth_type: AuthType::CustomAuth,
            custom_auth_options: Some(custom_auth_options),
            connect_options: None,
            client_options: None,
            tls_options: tls_options_result.unwrap(),
            endpoint: endpoint.to_string()
        };

        Ok(builder)
    }

    /// Registers a set of configuration options relevant to the MQTT Connect packet with the
    /// builder.  These options will be used every time the client attempts to connect to
    /// the broker.
    pub fn with_connect_options(mut self, connect_options: ConnectOptions) -> Self {
        self.connect_options = Some(connect_options);
        self
    }

    /// Registers a set of client configuration options with the builder.
    pub fn with_client_options(mut self, client_options: Mqtt5ClientOptions) -> Self {
        self.client_options = Some(client_options);
        self
    }

    /// Creates a new MQTT5 client from all of the configuration options registered with the
    /// builder.
    pub fn build(&self, runtime: &Handle) -> MqttResult<Mqtt5Client> {
        let mut connect_options =
            if let Some(options) = &self.connect_options {
                options.clone()
            } else {
                ConnectOptionsBuilder::new().build()
            };

        let client_options =
            if let Some(options) = &self.client_options {
                options.clone()
            } else {
                Mqtt5ClientOptionsBuilder::new().build()
            };

        self.apply_custom_auth_options_to_connect_options(&mut connect_options);

        let tls_options = self.tls_options.clone();

        GenericClientBuilder::new(self.endpoint.as_str(), DEFAULT_PORT)
            .with_connect_options(connect_options)
            .with_client_options(client_options)
            .with_tls_options(tls_options)
            .build(runtime)
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
}