/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::fs::File;
use argh::FromArgs;
use elasti_gneiss_core::{ElastiError, ElastiResult, main_loop};
use gneiss_mqtt::client::Mqtt5Client;
use gneiss_mqtt::config::*;
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptionsBuilder, WebsocketSigv4OptionsBuilder};
use simplelog::{LevelFilter, WriteLogger};
use std::path::PathBuf;
use tokio::runtime::Handle;
use url::Url;

#[derive(FromArgs, Debug, PartialEq)]
/// elasti-gneiss-aws - an interactive MQTT5 console
struct CommandLineArgs {

    /// path to the root CA to use when connecting.  If the endpoint URI is a TLS-enabled
    /// scheme and this is not set, then the default system trust store will be used instead.
    #[argh(option)]
    capath: Option<String>,

    /// path to a client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    cert: Option<String>,

    /// path to the private key associated with the client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    key: Option<String>,

    /// URI of endpoint to connect to.  Supported schemes include `aws-mqtts` and `aws-custom-auth` (`aws-wss-sigv4` to come later)
    #[argh(positional)]
    endpoint_uri: String,

    /// path to a log file that should be written
    #[argh(option)]
    logpath: Option<PathBuf>,

    /// name of the custom authorizer to invoke
    #[argh(option)]
    authorizer: Option<String>,

    /// custom authorizer signature
    #[argh(option)]
    authorizer_signature: Option<String>,

    /// username override for custom auth
    #[argh(option)]
    username: Option<String>,

    /// password override for custom auth
    #[argh(option)]
    password: Option<String>,

    /// authorizer token key name
    #[argh(option)]
    authorizer_token_key_name: Option<String>,

    /// authorizer token key value
    #[argh(option)]
    authorizer_token_key_value: Option<String>,

    /// signing region for websocket connections
    #[argh(option)]
    signing_region: Option<String>
}

async fn build_client(connect_config: ConnectOptions, client_config: MqttClientOptions, runtime: &Handle, args: &CommandLineArgs) -> ElastiResult<Mqtt5Client> {
    let uri_string = args.endpoint_uri.clone();

    let url_parse_result = Url::parse(&uri_string);
    if url_parse_result.is_err() {
        return Err(ElastiError::InvalidUri(uri_string));
    }

    let uri = url_parse_result.unwrap();
    if uri.host_str().is_none() {
        return Err(ElastiError::InvalidUri(uri_string));
    }

    let endpoint = uri.host_str().unwrap().to_string();
    let capath = args.capath.as_deref();

    let scheme = uri.scheme().to_lowercase();
    match scheme.as_str() {
        "aws-mqtts" => {
            if args.cert.is_some() && args.key.is_some() {
                Ok(AwsClientBuilder::new_direct_with_mtls_from_fs(&endpoint, args.cert.as_ref().unwrap(), args.key.as_ref().unwrap(), capath)?
                    .with_connect_options(connect_config)
                    .with_client_options(client_config)
                    .build(runtime)?)
            } else {
                println!("ERROR: aws-mqtts scheme requires certification and private key fields for mTLS");
                Err(ElastiError::MissingArguments("--cert, --key"))
            }
        }
        "aws-custom-auth" => {
            let mut config =
                if args.authorizer_signature.is_some() && args.authorizer_token_key_value.is_some() && args.authorizer_token_key_name.is_some() {
                    AwsCustomAuthOptionsBuilder::new_signed(
                        args.authorizer.as_deref(),
                        args.authorizer_signature.as_ref().unwrap(),
                        args.authorizer_token_key_name.as_ref().unwrap(),
                        args.authorizer_token_key_value.as_ref().unwrap(),
                    )
                } else {
                    AwsCustomAuthOptionsBuilder::new_unsigned(
                        args.authorizer.as_deref()
                    )
                };

            if let Some(username) = &args.username {
                config.with_username(username.as_str());
            }

            if let Some(password) = &args.password {
                config.with_password(password.as_bytes());
            }

            Ok(AwsClientBuilder::new_direct_with_custom_auth(&endpoint, config.build(), capath)?
                .with_connect_options(connect_config)
                .with_client_options(client_config)
                .build(runtime)?)
        }
        "aws-wss" => {
            let signing_region = args.signing_region.clone().unwrap_or("us-east-1".to_string());
            let sigv4_builder = WebsocketSigv4OptionsBuilder::new(signing_region.as_str()).await;
            let sigv4_options = sigv4_builder.build();

            Ok(AwsClientBuilder::new_websockets_with_sigv4(&endpoint, sigv4_options, capath)?
                .with_connect_options(connect_config)
                .with_client_options(client_config)
                .build(runtime)?)
        }
        _ => {
            Err(ElastiError::UnsupportedUriScheme(scheme))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: CommandLineArgs = argh::from_env();

    if let Some(log_file_path) = &cli_args.logpath {
        let log_file_result = File::create(log_file_path);
        if log_file_result.is_err() {
            println!("Could not create log file");
            return Ok(());
        }

        let mut log_config_builder = simplelog::ConfigBuilder::new();
        let log_config = log_config_builder.build();
        WriteLogger::init(LevelFilter::Debug, log_config, log_file_result.unwrap()).unwrap();
    }

    let connect_options = ConnectOptionsBuilder::new().build();

    let config = MqttClientOptionsBuilder::new()
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .with_reconnect_period_jitter(ExponentialBackoffJitterType::Uniform)
        .build();

    let client = build_client(connect_options, config, &Handle::current(), &cli_args).await.unwrap();

    println!("elasti-gneiss - an interactive MQTT5 console application\n");
    println!(" `help` for command assistance\n");

    main_loop(client).await;

    Ok(())
}
