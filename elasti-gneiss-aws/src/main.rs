/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate argh;
extern crate elasti_gneiss_core;
extern crate gneiss_mqtt;
extern crate gneiss_mqtt_aws;
extern crate simplelog;
extern crate tokio;
extern crate url;

use std::fs::File;
use argh::FromArgs;
use elasti_gneiss_core::{client_event_callback, ElastiError, ElastiResult, main_loop};
use gneiss_mqtt::*;
use gneiss_mqtt::client::{ExponentialBackoffJitterType};
use gneiss_mqtt_aws::{AwsClientBuilder, AwsCustomAuthOptions};
use simplelog::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
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
}

fn build_client(connect_config: ConnectOptions, client_config: Mqtt5ClientOptions, runtime: &Handle, args: &CommandLineArgs) -> ElastiResult<Mqtt5Client> {
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
            let password: Option<Vec<u8>> = args.password.as_ref().map(|p| p.as_bytes().to_vec());

            if args.authorizer.is_some() {
                if args.authorizer_signature.is_some() && args.authorizer_token_key_value.is_some() && args.authorizer_token_key_name.is_some() {
                    let signed_config = AwsCustomAuthOptions::new_signed(
                        args.authorizer.as_ref().unwrap(),
                        args.authorizer_signature.as_ref().unwrap(),
                        args.authorizer_token_key_name.as_ref().unwrap(),
                        args.authorizer_token_key_value.as_ref().unwrap(),
                        args.username.as_deref(),
                        password.as_deref()
                    );

                    Ok(AwsClientBuilder::new_direct_with_signed_custom_auth(&endpoint, signed_config, capath)?
                        .with_connect_options(connect_config)
                        .with_client_options(client_config)
                        .build(runtime)?)
                } else {
                    let unsigned_config = AwsCustomAuthOptions::new_unsigned(
                        args.authorizer.as_ref().unwrap(),
                        args.username.as_deref(),
                        password.as_deref()
                    );

                    Ok(AwsClientBuilder::new_direct_with_unsigned_custom_auth(&endpoint, unsigned_config, capath)?
                        .with_connect_options(connect_config)
                        .with_client_options(client_config)
                        .build(runtime)?)
                }
            } else {
                println!("ERROR: aws-custom-auth scheme requires authorizer parameter");
                Err(ElastiError::MissingArguments("--authorizer"))
            }
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

    let function = |event|{client_event_callback(event)} ;
    let dyn_function = Arc::new(function);
    let callback = ClientEventListener::Callback(dyn_function);

    let connect_options = ConnectOptionsBuilder::new()
        .with_keep_alive_interval_seconds(60)
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .build();

    let config = client::Mqtt5ClientOptionsBuilder::new()
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .with_connack_timeout(Duration::from_secs(60))
        .with_ping_timeout(Duration::from_secs(60))
        .with_default_event_listener(callback)
        .with_reconnect_period_jitter(ExponentialBackoffJitterType::Uniform)
        .build();

    let client = build_client(connect_options, config, &Handle::current(), &cli_args).unwrap();

    println!("elasti-gneiss - an interactive MQTT5 console application\n");
    println!(" `help` for command assistance\n");

    main_loop(client).await;

    Ok(())
}
