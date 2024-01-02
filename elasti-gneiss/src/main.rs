/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate argh;
extern crate elasti_gneiss_core;
extern crate gneiss_mqtt;
extern crate simplelog;
extern crate tokio;
extern crate url;

use std::fs::File;
use argh::FromArgs;
use elasti_gneiss_core::{client_event_callback, main_loop};
use gneiss_mqtt::*;
use gneiss_mqtt::client;
use gneiss_mqtt::client::builder::ClientBuilder;
use simplelog::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use url::Url;

#[derive(FromArgs, Debug, PartialEq)]
/// elasti-gneiss - an interactive MQTT5 console
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

    /// URI of endpoint to connect to.  Supported schemes include `mqtt` and `mqtts`
    #[argh(positional)]
    endpoint_uri: String,

    /// path to a log file that should be written
    #[argh(option)]
    logpath: Option<PathBuf>,
}

fn build_client(config: Mqtt5ClientOptions, runtime: &Handle, args: &CommandLineArgs) -> MqttResult<Mqtt5Client> {
    let url_parse_result = Url::parse(&args.endpoint_uri);
    if url_parse_result.is_err() {
        return Err(MqttError::Unknown);
    }

    let uri = url_parse_result.unwrap();
    if uri.host_str().is_none() {
        return Err(MqttError::Unknown);
    }

    let endpoint = uri.host_str().unwrap().to_string();

    if uri.port().is_none() {
        return Err(MqttError::Unknown);
    }

    let port = uri.port().unwrap();

    match uri.scheme().to_lowercase().as_str() {
        "mqtt" => {
            ClientBuilder::new(&endpoint, port)
                .unwrap()
                .with_client_options(config)
                .build(runtime)
        }
        "mqtts" => {
            let capath = args.capath.as_deref();

            if args.cert.is_some() && args.key.is_some() {
                ClientBuilder::new_with_mtls_from_fs(&endpoint, port, args.cert.as_ref().unwrap(), args.key.as_ref().unwrap(), capath)
                    .unwrap()
                    .with_client_options(config)
                    .build(runtime)
            } else {
                ClientBuilder::new_with_tls(&endpoint, port, capath)
                    .unwrap()
                    .with_client_options(config)
                    .build(runtime)
            }
        }
        _ => {
            Err(MqttError::Unknown)
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
        .with_client_id("HelloClient")
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .build();

    let config = client::Mqtt5ClientOptionsBuilder::new()
        .with_connect_options(connect_options)
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .with_connack_timeout(Duration::from_secs(60))
        .with_ping_timeout(Duration::from_secs(60))
        .with_default_event_listener(callback)
        .with_reconnect_period_jitter(ExponentialBackoffJitterType::None)
        .build();

    let client = build_client(config, &Handle::current(), &cli_args).unwrap();

    println!("elasti-gneiss - an interactive MQTT5 console application\n");
    println!(" `help` for command assistance\n");

    main_loop(client).await;

    Ok(())
}
