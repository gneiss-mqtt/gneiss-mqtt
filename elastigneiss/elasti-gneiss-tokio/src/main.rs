/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::fs::File;
use argh::FromArgs;
use elasti_gneiss_core::{ElastiError, ElastiResult};
use elasti_gneiss_core_async::main_loop;
use gneiss_mqtt::client::*;
use gneiss_mqtt::client::config::*;
use simplelog::{LevelFilter, WriteLogger};
use std::path::PathBuf;
use url::Url;
use gneiss_mqtt::alias::OutboundAliasResolverFactory;

#[derive(FromArgs, Debug, PartialEq)]
/// elasti-gneiss-tokio - an interactive MQTT console
struct CommandLineArgs {

    /// URI of endpoint to connect to.  Supported schemes include `ws`, `mqtt` and `mqtts`
    #[argh(positional)]
    endpoint_uri: String,

    /// path to a log file that should be written
    #[argh(option)]
    logpath: Option<PathBuf>,

    /// protocol version to use.  Valid values are `5` and `311`
    #[argh(option)]
    version: Option<u32>
}

fn build_client(connect_options: ConnectOptions, client_config: MqttClientOptions, args: &CommandLineArgs) -> ElastiResult<AsyncClientHandle> {
    let uri_string = args.endpoint_uri.clone();

    let url_parse_result = Url::parse(&args.endpoint_uri);
    if url_parse_result.is_err() {
        return Err(ElastiError::InvalidUri(uri_string));
    }

    let uri = url_parse_result.unwrap();
    if uri.host_str().is_none() {
        return Err(ElastiError::InvalidUri(uri_string));
    }

    let endpoint = uri.host_str().unwrap().to_string();

    if uri.port().is_none() {
        return Err(ElastiError::InvalidUri(uri_string));
    }

    let port = uri.port().unwrap();
    let scheme = uri.scheme().to_lowercase();

    let mut builder = TokioClientBuilder::new();
    builder.with_connect_options(connect_options);
    builder.with_client_options(client_config);

    match scheme.as_str() {
        "mqtt" => {
        }
        _ => {
            return Err(ElastiError::UnsupportedUriScheme(scheme));
        }
    }

    Ok(builder.build(
        make_tokio_tcp_stream_connection_factory(endpoint, port)
    )?)
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

        let log_config = simplelog::ConfigBuilder::new().build();
        WriteLogger::init(LevelFilter::Debug, log_config, log_file_result.unwrap()).unwrap();
    }

    let connect_options = ConnectOptions::builder()
        .with_keep_alive_interval_seconds(Some(60))
        .with_client_id("elasti-gneiss-tokio")
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .build();

    let mut protocol_mode = ProtocolMode::Mqtt5;
    if let Some(raw_version) = cli_args.version {
        protocol_mode = ProtocolMode::try_from(raw_version)?;
    }

    let config = MqttClientOptions::builder()
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .with_reconnect_period_jitter(ExponentialBackoffJitterType::None)
        .with_outbound_alias_resolver_factory(OutboundAliasResolverFactory::new_lru_factory(10))
        .with_protocol_mode(protocol_mode)
        .build();

    let client = build_client(connect_options, config, &cli_args).unwrap();

    println!("elasti-gneiss-tokio - an interactive MQTT console application\n");
    println!(" `help` for command assistance\n");

    main_loop(client).await;

    Ok(())
}
