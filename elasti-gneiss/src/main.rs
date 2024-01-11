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
use elasti_gneiss_core::{client_event_callback, ElastiError, ElastiResult, main_loop};
use gneiss_mqtt::client::Mqtt5Client;
use gneiss_mqtt::config::*;
use simplelog::{LevelFilter, WriteLogger};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use url::Url;
use gneiss_mqtt::alias::OutboundAliasResolverFactory;

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

    /// URI of endpoint to connect to.  Supported schemes include `ws`, `mqtt` and `mqtts`
    #[argh(positional)]
    endpoint_uri: String,

    /// path to a log file that should be written
    #[argh(option)]
    logpath: Option<PathBuf>,

    /// disables SNI and peer verification; only use for testing
    #[argh(switch)]
    no_verify_peer: bool,

    /// http proxy host and port to CONNECT through
    #[argh(option)]
    http_proxy_uri: Option<String>
}

fn build_client(connect_options: ConnectOptions, client_config: Mqtt5ClientOptions, runtime: &Handle, args: &CommandLineArgs) -> ElastiResult<Mqtt5Client> {
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

    let mut builder = GenericClientBuilder::new(&endpoint, port);
    builder.with_connect_options(connect_options);
    builder.with_client_options(client_config);

    if let Some(http_proxy_uri) = args.http_proxy_uri.clone() {
        let proxy_url_parse_result = Url::parse(&http_proxy_uri);
        if proxy_url_parse_result.is_err() {
            return Err(ElastiError::InvalidUri(http_proxy_uri));
        }

        let proxy_uri = proxy_url_parse_result.unwrap();
        if proxy_uri.host_str().is_none() {
            return Err(ElastiError::InvalidUri(http_proxy_uri));
        }

        let proxy_endpoint = proxy_uri.host_str().unwrap().to_string();

        if proxy_uri.port().is_none() {
            return Err(ElastiError::InvalidUri(uri_string));
        }

        let http_proxy_options = HttpProxyOptionsBuilder::new(proxy_endpoint.as_str(), proxy_uri.port().unwrap()).build();
        builder.with_http_proxy_options(http_proxy_options);
    }

    match scheme.as_str() {
        "mqtts" => {
            let mut tls_options_builder =
                if args.cert.is_some() && args.key.is_some() {
                    TlsOptionsBuilder::new_with_mtls_from_path(args.cert.as_ref().unwrap(), args.key.as_ref().unwrap()).unwrap()
                } else {
                    TlsOptionsBuilder::new()
                };

            if let Some(capath) = &args.capath {
                tls_options_builder.with_root_ca_from_path(capath.as_str()).unwrap();
            }

            tls_options_builder.with_verify_peer(!args.no_verify_peer);

            builder.with_tls_options(tls_options_builder.build_rustls().unwrap());
        }
        "ws" => {
            let websocket_options = WebsocketOptionsBuilder::new().build();
            builder.with_websocket_options(websocket_options);
        }
        "wss" => {
            let mut tls_options_builder = TlsOptionsBuilder::new();
            tls_options_builder.with_verify_peer(!args.no_verify_peer);
            if let Some(capath) = &args.capath {
                tls_options_builder.with_root_ca_from_path(capath.as_str()).unwrap();
            }

            let tls_options = tls_options_builder.build_rustls().unwrap();
            builder.with_tls_options(tls_options);

            let websocket_options = WebsocketOptionsBuilder::new().build();
            builder.with_websocket_options(websocket_options);
        }
        _ => {}
    }

    Ok(builder.build(runtime)?)
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
        .with_keep_alive_interval_seconds(Some(60))
        .with_client_id("HelloClient-wss")
        .with_rejoin_session_policy(RejoinSessionPolicy::PostSuccess)
        .build();

    let config = Mqtt5ClientOptionsBuilder::new()
        .with_offline_queue_policy(OfflineQueuePolicy::PreserveAll)
        .with_connack_timeout(Duration::from_secs(3600))
        .with_ping_timeout(Duration::from_secs(60))
        .with_default_event_listener(callback)
        .with_reconnect_period_jitter(ExponentialBackoffJitterType::None)
        .with_outbound_alias_resolver_factory(OutboundAliasResolverFactory::new_lru_factory(10))
        .build();

    let client = build_client(connect_options, config, &Handle::current(), &cli_args).unwrap();

    println!("elasti-gneiss - an interactive MQTT5 console application\n");
    println!(" `help` for command assistance\n");

    main_loop(client).await;

    Ok(())
}
