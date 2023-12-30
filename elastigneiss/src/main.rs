/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate argh;
extern crate inline_colorization;
extern crate gneiss_mqtt;
extern crate rustls;
extern crate rustls_pemfile;
extern crate simplelog;
extern crate tokio;
extern crate tokio_rustls;
extern crate url;

use argh::FromArgs;
use inline_colorization::*;
use gneiss_mqtt::client;
use rustls::pki_types::PrivateKeyDer;
use simplelog::*;
use std::sync::Arc;
use std::fs::File;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_rustls::client::TlsStream;
use tokio_rustls::{TlsConnector};
use url::Url;

use gneiss_mqtt::*;

#[derive(FromArgs, Debug, PartialEq)]
/// Elastimqtt5 - an interactive MQTT5 console
struct CommandLineArgs {

    /// path to the root CA to use when connecting.  If the endpoint URI is a TLS-enabled
    /// scheme and this is not set, then the default system trust store will be used instead.
    #[argh(option)]
    capath: Option<PathBuf>,

    /// path to a client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    cert: Option<PathBuf>,

    /// path to the private key associated with the client X509 certificate to use while connecting via mTLS.
    #[argh(option)]
    key: Option<PathBuf>,

    /// URI of endpoint to connect to.  Supported schemes include `mqtt` and `mqtts`
    #[argh(positional)]
    endpoint_uri: String,

    /// path to a log file that should be written
    #[argh(option)]
    logpath: Option<PathBuf>,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "start")]
/// starts the client
struct StartArgs {
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "stop")]
/// stops the client
struct StopArgs {

    /// disconnect reason code. If none given then a disconnect will not be sent prior to stream close.
    #[argh(positional)]
    reason_code: Option<u8>,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "quit")]
/// causes the program to quit
struct QuitArgs {
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "close")]
/// closes the client, dropping any connection and rendering it unusable
struct CloseArgs {
}


#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "subscribe")]
/// Subscribe client command
struct SubscribeArgs {

    /// topic filter to subscribe to
    #[argh(positional)]
    topic_filter: String,

    /// subscription quality of service (0, 1, 2)
    #[argh(positional)]
    qos: u8,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "unsubscribe")]
/// Unsubscribe client command
struct UnsubscribeArgs {

    /// topic filter to unsubscribe from
    #[argh(positional)]
    topic_filter: String,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "publish")]
/// Publish client command
struct PublishArgs {

    /// topic to publish a message to
    #[argh(positional)]
    topic: String,

    /// quality of service (0, 1, 2)
    #[argh(positional)]
    qos: u8,

    /// message payload
    #[argh(positional)]
    payload: Option<String>,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand)]
enum SubCommandEnum {
    Start(StartArgs),
    Stop(StopArgs),
    Quit(QuitArgs),
    Close(CloseArgs),
    Publish(PublishArgs),
    Subscribe(SubscribeArgs),
    Unsubscribe(UnsubscribeArgs),
}

#[derive(FromArgs, Debug, PartialEq)]
/// Elastimqtt5 - an interactive MQTT5 console
struct CommandArgs {
    #[argh(subcommand)]
    nested: SubCommandEnum,
}

fn client_event_callback(event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::ConnectionAttempt(_) => {
            println!("Connection Attempt!\n");
        }
        ClientEvent::ConnectionFailure(event) => {
            println!("{color_red}Connection Failure!");
            println!("{:?}\n{color_reset}", event);
        }
        ClientEvent::ConnectionSuccess(event) => {
            println!("{color_green}Connection Success!");
            println!("{}", event.connack);
            println!("{}\n{color_reset}", event.settings);
        }
        ClientEvent::Disconnection(event) => {
            println!("{color_yellow}Disconnection!");
            println!("{:?}\n{color_reset}", event);
        }
        ClientEvent::Stopped(_) => {
            println!("Stopped!\n");
        }
        ClientEvent::PublishReceived(event) => {
            println!("Publish Received!");
            println!("{}\n", &event.publish);
        }
    }
}

fn handle_start(client: &Mqtt5Client, _: StartArgs) {
    let _ = client.start();
}

fn handle_stop(client: &Mqtt5Client, args: StopArgs) {
    let mut stop_options_builder = StopOptionsBuilder::new();

    if let Some(reason_code_u8) = args.reason_code {
        if let Ok(reason_code) = convert_u8_to_disconnect_reason_code(reason_code_u8) {
            stop_options_builder = stop_options_builder.with_disconnect_packet(DisconnectPacket{
                reason_code,
                ..Default::default()
            });
        } else {
            println!("{color_red}Invalid input!  reason_code must be a valid numeric Disconnect reason code{color_reset}");
            return;
        }
    }

    let _ = client.stop(stop_options_builder.build());
}

fn handle_close(client: &Mqtt5Client, _ : CloseArgs) {
    let _ = client.close();
}

async fn handle_publish(client: &Mqtt5Client, args: PublishArgs) {

    let mut publish = PublishPacket::new_empty(&args.topic, QualityOfService::AtLeastOnce);

    if let Ok(qos) = convert_u8_to_quality_of_service(args.qos) {
        publish.qos = qos;
    } else {
        println!("{color_red}Invalid input!  Qos must be 0, 1, or 2{color_reset}");
        return;
    }

    if let Some(payload) = &args.payload {
        publish.payload = Some(payload.as_bytes().to_vec());
    }

    let publish_result = client.publish(publish, PublishOptionsBuilder::new().build()).await;
    match &publish_result {
        Ok(publish_response) => {
            println!("{color_green}Publish Result: Ok(\n  {} )\n{color_reset}", publish_response);
        }
        Err(err) => {
            println!("{color_red}Publish Result: Err( {} )\n{color_reset}", err);
        }
    }
}

async fn handle_subscribe(client: &Mqtt5Client, args: SubscribeArgs) {
    let qos_result = convert_u8_to_quality_of_service(args.qos);
    if qos_result.is_err() {
        println!("{color_red}Invalid input!  Qos must be 0, 1, or 2{color_reset}");
        return;
    }

    let subscribe = SubscribePacket {
        subscriptions: vec!(
            Subscription::new(&args.topic_filter, qos_result.unwrap())
        ),
        ..Default::default()
    };

    let subscribe_result = client.subscribe(subscribe, SubscribeOptionsBuilder::new().build()).await;

    match &subscribe_result {
        Ok(subscribe_response) => {
            println!("{color_green}Subscribe Result: Ok(\n  {} )\n{color_reset}", subscribe_response);
        }
        Err(err) => {
            println!("{color_red}Subscribe Result: Err( {} )\n{color_reset}", err);
        }
    }
}

async fn handle_unsubscribe(client: &Mqtt5Client, args: UnsubscribeArgs) {

    let unsubscribe = UnsubscribePacket {
        topic_filters: vec!(
            args.topic_filter
        ),
        ..Default::default()
    };

    let unsubscribe_result = client.unsubscribe(unsubscribe, UnsubscribeOptionsBuilder::new().build()).await;

    match &unsubscribe_result {
        Ok(unsubscribe_response) => {
            println!("{color_green}Unsubscribe Result: Ok(\n  {} )\n{color_reset}", unsubscribe_response);
        }
        Err(err) => {
            println!("{color_red}Unsubscribe Result: Err( {} )\n{color_reset}", err);
        }
    }
}

async fn handle_input(value: String, client: &Mqtt5Client) -> bool {
    let args : Vec<&str> = value.split_whitespace().collect();
    if args.is_empty() {
        println!("{color_red}Invalid input!{color_reset}");
        return false;
    }

    let parsed_result = CommandArgs::from_args(&[], &args[0..]);
    if let Err(err) = parsed_result {
        if args[0].to_lowercase() == "help" || (args.len() > 1 && args[1].to_lowercase() == "--help") {
            println!("{}", err.output);
        } else {
            println!("{color_red}{}{color_reset}", err.output);
        }

        return false;
    }

    match parsed_result.unwrap().nested {
        SubCommandEnum::Start(args) => { handle_start(client, args) }
        SubCommandEnum::Stop(args) => { handle_stop(client, args) }
        SubCommandEnum::Close(args) => { handle_close(client, args) }
        SubCommandEnum::Quit(_) => { return true; }
        SubCommandEnum::Publish(args) => { handle_publish(client, args).await }
        SubCommandEnum::Subscribe(args) => { handle_subscribe(client, args).await }
        SubCommandEnum::Unsubscribe(args) => { handle_unsubscribe(client, args).await }
    }

    false
}

async fn make_tls_stream(addr: SocketAddr, endpoint: String, connector: TlsConnector) -> std::io::Result<TlsStream<TcpStream>> {
    let tcp_stream = TcpStream::connect(&addr).await?;

    let domain = pki_types::ServerName::try_from(endpoint.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))?
        .to_owned();

    connector.connect(domain, tcp_stream).await
}

fn load_private_key(keypath: &PathBuf) -> PrivateKeyDer<'static> {
    let mut reader = std::io::BufReader::new(File::open(keypath).expect("cannot open private key file"));

    loop {
        match rustls_pemfile::read_one(&mut reader).expect("cannot parse private key .pem file") {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return key.into(),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return key.into(),
            None => break,
            _ => {}
        }
    }

    panic!(
        "no keys found in {:?} (encrypted keys not supported)",
        keypath
    );
}

fn build_client(config: Mqtt5ClientOptions, runtime: &Handle, args: &CommandLineArgs) -> std::io::Result<Mqtt5Client> {
    let url_parse_result = Url::parse(&args.endpoint_uri);
    if url_parse_result.is_err() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid URL!"));
    }

    let uri = url_parse_result.unwrap();
    if uri.host_str().is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "URL must specify host!"));
    }

    let endpoint = uri.host_str().unwrap().to_string();

    if uri.port().is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "URL must specify port!"));
    }

    let port = uri.port().unwrap();

    let to_socket_addrs = (endpoint.clone(), port).to_socket_addrs();
    if to_socket_addrs.is_err()  {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to convert URL to address!"));
    }

    let addr = to_socket_addrs.unwrap().next().unwrap();

    match uri.scheme().to_lowercase().as_str() {
        "mqtt" => {
            Ok(Mqtt5Client::new(config,TokioClientOptions {
                connection_factory: Box::new(move || { Box::pin(TcpStream::connect(addr)) })
            }, runtime))
        }
        "mqtts" => {
            let mut root_cert_store = rustls::RootCertStore::empty();
            if let Some(capath) = &args.capath {
                let mut pem = std::io::BufReader::new(File::open(capath)?);
                for cert in rustls_pemfile::certs(&mut pem) {
                    root_cert_store.add(cert?).unwrap();
                }
            } else {
                for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs") {
                    root_cert_store.add(cert).unwrap();
                }
            }

            let rustls_config_builder = rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store);

            let rustls_config =
                if args.cert.is_some() && args.key.is_some() {
                    let mut reader = std::io::BufReader::new(File::open(args.cert.as_ref().unwrap())?);
                    let certs = rustls_pemfile::certs(&mut reader)
                        .map(|result| result.unwrap())
                        .collect();

                    let private_key = load_private_key(args.key.as_ref().unwrap());

                    rustls_config_builder.with_client_auth_cert(certs, private_key).unwrap()
                } else {
                    rustls_config_builder.with_no_client_auth()
                };

            let connector = TlsConnector::from(Arc::new(rustls_config));

            let tokio_options = TokioClientOptions {
                connection_factory: Box::new(move || {
                    Box::pin(make_tls_stream(addr, endpoint.clone(), connector.clone()))
                })
            };

            Ok(Mqtt5Client::new(config, tokio_options, runtime))
        }
        _ => {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "Unsupported scheme in URL!"))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli_args: CommandLineArgs = argh::from_env();

    if let Some(log_file_path) = &cli_args.logpath {
        let log_file_result = File::create(log_file_path);
        if log_file_result.is_err() {
            println!("{color_red}Could not create log file{color_reset}");
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

    println!("Elastimqtt5 - an interactive MQTT5 console\n");
    println!(" `help` for command assistance");

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if handle_input(line, &client).await {
            break;
        }
    }

    println!("Done");

    Ok(())
}
