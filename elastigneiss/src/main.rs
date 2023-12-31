/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate argh;
extern crate gneiss_mqtt;
extern crate rustls;
extern crate rustls_pemfile;
extern crate simplelog;
extern crate tokio;
extern crate tokio_rustls;
extern crate url;

use std::fs::File;
use argh::FromArgs;
use gneiss_mqtt::client;
use gneiss_mqtt::client::builder::ClientBuilder;
use simplelog::*;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::runtime::Handle;
use url::Url;

use gneiss_mqtt::*;

#[derive(FromArgs, Debug, PartialEq)]
/// Elastimqtt5 - an interactive MQTT5 console
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
            println!("Connection Failure!");
            println!("{:?}\n", event);
        }
        ClientEvent::ConnectionSuccess(event) => {
            println!("Connection Success!");
            println!("{}", event.connack);
            println!("{}\n", event.settings);
        }
        ClientEvent::Disconnection(event) => {
            println!("Disconnection!");
            println!("{:?}\n", event);
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
            println!("Invalid input!  reason_code must be a valid numeric Disconnect reason code");
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
        println!("Invalid input!  Qos must be 0, 1, or 2");
        return;
    }

    if let Some(payload) = &args.payload {
        publish.payload = Some(payload.as_bytes().to_vec());
    }

    let publish_result = client.publish(publish, PublishOptionsBuilder::new().build()).await;
    match &publish_result {
        Ok(publish_response) => {
            println!("Publish Result: Ok(\n  {} )\n", publish_response);
        }
        Err(err) => {
            println!("Publish Result: Err( {} )\n", err);
        }
    }
}

async fn handle_subscribe(client: &Mqtt5Client, args: SubscribeArgs) {
    let qos_result = convert_u8_to_quality_of_service(args.qos);
    if qos_result.is_err() {
        println!("Invalid input!  Qos must be 0, 1, or 2");
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
            println!("Subscribe Result: Ok(\n  {} )\n", subscribe_response);
        }
        Err(err) => {
            println!("Subscribe Result: Err( {} )\n", err);
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
            println!("Unsubscribe Result: Ok(\n  {} )\n", unsubscribe_response);
        }
        Err(err) => {
            println!("Unsubscribe Result: Err( {} )\n", err);
        }
    }
}

async fn handle_input(value: String, client: &Mqtt5Client) -> bool {
    let args : Vec<&str> = value.split_whitespace().collect();
    if args.is_empty() {
        println!("Invalid input!");
        return false;
    }

    let parsed_result = CommandArgs::from_args(&[], &args[0..]);
    if let Err(err) = parsed_result {
        println!("{}", err.output);

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

fn build_client(config: Mqtt5ClientOptions, runtime: &Handle, args: &CommandLineArgs) -> Mqtt5Result<Mqtt5Client> {
    let url_parse_result = Url::parse(&args.endpoint_uri);
    if url_parse_result.is_err() {
        return Err(Mqtt5Error::Unknown);
    }

    let uri = url_parse_result.unwrap();
    if uri.host_str().is_none() {
        return Err(Mqtt5Error::Unknown);
    }

    let endpoint = uri.host_str().unwrap().to_string();

    if uri.port().is_none() {
        return Err(Mqtt5Error::Unknown);
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
                ClientBuilder::new_with_mtls_from_path(&endpoint, port, args.cert.as_ref().unwrap(), args.key.as_ref().unwrap(), capath)
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
            Err(Mqtt5Error::Unknown)
            //Err(std::io::Error::new(std::io::ErrorKind::Other, "Unsupported scheme in URL!"))
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

    println!("elastigneiss - an interactive MQTT5 console application\n");
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
