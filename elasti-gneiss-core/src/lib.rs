/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use argh::FromArgs;
use gneiss_mqtt::error::{GneissError};
use gneiss_mqtt::client::*;
use gneiss_mqtt::client::asynchronous::{AsyncClientHandle};
use gneiss_mqtt::mqtt::*;
use std::fmt;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};


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

#[derive(Debug)]
pub enum ElastiError {
    Unimplemented,
    ClientError(GneissError),
    InvalidUri(String),
    UnsupportedUriScheme(String),
    MissingArguments(&'static str),
}

impl fmt::Display for ElastiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElastiError::Unimplemented => { write!(f, "unimplemented") }
            ElastiError::ClientError(inner) => {
                write!(f, "client error - {}", inner)
            }
            ElastiError::InvalidUri(uri) => {
                write!(f, "invalid uri - `{}`", uri)
            }
            ElastiError::UnsupportedUriScheme(scheme) => {
                write!(f, "invalid uri scheme - `{}`", scheme)
            }
            ElastiError::MissingArguments(args) => {
                write!(f, "missing arguments - {}", *args)
            }
        }
    }
}

impl std::error::Error for ElastiError {

}

impl From<GneissError> for ElastiError {
    fn from(value: GneissError) -> Self {
        ElastiError::ClientError(value)
    }
}

pub type ElastiResult<T> = Result<T, ElastiError>;

pub fn client_event_callback(event: Arc<ClientEvent>) {
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
        _ => {
            println!("Unknon client event!");
        }
    }
}

fn handle_start(client: &AsyncClientHandle, _: StartArgs) {
    let function = |event|{ client_event_callback(event) };
    let listener_callback = Arc::new(function);

    let _ = client.start(Some(listener_callback));
}

fn handle_stop(client: &AsyncClientHandle, args: StopArgs) {
    let mut stop_options_builder = StopOptions::builder();

    if let Some(reason_code_u8) = args.reason_code {
        if let Ok(reason_code) = DisconnectReasonCode::try_from(reason_code_u8) {
            stop_options_builder = stop_options_builder.with_disconnect_packet(DisconnectPacket::builder().with_reason_code(reason_code).build());
        } else {
            println!("Invalid input!  reason_code must be a valid numeric Disconnect reason code");
            return;
        }
    }

    let _ = client.stop(Some(stop_options_builder.build()));
}

fn handle_close(client: &AsyncClientHandle, _ : CloseArgs) {
    let _ = client.close();
}

async fn handle_publish(client: &AsyncClientHandle, args: PublishArgs) {

    let qos_result = QualityOfService::try_from(args.qos);
    if qos_result.is_err() {
        println!("Invalid input!  Qos must be 0, 1, or 2");
        return;
    }

    let mut publish_builder = PublishPacket::builder(args.topic, qos_result.unwrap());

    if let Some(payload) = &args.payload {
        publish_builder = publish_builder.with_payload(payload.as_bytes().to_vec());
    }

    let correlation_data = vec![0; 1024 * 9];
    publish_builder = publish_builder.with_correlation_data(correlation_data);

    let publish_result = client.publish(publish_builder.build(), None).await;
    match &publish_result {
        Ok(publish_response) => {
            println!("Publish Result: Ok( {} )\n", publish_response);
        }
        Err(err) => {
            println!("Publish Result: Err( {} )\n", err);
        }
    }
}

async fn handle_subscribe(client: &AsyncClientHandle, args: SubscribeArgs) {
    let qos_result = QualityOfService::try_from(args.qos);
    if qos_result.is_err() {
        println!("Invalid input!  Qos must be 0, 1, or 2");
        return;
    }

    let subscribe = SubscribePacket::builder()
        .with_subscription(Subscription::builder(args.topic_filter, qos_result.unwrap()).build())
        .build();

    let subscribe_result = client.subscribe(subscribe, None).await;

    match &subscribe_result {
        Ok(subscribe_response) => {
            println!("Subscribe Result: Ok( {} )\n", subscribe_response);
        }
        Err(err) => {
            println!("Subscribe Result: Err( {} )\n", err);
        }
    }
}

async fn handle_unsubscribe(client: &AsyncClientHandle, args: UnsubscribeArgs) {

    let unsubscribe = UnsubscribePacket::builder().with_topic_filter(args.topic_filter).build();

    let unsubscribe_result = client.unsubscribe(unsubscribe, None).await;

    match &unsubscribe_result {
        Ok(unsubscribe_response) => {
            println!("Unsubscribe Result: Ok( {} )\n", unsubscribe_response);
        }
        Err(err) => {
            println!("Unsubscribe Result: Err( {} )\n", err);
        }
    }
}

async fn handle_input(value: String, client: &AsyncClientHandle) -> bool {
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

pub async fn main_loop(client: AsyncClientHandle) {

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if handle_input(line, &client).await {
            break;
        }
    }

    println!("Done");
}
