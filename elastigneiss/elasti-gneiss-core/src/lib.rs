/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use argh::FromArgs;
use gneiss_mqtt::error::{GneissError};
use gneiss_mqtt::client::*;
use std::fmt;
use std::sync::Arc;


#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "start")]
/// starts the client
pub struct StartArgs {
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "stop")]
/// stops the client
pub struct StopArgs {

    /// disconnect reason code. If none given then a disconnect will not be sent prior to stream close.
    #[argh(positional)]
    pub reason_code: Option<u8>,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "quit")]
/// causes the program to quit
pub struct QuitArgs {
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "close")]
/// closes the client, dropping any connection and rendering it unusable
pub struct CloseArgs {
}


#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "subscribe")]
/// Subscribe client command
pub struct SubscribeArgs {

    /// topic filter to subscribe to
    #[argh(positional)]
    pub topic_filter: String,

    /// subscription quality of service (0, 1, 2)
    #[argh(positional)]
    pub qos: u8,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "unsubscribe")]
/// Unsubscribe client command
pub struct UnsubscribeArgs {

    /// topic filter to unsubscribe from
    #[argh(positional)]
    pub topic_filter: String,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand, name = "publish")]
/// Publish client command
pub struct PublishArgs {

    /// topic to publish a message to
    #[argh(positional)]
    pub topic: String,

    /// quality of service (0, 1, 2)
    #[argh(positional)]
    pub qos: u8,

    /// message payload
    #[argh(positional)]
    pub payload: Option<String>,
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand)]
pub enum SubCommandEnum {
    Start(StartArgs),
    Stop(StopArgs),
    Quit(QuitArgs),
    Close(CloseArgs),
    Publish(PublishArgs),
    Subscribe(SubscribeArgs),
    Unsubscribe(UnsubscribeArgs),
}

#[derive(FromArgs, Debug, PartialEq)]
/// Elastimqtt - an interactive MQTT console
pub struct CommandArgs {
    #[argh(subcommand)]
    pub nested: SubCommandEnum,
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
            println!("Unknown client event!");
        }
    }
}
