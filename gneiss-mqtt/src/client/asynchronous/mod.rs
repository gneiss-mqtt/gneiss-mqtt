/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types and functionality for async MQTT clients
 */

#[cfg(feature = "tokio")]
pub mod tokio;

use std::future::Future;
use std::pin::Pin;
use super::*;

/// Return type of a Publish operation for the asynchronous client.  Await on this value to
/// receive the operation's result, but note that the operation will complete independently of
/// the use of `.await` (you don't need to await for the operation to be performed, you only need
/// to await to get the final result of performing it).
pub type AsyncPublishResult = Pin<Box<dyn Future<Output = PublishResult> + Send>>;

/// Return type of a Subscribe operation for the asynchronous client.  Await on this value to
/// receive the operation's result, but note that the operation will complete independently of
/// the use of `.await` (you don't need to await for the operation to be performed, you only need
/// to await to get the final result of performing it).
pub type AsyncSubscribeResult = Pin<Box<dyn Future<Output = SubscribeResult> + Send>>;

/// Return type of an Unsubscribe operation for the asynchronous client.  Await on this value to
/// receive the operation's result, but note that the operation will complete independently of
/// the use of `.await` (you don't need to await for the operation to be performed, you only need
/// to await to get the final result of performing it).
pub type AsyncUnsubscribeResult = Pin<Box<dyn Future<Output = UnsubscribeResult> + Send>>;

/// Interface for An async network client that functions as a thin wrapper over the MQTT5 protocol.
///
/// A client is always in one of two states:
/// * Stopped - the client is not connected and will perform no work
/// * Not Stopped - the client will continually attempt to maintain a connection to the configured broker.
///
/// The start() and stop() APIs toggle between these two states.
///
/// The client will use configurable exponential backoff with jitter when re-establishing connections.
///
/// Regardless of the client's state, you may always safely invoke MQTT operations on it, but
/// whether or not they are rejected (due to no connection) is a function of client configuration.
///
/// There are no mutable functions in the client API, so you can safely share it amongst threads,
/// runtimes/tasks, etc...
///
/// Submitted operations are placed in a queue where they remain until they reach the head.  At
/// that point, the operation's packet is assigned a packet id (if appropriate) and encoded and
/// written to the socket.
///
/// Direct client construction is messy due to the different possibilities for TLS, async runtime,
/// etc...  We encourage you to use the various client builders in this crate, or in other crates,
/// to simplify this process.
pub trait AsyncMqttClient {

    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> MqttResult<()>;

    /// Signals the client that it should close any current connection it has and enter the
    /// Stopped state, where it does nothing.
    fn stop(&self, options: Option<StopOptions>) -> MqttResult<()>;

    /// Signals the client that it should clean up all internal resources (connection, channels,
    /// runtime tasks, etc...) and enter a terminal state that cannot be escaped.  Useful to ensure
    /// a full resource wipe.  If just `stop()` is used then the client will continue to track
    /// MQTT session state internally.
    fn close(&self) -> MqttResult<()>;

    /// Submits a Publish operation to the client's operation queue.  The publish will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> AsyncPublishResult;

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> AsyncSubscribeResult;

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.
    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> AsyncUnsubscribeResult;

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    fn add_event_listener(&self, listener: ClientEventListener) -> MqttResult<ListenerHandle>;

    /// Removes a listener from this client's set of event listeners.
    fn remove_event_listener(&self, listener: ListenerHandle) -> MqttResult<()>;
}

/// An async network client that functions as a thin wrapper over the MQTT5 protocol.
///
/// A client is always in one of two states:
/// * Stopped - the client is not connected and will perform no work
/// * Running - the client will continually attempt to maintain a connection to the configured broker.
///
/// The start() and stop() APIs toggle between these two states.
///
/// The client will use configurable exponential backoff with jitter when re-establishing connections.
///
/// Regardless of the client's state, you may always safely invoke MQTT operations on it, but
/// whether or not they are rejected (due to no connection) is a function of client configuration.
///
/// There are no mutable functions in the client API, so you can safely share it amongst threads,
/// runtimes/tasks, etc...
///
/// Submitted operations are placed in a queue where they remain until they reach the head.  At
/// that point, the operation's packet is assigned a packet id (if appropriate) and encoded and
/// written to the socket.
///
/// Direct client construction is messy due to the different possibilities for TLS, async runtime,
/// etc...  We encourage you to use the various client builders in this crate, or in other crates,
/// to simplify this process.
pub type AsyncGneissClient = Arc<dyn AsyncMqttClient + Send + Sync>;

/// A structure that holds configuration related to a client's asynchronous properties and
/// internal implementation.  Only relevant to asynchronous clients.
pub struct AsyncClientOptions {

    #[cfg(feature="tokio-websockets")]
    pub(crate) websocket_options: Option<AsyncWebsocketOptions>
}

/// Builder type for asynchronous client behavior.
pub struct AsyncClientOptionsBuilder {
    options: AsyncClientOptions
}

impl AsyncClientOptionsBuilder {

    /// Creates a new builder object for AsyncClientOptions
    pub fn new() -> Self {
        AsyncClientOptionsBuilder {
            options: AsyncClientOptions {
                #[cfg(feature="tokio-websockets")]
                websocket_options: None
            }
        }
    }

    #[cfg(feature="tokio-websockets")]
    /// Configures an asynchronous client to use websockets for MQTT transport
    pub fn with_websocket_options(&mut self, websocket_options: AsyncWebsocketOptions) -> &mut Self {
        self.options.websocket_options = Some(websocket_options);
        self
    }

    /// Builds a new set of asynchronous client options
    pub fn build(self) -> AsyncClientOptions {
        self.options
    }
}

impl Default for AsyncClientOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}