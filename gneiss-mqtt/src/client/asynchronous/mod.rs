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
pub trait AsyncClient {

    /// Signals the client that it should attempt to recurrently maintain a connection to
    /// the broker endpoint it has been configured with.
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> GneissResult<()>;

    /// Signals the client that it should close any current connection it has and enter the
    /// Stopped state, where it does nothing.
    fn stop(&self, options: Option<StopOptions>) -> GneissResult<()>;

    /// Signals the client that it should clean up all internal resources (connection, channels,
    /// runtime tasks, etc...) and enter a terminal state that cannot be escaped.  Useful to ensure
    /// a full resource wipe.  If just `stop()` is used then the client will continue to track
    /// MQTT session state internally.
    fn close(&self) -> GneissResult<()>;

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
    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<ListenerHandle>;

    /// Removes a listener from this client's set of event listeners.
    fn remove_event_listener(&self, listener: ListenerHandle) -> GneissResult<()>;
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
#[derive(Clone)]
pub struct AsyncClientHandle {
    client: Arc<dyn AsyncClient + Send + Sync>
}

impl AsyncClientHandle {

    #[cfg_attr(not(any(feature = "tokio-rustls", feature = "tokio-native-tls", feature = "tokio-websockets")), allow(dead_code))]
    pub(crate) fn new(client: Arc<dyn AsyncClient + Send + Sync>) -> Self {
        Self { client }
    }
}

impl AsyncClient for AsyncClientHandle {
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> GneissResult<()> {
        self.client.start(default_listener)
    }

    fn stop(&self, options: Option<StopOptions>) -> GneissResult<()> {
        self.client.stop(options)
    }

    fn close(&self) -> GneissResult<()> {
        self.client.close()
    }

    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> AsyncPublishResult {
        self.client.publish(packet, options)
    }

    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> AsyncSubscribeResult {
        self.client.subscribe(packet, options)
    }

    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> AsyncUnsubscribeResult {
        self.client.unsubscribe(packet, options)
    }

    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<ListenerHandle> {
        self.client.add_event_listener(listener)
    }

    fn remove_event_listener(&self, listener: ListenerHandle) -> GneissResult<()> {
        self.client.remove_event_listener(listener)
    }
}

/// A structure that holds configuration related to a client's asynchronous properties and
/// internal implementation.  Only relevant to asynchronous clients.
#[derive(Clone)]
pub struct AsyncClientOptions {

    #[cfg(feature="tokio-websockets")]
    pub(crate) websocket_options: Option<AsyncWebsocketOptions>
}

impl AsyncClientOptions {

    /// Creates a new builder for AsyncClientOptions instances
    pub fn builder() -> AsyncClientOptionsBuilder {
        AsyncClientOptionsBuilder::new()
    }
}

/// Builder type for asynchronous client behavior.
pub struct AsyncClientOptionsBuilder {
    options: AsyncClientOptions
}

impl AsyncClientOptionsBuilder {

    /// Creates a new builder object for AsyncClientOptions
    pub(crate) fn new() -> Self {
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
    pub fn build(&self) -> AsyncClientOptions {
        self.options.clone()
    }
}
