/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types and functionality for non-async MQTT clients
 */

use std::sync::Arc;
use crate::error::GneissResult;
use crate::mqtt::*;
use super::*;


/// Helper type to wait on MQTT operation results when using a sync client
pub trait SyncResultReceiver<T> {

    /// Blocking.  Waits for a result from a synchronous client MQTT operation.
    fn recv(&self) -> T;

    /// Non-blocking.  Checks if a synchronous client MQTT operation has produced a result yet.
    /// Returns the result value if so.
    fn try_recv(&self) -> Option<T>;
}

/// Return type of a Publish operation for a synchronous client.
///
/// Invoke recv() on this value to wait for the operation's result.
pub type SyncPublishResult = Arc<dyn SyncResultReceiver<PublishResult>>;

/// Return type of a Subscribe operation for a synchronous client.
///
/// Invoke recv() on this value to wait for the operation's result.
pub type SyncSubscribeResult = Arc<dyn SyncResultReceiver<SubscribeResult>>;

/// Return type of a Unsubscribe operation for a synchronous client.
///
/// Invoke recv() on this value to wait for the operation's result.
pub type SyncUnsubscribeResult = Arc<dyn SyncResultReceiver<UnsubscribeResult>>;

/// Result callback for a Publish operation on a synchronous client.
pub type SyncPublishResultCallback = Box<dyn Fn(PublishResult) + Send + Sync>;

/// Result callback for a Subscribe operation on a synchronous client.
pub type SyncSubscribeResultCallback = Box<dyn Fn(SubscribeResult) + Send + Sync>;

/// Result callback for an Unsubscribe operation on a synchronous client.
pub type SyncUnsubscribeResultCallback = Box<dyn Fn(UnsubscribeResult) + Send + Sync>;

/// Interface for a non-async network client that functions as a thin wrapper over the MQTT protocol.
///
/// There is no support for external implementations of this trait.  APIs may be added in the
/// future to support new functionality.  Existing APIs will honor standard semantic versioning.
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
/// Direct client construction is messy due to the different possibilities for TLS, proxy,
/// etc...  We encourage you to use a client builder like [ThreadedClientBuilder] to
/// simplify this process.
pub trait SyncClient {

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
    /// the broker when it reaches the head of the queue and the client is connected.  Returns
    /// a receiver type that allows for polling or a blocking wait on the result.
    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> SyncPublishResult;

    /// Submits a Publish operation to the client's operation queue.  The publish will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.  Invokes a
    /// completion callback function when the result of the operation is determined.
    fn publish_with_callback(&self, packet: PublishPacket, options: Option<PublishOptions>, completion_callback: SyncPublishResultCallback) -> GneissResult<()>;

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.  Returns
    /// a receiver type that allows for polling or a blocking wait on the result.
    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> SyncSubscribeResult;

    /// Submits a Subscribe operation to the client's operation queue.  The subscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.  Invokes a
    /// completion callback function when the result of the operation is determined.
    fn subscribe_with_callback(&self, packet: SubscribePacket, options: Option<SubscribeOptions>, completion_callback: SyncSubscribeResultCallback) -> GneissResult<()>;

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.  Returns
    /// a receiver type that allows for polling or a blocking wait on the result.
    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> SyncUnsubscribeResult;

    /// Submits an Unsubscribe operation to the client's operation queue.  The unsubscribe will be sent to
    /// the broker when it reaches the head of the queue and the client is connected.  Invokes a
    /// completion callback function when the result of the operation is determined.
    fn unsubscribe_with_callback(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>, completion_callback: SyncUnsubscribeResultCallback) -> GneissResult<()>;

    /// Adds an additional listener to the events emitted by this client.  This is useful when
    /// multiple higher-level constructs are sharing the same MQTT client.
    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<Arc<dyn ListenerHandle>>;
}

/// A non-async network client that functions as a thin wrapper over the MQTT protocol.
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
/// Direct client construction is messy due to the different possibilities for TLS, proxy,
/// etc...  We encourage you to use a client builder like [ThreadedClientBuilder] to
/// simplify this process.
//pub type SyncClientHandle = Arc<dyn SyncClient + Send + Sync>;
#[derive(Clone)]
pub struct SyncClientHandle {
    client: Arc<dyn SyncClient + Send + Sync>
}

impl SyncClientHandle {

    /// Creates a new client handle from a client
    pub fn new(client: Arc<dyn SyncClient + Send + Sync>) -> SyncClientHandle {
        SyncClientHandle {
            client,
        }
    }
}

impl SyncClient for SyncClientHandle {
    fn start(&self, default_listener: Option<Arc<ClientEventListenerCallback>>) -> GneissResult<()> {
        self.client.start(default_listener)
    }

    fn stop(&self, options: Option<StopOptions>) -> GneissResult<()> {
        self.client.stop(options)
    }

    fn close(&self) -> GneissResult<()> {
        self.client.close()
    }

    fn publish(&self, packet: PublishPacket, options: Option<PublishOptions>) -> SyncPublishResult {
        self.client.publish(packet, options)
    }

    fn publish_with_callback(&self, packet: PublishPacket, options: Option<PublishOptions>, completion_callback: SyncPublishResultCallback) -> GneissResult<()> {
        self.client.publish_with_callback(packet, options, completion_callback)
    }

    fn subscribe(&self, packet: SubscribePacket, options: Option<SubscribeOptions>) -> SyncSubscribeResult {
        self.client.subscribe(packet, options)
    }

    fn subscribe_with_callback(&self, packet: SubscribePacket, options: Option<SubscribeOptions>, completion_callback: SyncSubscribeResultCallback) -> GneissResult<()> {
        self.client.subscribe_with_callback(packet, options, completion_callback)
    }

    fn unsubscribe(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>) -> SyncUnsubscribeResult {
        self.client.unsubscribe(packet, options)
    }

    fn unsubscribe_with_callback(&self, packet: UnsubscribePacket, options: Option<UnsubscribeOptions>, completion_callback: SyncUnsubscribeResultCallback) -> GneissResult<()> {
        self.client.unsubscribe_with_callback(packet, options, completion_callback)
    }

    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<Arc<dyn ListenerHandle>> {
        self.client.add_event_listener(listener)
    }
}
