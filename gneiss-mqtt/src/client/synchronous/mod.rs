/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types and functionality for non-async MQTT clients
 */

#[cfg(feature = "threaded")]
pub mod threaded;

use std::sync::{Arc, Condvar, Mutex};
use crate::error::GneissResult;
use crate::mqtt::*;
use super::*;

/// Helper type to wait on MQTT operation results when using a synchronous client
pub struct SyncResultReceiver<T> {
    result_lock: Arc<Mutex<Option<T>>>,
    result_signal: Arc<Condvar>
}

pub(crate) struct SyncResultSender<T> {
    result_lock: Arc<Mutex<Option<T>>>,
    result_signal: Arc<Condvar>
}

impl<T> Clone for SyncResultSender<T> {
    fn clone(&self) -> Self {
        SyncResultSender {
            result_lock: self.result_lock.clone(),
            result_signal: self.result_signal.clone()
        }
    }
}

impl<T> SyncResultSender<T> {

    pub(crate) fn new(result_lock: Arc<Mutex<Option<T>>>, result_signal: Arc<Condvar>) -> SyncResultSender<T> {
        SyncResultSender {
            result_lock,
            result_signal
        }
    }

    #[cfg_attr(not(feature="threaded"), allow(dead_code))]
    pub(crate) fn apply(&self, value: T) {
        let mut current_value = self.result_lock.lock().unwrap();

        if current_value.is_some() {
            panic!("Cannot set operation result twice!");
        }

        *current_value = Some(value);

        self.result_signal.notify_all();
    }
}

impl<T> SyncResultReceiver<T> {

    pub(crate) fn new(result_lock: Arc<Mutex<Option<T>>>, result_signal: Arc<Condvar>) -> SyncResultReceiver<T> {
        SyncResultReceiver {
            result_lock,
            result_signal
        }
    }

    /// Blocking.  Waits for a result from a synchronous client MQTT operation.
    pub fn recv(&self) -> T {
        let mut current_value = self.result_lock.lock().unwrap();
        while current_value.is_none() {
            current_value = self.result_signal.wait(current_value).unwrap();
        }

        current_value.take().unwrap()
    }

    /// Non-blocking.  Checks if a synchronous client MQTT operation has produced a result yet.
    /// Returns the result value if so.
    pub fn try_recv(&self) -> Option<T> {
        let mut current_value = self.result_lock.lock().unwrap();
        if current_value.is_none() {
            None
        } else {
            current_value.take()
        }
    }
}

#[cfg_attr(not(feature="threaded"), allow(dead_code))]
pub(crate) fn new_sync_result_pair<T>() -> (SyncResultReceiver<T>, SyncResultSender<T>) {
    let lock = Arc::new(Mutex::new(None));
    let signal = Arc::new(Condvar::new());

    (SyncResultReceiver::new(lock.clone(), signal.clone()), SyncResultSender::new(lock.clone(), signal.clone()))
}

/// Return type of a Publish operation for a synchronous client.  Invoke recv() on this value to
/// wait for the operation's result.
pub type SyncPublishResult = SyncResultReceiver<PublishResult>;

/// Return type of a Subscribe operation for a synchronous client.  Invoke recv() on this value to
/// wait for the operation's result.
pub type SyncSubscribeResult = SyncResultReceiver<SubscribeResult>;

/// Return type of a Unsubscribe operation for a synchronous client.  Invoke recv() on this value to
/// wait for the operation's result.
pub type SyncUnsubscribeResult = SyncResultReceiver<UnsubscribeResult>;

/// Result callback for a Publish operation on a synchronous client.
pub type SyncPublishResultCallback = Box<dyn Fn(PublishResult) + Send + Sync>;

/// Result callback for a Subscribe operation on a synchronous client.
pub type SyncSubscribeResultCallback = Box<dyn Fn(SubscribeResult) + Send + Sync>;

/// Result callback for an Unsubscribe operation on a synchronous client.
pub type SyncUnsubscribeResultCallback = Box<dyn Fn(UnsubscribeResult) + Send + Sync>;

/// An async network client that functions as a thin wrapper over the MQTT5 protocol.
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
    fn add_event_listener(&self, listener: ClientEventListener) -> GneissResult<ListenerHandle>;

    /// Removes a listener from this client's set of event listeners.
    fn remove_event_listener(&self, listener: ListenerHandle) -> GneissResult<()>;
}

/// A non-async network client that functions as a thin wrapper over the MQTT5 protocol.
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
pub type SyncClientHandle = Arc<dyn SyncClient + Send + Sync>;

/// A structure that holds configuration related to a client's synchronous properties and
/// internal implementation.  Only relevant to synchronous clients.
#[derive(Clone)]
pub struct SyncClientOptions {
    #[cfg(feature="threaded-websockets")]
    pub(crate) websocket_options: Option<SyncWebsocketOptions>
}

impl SyncClientOptions {

    /// Creates a new builder for SyncClientOptions instances.
    pub fn builder() -> SyncClientOptionsBuilder {
        SyncClientOptionsBuilder::new()
    }
}

/// Builder type for synchronous client behavior.
pub struct SyncClientOptionsBuilder {
    config: SyncClientOptions
}

impl SyncClientOptionsBuilder {

    /// Creates a new builder object for SyncClientOptions
    pub(crate) fn new() -> Self {
        SyncClientOptionsBuilder {
            config: SyncClientOptions {
                #[cfg(feature="threaded-websockets")]
                websocket_options: None,
            }
        }
    }

    #[cfg(feature="threaded-websockets")]
    /// Configures a synchronous client to use websockets for MQTT transport
    pub fn with_websocket_options(&mut self, websocket_options: SyncWebsocketOptions) -> &mut Self {
        self.config.websocket_options = Some(websocket_options);
        self
    }

    /// Builds a new set of synchronous client options
    pub fn build(&self) -> SyncClientOptions {
        self.config.clone()
    }
}
