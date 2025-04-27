/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
use std::sync::Arc;
use std::time::Duration;
use gneiss_mqtt::mqtt::PublishPacket;
use crate::error::*;

#[cfg(feature="tokio")]
pub mod asynchronous;
#[cfg(feature="threaded")]
pub mod synchronous;

#[derive(Default, Copy, Clone)]
pub struct ClientOptions {
    pub(crate) operation_timeout: Option<Duration>,
    pub(crate) max_request_response_subscriptions: u32,
    pub(crate) max_streaming_subscriptions: u32,
}

impl ClientOptions {
    pub fn builder() -> ClientOptionsBuilder {
        ClientOptionsBuilder::new()
    }
}

#[derive(Default, Copy, Clone)]
pub struct ClientOptionsBuilder {
    options: ClientOptions
}

impl ClientOptionsBuilder {
    pub fn with_operation_timeout(&mut self, timeout: Option<Duration>) -> &mut Self {
        self.options.operation_timeout = timeout;
        self
    }

    pub fn with_max_request_response_subscriptions(&mut self, max_request_response_subscriptions: u32) -> &mut Self {
        self.options.max_request_response_subscriptions = max_request_response_subscriptions;
        self
    }

    pub fn with_max_streaming_subscriptions(&mut self, max_streaming_subscriptions: u32) -> &mut Self {
        self.options.max_streaming_subscriptions = max_streaming_subscriptions;
        self
    }

    pub fn build(&self) -> ClientOptions {
        self.options
    }

    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
}

#[derive(Default, Clone)]
pub struct ResponsePath {
    pub(crate) topic: String,
    pub(crate) correlation_token_json_path: Option<String>,
}

impl ResponsePath {
    pub fn new(topic: String, correlation_token_json_path: Option<String>) -> Self {
        Self {
            topic,
            correlation_token_json_path,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

#[derive(Clone)]
pub struct RequestOptions {
    pub(crate) publish_topic: String,
    pub(crate) subscriptions: Vec<String>,
    pub(crate) payload: Vec<u8>,
    pub(crate) response_paths: Vec<ResponsePath>,
    pub(crate) correlation_token: Option<String>,
}

impl RequestOptions {
    pub fn builder(publish_topic: String, payload: Vec<u8>) -> RequestOptionsBuilder {
        RequestOptionsBuilder::new(publish_topic, payload)
    }

    pub fn response_paths(&self) -> &[ResponsePath] {
        &self.response_paths
    }
}

pub struct RequestOptionsBuilder {
    options: RequestOptions,
}

impl RequestOptionsBuilder {
    fn is_valid_configuration(&self) -> bool {
        !self.options.subscriptions.is_empty() && !self.options.response_paths.is_empty()
    }

    pub(crate) fn new(publish_topic: String, payload: Vec<u8>) -> Self {
        Self {
            options: RequestOptions {
                publish_topic,
                subscriptions: Vec::new(),
                payload,
                response_paths: Vec::new(),
                correlation_token: None,
            }
        }
    }

    pub fn with_subscription(&mut self, subscription: String) -> &mut Self {
        self.options.subscriptions.push(subscription);

        self
    }

    pub fn with_response_path(&mut self, response_path: ResponsePath) -> &mut Self {
        self.options.response_paths.push(response_path);

        self
    }

    pub fn with_correlation_token(&mut self, correlation_token: String) -> &mut Self {
        self.options.correlation_token = Some(correlation_token);

        self
    }

    pub fn build(mut self) -> RequestResponseResult<RequestOptions> {
        if !self.is_valid_configuration() {
            Err(RequestResponseError::new_invalid_configuration())
        } else {
            Ok(self.options)
        }
    }
}

#[derive(Clone)]
pub struct Response {
    message: PublishPacket,
}

impl Response {
    pub fn message(&self) -> &PublishPacket {
        &self.message
    }

    pub(crate) fn new(message: PublishPacket) -> Self {
        Self {
            message
        }
    }
}

#[derive(Clone)]
pub struct StreamingOperationMessage {
    message: PublishPacket,
}

impl StreamingOperationMessage {
    pub fn message(&self) -> &PublishPacket {
        &self.message
    }
}

pub type StreamingOperationMessageHandler = dyn Fn(Arc<StreamingOperationMessage>) + Send + Sync;

pub enum StreamingOperationEvent {

    /// The streaming operation is successfully subscribed to its topic (filter)
    SubscriptionEstablished,

    /// The streaming operation has temporarily lost its subscription to its topic (filter)
    SubscriptionLost(RequestResponseError),

    /// The streaming operation has entered a terminal state where it has given up trying to subscribe
    /// to its topic (filter).  This is always due to user error (bad topic filter or IoT Core permission policy).
    SubscriptionHalted(RequestResponseError),
}

pub type StreamingOperationEventHandler = dyn Fn(Arc<StreamingOperationEvent>) + Send + Sync;

#[derive(Clone)]
pub struct StreamingOperationOptions {
    pub(crate) topic_filter: String,

    pub(crate) event_handler: Option<Arc<StreamingOperationEventHandler>>,

    pub(crate) message_handler: Arc<StreamingOperationMessageHandler>
}

impl StreamingOperationOptions {
    pub fn builder(topic_filter: String, message_handler: Arc<StreamingOperationMessageHandler>) -> StreamingOperationOptionsBuilder {
        StreamingOperationOptionsBuilder::new(topic_filter, message_handler)
    }
}

pub struct StreamingOperationOptionsBuilder {
    options: StreamingOperationOptions,
}

impl StreamingOperationOptionsBuilder {

    pub fn new(topic_filter: String, message_handler: Arc<StreamingOperationMessageHandler>) -> Self {
        Self {
            options: StreamingOperationOptions {
                topic_filter,
                event_handler: None,
                message_handler
            }
        }
    }

    pub fn with_event_handler(&mut self, event_handler: Arc<StreamingOperationEventHandler>) -> &mut Self {
        self.options.event_handler = Some(event_handler);

        self
    }

    pub fn build(self) -> StreamingOperationOptions {
        self.options
    }
}

pub trait StreamingOperation {

    fn open(&self) -> ();

    fn close(&self) -> ();

}

pub struct StreamingOperationHandle {
    stream: Arc<dyn StreamingOperation>,
}

impl StreamingOperation for StreamingOperationHandle {
    fn open(&self) -> () {
        self.stream.open()
    }

    fn close(&self) -> () {
        self.stream.close()
    }
}

pub(crate) type ResponseHandler = Box<dyn FnOnce(RequestResponseResult<Response>) -> RequestResponseResult<()> + Send + Sync>;

pub trait Client {

    fn make_request(&self, options: RequestOptions, response_handler: ResponseHandler) -> RequestResponseResult<()>;

    fn create_stream(&self, options: StreamingOperationOptions) -> RequestResponseResult<StreamingOperationHandle>;
}

#[derive(Clone)]
pub struct ClientHandle {
    client: Arc<dyn Client + Send + Sync>
}

impl Client for ClientHandle {

    fn make_request(&self, options: RequestOptions, response_handler: ResponseHandler) -> RequestResponseResult<()> {
        self.client.make_request(options, response_handler)
    }

    fn create_stream(&self, options: StreamingOperationOptions) -> RequestResponseResult<StreamingOperationHandle> {
        self.client.create_stream(options)
    }
}

// protocol adapter

pub(crate) struct SubscriptionEventContext {
    pub(crate) topic_filter: String,
    pub(crate) outcome: RequestResponseResult<()>,
    pub(crate) retryable: bool,
}

pub(crate) enum SubscriptionResultEvent {
    Subscribe(SubscriptionEventContext),
    Unsubscribe(SubscriptionEventContext),
}

pub(crate) struct IncomingPublishEvent {
    pub(crate) publish: Arc<PublishPacket>,
}

pub(crate) struct ConnectedContext {
    pub(crate) rejoined_session: bool
}

pub(crate) enum ConnectionStatusEvent {
    Connected(ConnectedContext),
    Disconnected,
}

pub(crate) enum ProtocolAdapterEvent {
    Publish(IncomingPublishEvent),
    Subscription(SubscriptionResultEvent),
    Connection(ConnectionStatusEvent)
}

pub(crate) struct SubscribeOptions {
    pub(crate) topic_filter: String,
    pub(crate) timeout: Duration,
}

pub(crate) struct UnsubscribeOptions {
    pub(crate) topic_filter: String,
    pub(crate) timeout: Duration,
}

pub(crate) struct PublishOptions {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) timeout: Duration
}

pub(crate) trait ProtocolAdapter {

    fn subscribe(&self, options: SubscribeOptions) -> RequestResponseResult<()>;

    fn unsubscribe(&self, options: UnsubscribeOptions) -> RequestResponseResult<()>;

    fn publish(&self, options: PublishOptions) -> RequestResponseResult<()>;
}

