/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
use std::time::Duration;
use crate::error::*;

pub mod asynchronous;
pub mod synchronous;

#[derive(Default, Copy, Clone)]
pub struct RequestResponseClientOptions {
    operation_timeout: Option<Duration>,
    max_request_response_subscriptions: u32,
    max_streaming_subscriptions: u32,
}

impl RequestResponseClientOptions {
    pub fn builder() -> RequestResponseClientOptionsBuilder {
        RequestResponseClientOptionsBuilder::new()
    }
}

#[derive(Default, Copy, Clone)]
pub struct RequestResponseClientOptionsBuilder {
    options: RequestResponseClientOptions
}

impl RequestResponseClientOptionsBuilder {
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

    pub fn build(&self) -> RequestResponseClientOptions {
        self.options
    }

    fn new() -> RequestResponseClientOptionsBuilder {
        RequestResponseClientOptionsBuilder {
            ..Default::default()
        }
    }
}

#[derive(Default, Clone)]
pub struct RequestResponsePath {
    topic: String,
    correlation_token_json_path: Option<String>,
}

impl RequestResponsePath {
    pub fn new(topic: String, correlation_token_json_path: Option<String>) -> Self {
        RequestResponsePath {
            topic,
            correlation_token_json_path,
        }
    }
}

#[derive(Clone)]
pub struct RequestResponseOptions {
    publish_topic: String,
    subscriptions: Vec<String>,
    payload: Vec<u8>,
    response_paths: Vec<RequestResponsePath>,
    correlation_token: Option<String>,

}

impl RequestResponseOptions {
    pub fn builder() -> RequestResponseOptionsBuilder {
        RequestResponseOptionsBuilder::new()
    }
}

#[derive(Default)]
pub struct RequestResponseOptionsBuilder {
    publish_topic: Option<String>,
    subscriptions: Vec<String>,
    payload: Vec<u8>,
    response_paths: Vec<RequestResponsePath>,
    correlation_token: Option<String>,
}

impl RequestResponseOptionsBuilder {
    fn is_valid_configuration(&self) -> bool {
        self.publish_topic.is_some() && !self.subscriptions.is_empty() && !self.response_paths.is_empty()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> RequestResponseResult<RequestResponseOptions> {
        if !self.is_valid_configuration() {
            Err(RequestResponseError::new_invalid_configuration())
        } else {
            Ok(RequestResponseOptions{
                publish_topic: self.publish_topic.unwrap(),
                subscriptions: self.subscriptions,
                payload: self.payload,
                response_paths: self.response_paths,
                correlation_token: self.correlation_token,
            })
        }
    }
}