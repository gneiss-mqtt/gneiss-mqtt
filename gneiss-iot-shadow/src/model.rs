/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use gneiss_mqtt_request_response::client::{RequestOptions, RequestOptionsBuilder, ResponsePath};
use crate::error::*;

#[derive(Clone, Default, Serialize)]
pub struct GetShadowRequest {

    #[serde(skip)]
    pub(crate) thing_name: String,

    #[serde(rename = "clientToken")]
    pub(crate) client_token: String,
}

impl GetShadowRequest {
    pub fn builder() -> GetShadowRequestBuilder {
        GetShadowRequestBuilder::new()
    }

    pub fn to_json_payload(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }

    pub fn validate(&self) -> ShadowResult<()> {
        Ok(())
    }

    pub fn to_request_response_options(mut self) -> ShadowResult<RequestOptions> {
        self.validate()?;

        let correlation_token = uuid::Uuid::new_v4().to_string();
        self.client_token = correlation_token.clone();

        let publish_topic = format!("$aws/things/{}/shadow/get", self.thing_name);
        let payload = self.to_json_payload();

        let mut builder = RequestOptions::builder(publish_topic, payload);
        builder.with_subscription(format!("$aws/things/{}/shadow/get/+", self.thing_name));
        builder.with_response_path(ResponsePath::new(format!("$aws/things/{}/shadow/get/accepted", self.thing_name), Some("clientToken".to_string())));
        builder.with_response_path(ResponsePath::new(format!("$aws/things/{}/shadow/get/rejected", self.thing_name), Some("clientToken".to_string())));
        builder.with_correlation_token(correlation_token);

        from_request_response_result(builder.build())
    }
}

pub struct GetShadowRequestBuilder {
    options: GetShadowRequest,
}

impl GetShadowRequestBuilder {
    pub(crate) fn new() -> Self {
        GetShadowRequestBuilder {
            options: GetShadowRequest {
                ..Default::default()
            }
        }
    }

    pub fn with_thing_name(&mut self, thing_name: &str) -> &mut Self {
        self.options.thing_name = thing_name.into();

        self
    }

    pub fn build(self) -> GetShadowRequest {
        self.options
    }
}

#[derive(Clone, Deserialize)]
pub struct ShadowStateWithDelta {
    pub desired: serde_json::Value,
    pub reported: serde_json::Value,
    pub delta: serde_json::Value,
}

#[derive(Clone, Deserialize)]
pub struct ShadowMetadata {
    pub desired: serde_json::Value,
    pub reported: serde_json::Value,
}

#[derive(Clone, Deserialize)]
pub struct GetShadowResponse {
    #[serde(rename = "clientToken")]
    pub client_token: String,

    pub state: ShadowStateWithDelta,

    pub metadata: ShadowMetadata,

    pub timestamp: DateTime<chrono::Utc>,

    pub version: i32,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServiceErrorResponse {
    #[serde(rename = "clientToken")]
    pub client_token: String,

    pub code: i32,

    pub message: String,

    pub timestamp: DateTime<chrono::Utc>,
}
