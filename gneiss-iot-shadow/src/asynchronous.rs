/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
use crate::model::*;
use crate::error::ShadowResult;

use gneiss_mqtt_request_response::client::*;

use std::future::Future;
use std::pin::Pin;
use ::tokio::runtime::Handle;

pub type GetShadowResult = ShadowResult<GetShadowResponse>;
pub type AsyncGetShadowResult = Pin<Box<dyn Future<Output = GetShadowResult> + Send>>;

pub trait AsynchronousShadowClient {
    fn get_shadow(&self, request: GetShadowRequest) -> AsyncGetShadowResult;
}

struct TokioShadowClient {
    runtime: Handle,
    rr_client: ClientHandle,
}

impl AsynchronousShadowClient for TokioShadowClient {
    fn get_shadow(&self, request: GetShadowRequest) -> AsyncGetShadowResult {
        let request_options = request.to_request_response_options()?;

        self.rr_client.make_request(request_options, ??);
    }
}

#[derive(Clone)]
pub struct AsynchronousShadowClientHandle {
    client: Box<dyn AsynchronousShadowClient>,
}

impl AsynchronousShadowClient for AsynchronousShadowClientHandle {
    fn get_shadow(&self, request: GetShadowRequest) -> AsyncGetShadowResult {
        self.client.get_shadow(request)
    }
}

