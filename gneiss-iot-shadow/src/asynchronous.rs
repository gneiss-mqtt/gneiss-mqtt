/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
use crate::model::*;
use crate::error::*;

use gneiss_mqtt_request_response::client::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use ::tokio::runtime::Handle;
use gneiss_mqtt_request_response::error::RequestResponseError;

pub type GetShadowResult = ShadowResult<GetShadowResponse>;
pub type AsyncGetShadowResult = Pin<Box<dyn Future<Output = GetShadowResult> + Send>>;

macro_rules! submit_shadow_operation_tokio {
    ($self:ident, $request:ident, $response_type:ident, $error_type:ident, $result_type:ident) => ({

        let (response_sender, rx) = tokio::sync::oneshot::channel();

        let mut submit_error_option = None;
        match $request.to_request_response_options() {
            Err(e) => {
                submit_error_option = Some(e)
            }
            Ok(request_options) => {
                let accepted_topic = request_options.response_paths()[0].topic().to_string();
                let rejected_topic = request_options.response_paths()[1].topic().to_string();

                let response_handler = Box::new(move |res| {
                    let typed_result : $result_type = convert_json_response::<$response_type, $error_type>(res, &accepted_topic, &rejected_topic, &ModeledServiceException::from);
                    if response_sender.send(typed_result).is_err() {
                        return Err(RequestResponseError::new_operation_channel_failure("Failed to send the result on the result channel"));
                    }

                    Ok(())
                });

                if let Err(err) = $self.rr_client.make_request(request_options, response_handler) {
                    submit_error_option = Some(ShadowError::new_request_response(err))
                }
            }
        }

        Box::pin(async move {
            match submit_error_option {
                Some(error) => {
                    Err(ShadowError::new_operation_channel_failure(error))
                }
                _ => {
                    match rx.await {
                        Err(e) => {
                            Err(ShadowError::new_operation_channel_failure(e))
                        }
                        Ok(res) => {
                            res
                        }
                    }
                }
            }
        })
    })
}

pub trait AsynchronousShadowClient {
    fn get_shadow(&self, request: GetShadowRequest) -> AsyncGetShadowResult;
}

struct TokioShadowClient {
    rr_client: ClientHandle,
}

impl AsynchronousShadowClient for TokioShadowClient {
    fn get_shadow(&self, request: GetShadowRequest) -> AsyncGetShadowResult {
        submit_shadow_operation_tokio!(self, request, GetShadowResponse, ServiceErrorResponse, GetShadowResult)
    }
}

#[derive(Clone)]
pub struct AsynchronousShadowClientHandle {
    client: Arc<dyn AsynchronousShadowClient + Send + Sync>,
}

impl AsynchronousShadowClient for AsynchronousShadowClientHandle {
    fn get_shadow(&self, request: GetShadowRequest) -> AsyncGetShadowResult {
        self.client.get_shadow(request)
    }
}
