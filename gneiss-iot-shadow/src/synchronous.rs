/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
use gneiss_mqtt::client::{new_sync_result_pair, SyncResultReceiver};
use gneiss_mqtt_request_response::client::*;
use crate::model::*;
use crate::error::*;

pub type SyncGetShadowResult = SyncResultReceiver<GetShadowResult>;

pub trait SynchronousShadowClient {
    fn get_shadow(&self, request: GetShadowRequest) -> SyncGetShadowResult;
}

pub type GetShadowResultCallback = Box<dyn FnOnce(GetShadowResult) -> () + Send + Sync>;
pub trait CallbackShadowClient {
    fn get_shadow(&self, request: GetShadowRequest, callback: GetShadowResultCallback) -> ShadowResult<()>;
}

struct ThreadedShadowClient {
    rr_client: ClientHandle,
}

impl SynchronousShadowClient for ThreadedShadowClient {
    fn get_shadow(&self, request: GetShadowRequest) -> SyncGetShadowResult {
        let (recv, send) = new_sync_result_pair();

        match request.to_request_response_options() {
            Err(e) => {
                send.apply(Err(e));
            }
            Ok(request_options) => {
                let accepted_topic = request_options.response_paths()[0].topic().to_string();
                let rejected_topic = request_options.response_paths()[1].topic().to_string();
                let send_clone = send.clone();

                let response_handler = Box::new(move |res| {
                    let typed_result : GetShadowResult = convert_json_response::<GetShadowResponse, ServiceErrorResponse>(res, &accepted_topic, &rejected_topic, &ModeledServiceException::from);
                    send_clone.apply(typed_result);
                    Ok(())
                });

                if let Err(err) = self.rr_client.make_request(request_options, response_handler) {
                    send.apply(Err(err.into()))
                }
            }
        }

        recv
    }
}

impl CallbackShadowClient for ThreadedShadowClient {
    fn get_shadow(&self, request: GetShadowRequest, callback: GetShadowResultCallback) -> ShadowResult<()> {
        match request.to_request_response_options() {
            Err(e) => {
                Err(e)
            }
            Ok(request_options) => {
                let accepted_topic = request_options.response_paths()[0].topic().to_string();
                let rejected_topic = request_options.response_paths()[1].topic().to_string();

                let response_handler = Box::new(move |res| {
                    let typed_result : GetShadowResult = convert_json_response::<GetShadowResponse, ServiceErrorResponse>(res, &accepted_topic, &rejected_topic, &ModeledServiceException::from);
                    callback(typed_result);
                    Ok(())
                });

                if let Err(err) = self.rr_client.make_request(request_options, response_handler) {
                    Err(err.into())
                } else {
                    Ok(())
                }
            }
        }
    }
}