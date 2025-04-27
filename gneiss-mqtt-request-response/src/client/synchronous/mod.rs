/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use gneiss_mqtt::client::{ClientEvent, SyncClient, SyncClientHandle};
use gneiss_mqtt::mqtt::*;
use crate::client::*;
use crate::client::ConnectionStatusEvent::Connected;
use crate::error::*;

pub(crate) struct ThreadedClientProtocolAdapter {
    protocol_client: SyncClientHandle,

    event_sender: std::sync::mpsc::Sender<ProtocolAdapterEvent>,

    listener_handle: gneiss_mqtt::client::ListenerHandle,
}

impl ProtocolAdapter for ThreadedClientProtocolAdapter {
    fn subscribe(&self, options: SubscribeOptions) -> RequestResponseResult<()> {
        let topic_filter = options.topic_filter.clone();

        let subscribe = SubscribePacket::builder()
            .with_subscription_simple(options.topic_filter, QualityOfService::AtLeastOnce)
            .build();

        let subscribe_options = gneiss_mqtt::client::SubscribeOptions::builder()
            .with_ack_timeout(options.timeout)
            .build();

        let completion_output_channel = self.event_sender.clone();
        let completion_callback = Box::new(move |result| {
            let mut event = SubscriptionEventContext {
                topic_filter,
                outcome: Ok(()),
                retryable: false,
            };

            if let Err(e) = result {
                event.outcome = Err(RequestResponseError::from(e));
            }

            let _ = completion_output_channel.send(
                ProtocolAdapterEvent::Subscription(
                    SubscriptionResultEvent::Subscribe(event)
                )
            );
        });

        self.protocol_client.subscribe_with_callback(subscribe, Some(subscribe_options), completion_callback)
            .map_err(RequestResponseError::from)
    }

    fn unsubscribe(&self, options: UnsubscribeOptions) -> RequestResponseResult<()> {
        let topic_filter = options.topic_filter.clone();

        let unsubscribe = UnsubscribePacket::builder()
            .with_topic_filter(options.topic_filter)
            .build();

        let unsubscribe_options = gneiss_mqtt::client::UnsubscribeOptions::builder()
            .with_ack_timeout(options.timeout)
            .build();

        let completion_output_channel = self.event_sender.clone();
        let completion_callback = Box::new(move |result| {
            let mut event = SubscriptionEventContext {
                topic_filter,
                outcome: Ok(()),
                retryable: false,
            };

            if let Err(e) = result {
                event.outcome = Err(RequestResponseError::from(e));
            }

            let _ = completion_output_channel.send(
                ProtocolAdapterEvent::Subscription(
                    SubscriptionResultEvent::Unsubscribe(event)
                )
            );
        });

        self.protocol_client.unsubscribe_with_callback(unsubscribe, Some(unsubscribe_options), completion_callback)
            .map_err(RequestResponseError::from)
    }

    fn publish(&self, options: PublishOptions) -> RequestResponseResult<()> {
        let topic = options.topic.clone();

        let publish = PublishPacket::builder(topic, QualityOfService::AtLeastOnce)
            .with_payload(options.payload)
            .build();

        let publish_options = gneiss_mqtt::client::PublishOptions::builder()
            .with_ack_timeout(options.timeout)
            .build();

        self.protocol_client.publish_with_callback(publish, Some(publish_options), Box::new(|_result|{}))
            .map_err(RequestResponseError::from)
    }
}

impl Drop for ThreadedClientProtocolAdapter {
    fn drop(&mut self) {
        self.protocol_client.remove_event_listener(self.listener_handle.clone()).unwrap()
    }
}

fn client_event_handler(event_sender: &std::sync::mpsc::Sender<ProtocolAdapterEvent>, event: Arc<ClientEvent>) {
    match &*event {
        ClientEvent::ConnectionSuccess(success_event) => {
            let _ = event_sender.send(
                ProtocolAdapterEvent::Connection(ConnectionStatusEvent::Connected(ConnectedContext{
                    rejoined_session: success_event.connack.session_present()
                }))
            );
        }
        ClientEvent::Disconnection(disconnection_event) => {
            let _ = event_sender.send(
                ProtocolAdapterEvent::Connection(ConnectionStatusEvent::Disconnected)
            );
        }
        ClientEvent::PublishReceived(publish_event) => {
            let _ = event_sender.send(
                ProtocolAdapterEvent::Publish(IncomingPublishEvent{
                    publish: publish_event.publish.clone(),
                })
            );
        }
        ClientEvent::ListenerInitialStatus(status_event) => {
            let conn_event =
                if status_event.connected {
                    ProtocolAdapterEvent::Connection(ConnectionStatusEvent::Connected(ConnectedContext{
                        rejoined_session: false,
                    }))
                } else {
                    ProtocolAdapterEvent::Connection(ConnectionStatusEvent::Disconnected)
                };

            let _ = event_sender.send(conn_event);
        }
        _ => {}
    }
}

impl ThreadedClientProtocolAdapter {
    pub(crate) fn new(protocol_client: SyncClientHandle, event_sender: std::sync::mpsc::Sender<ProtocolAdapterEvent>) -> RequestResponseResult<Self> {
        let sender_clone = event_sender.clone();
        let handler = Arc::new(move |event : Arc<ClientEvent>| {
            client_event_handler(&sender_clone, event);
        });

        match protocol_client.add_event_listener(handler) {
            Ok(listener_handle) => {
                Ok(ThreadedClientProtocolAdapter {
                    protocol_client,
                    event_sender,
                    listener_handle,
                })
            },
            Err(e) => {
                Err(RequestResponseError::from(e))
            }
        }
    }
}