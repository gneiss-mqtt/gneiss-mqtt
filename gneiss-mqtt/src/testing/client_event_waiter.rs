/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::error::{MqttError, MqttResult};
use crate::client::{ClientEvent, ListenerHandle, Mqtt5Client};

use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum ClientEventType {
    ConnectionAttempt,
    ConnectionSuccess,
    ConnectionFailure,
    Disconnection,
    Stopped,
    PublishReceived,
}

fn client_event_matches(event: &Arc<ClientEvent>, event_type: ClientEventType) -> bool {
    match **event {
        ClientEvent::ConnectionAttempt(_) => { event_type == ClientEventType::ConnectionAttempt }
        ClientEvent::ConnectionSuccess(_) => { event_type == ClientEventType::ConnectionSuccess }
        ClientEvent::ConnectionFailure(_) => { event_type == ClientEventType::ConnectionFailure }
        ClientEvent::Disconnection(_) => { event_type == ClientEventType::Disconnection }
        ClientEvent::Stopped(_) => { event_type == ClientEventType::Stopped }
        ClientEvent::PublishReceived(_) => { event_type == ClientEventType::PublishReceived }
    }
}

type ClientEventPredicate = dyn Fn(&Arc<ClientEvent>) -> bool + Send + Sync;

pub(crate) struct ClientEventWaiterOptions {
    event_type: ClientEventType,

    event_predicate: Option<Box<ClientEventPredicate>>,
}

pub(crate) struct TokioClientEventWaiter {
    event_count: usize,

    client: Arc<Mqtt5Client>,

    listener: Option<ListenerHandle>,

    event_receiver: tokio::sync::mpsc::UnboundedReceiver<Arc<ClientEvent>>,

    events: Vec<Arc<ClientEvent>>,
}

impl TokioClientEventWaiter {
    pub(crate) fn new(client: Arc<Mqtt5Client>, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let event_type = config.event_type;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut waiter = TokioClientEventWaiter {
            event_count,
            client: client.clone(),
            listener: None,
            event_receiver: rx,
            events: Vec::new(),
        };

        let listener_fn = move |event: Arc<ClientEvent>| {
            if !client_event_matches(&event, event_type) {
                return;
            }

            if let Some(event_predicate) = &config.event_predicate {
                if !(*event_predicate)(&event) {
                    return;
                }
            }

            let _ = tx.send(event.clone());
        };

        waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
        waiter
    }

    pub fn new_single(client: Arc<Mqtt5Client>, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            event_type,
            event_predicate: None
        };

        Self::new(client, config, 1)
    }

    pub(crate) async fn wait(&mut self) -> MqttResult<Vec<Arc<ClientEvent>>> {
        while self.events.len() < self.event_count {
            match self.event_receiver.recv().await {
                None => {
                    return Err(MqttError::new_other_error("Channel closed"));
                }
                Some(event) => {
                    self.events.push(event);
                }
            }
        }

        Ok(self.events.clone())
    }
}

impl Drop for TokioClientEventWaiter {
    fn drop(&mut self) {
        let listener_handler = self.listener.take().unwrap();

        let _ = self.client.remove_event_listener(listener_handler);
    }
}