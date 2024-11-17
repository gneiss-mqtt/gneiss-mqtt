/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use crate::client::asynchronous::AsyncGneissClient;
use crate::error::{MqttError, MqttResult};
use super::*;

/// Result type for calling wait() on an async client event waiter
pub type ClientEventWaitFuture = dyn Future<Output = MqttResult<Vec<ClientEventRecord>>> + Send;

/// Simple debug type that uses the client listener framework to allow tests to asynchronously wait for
/// configurable client event sequences.  May be useful outside of tests.  May need polish.  Currently public
/// because we use it across crates.  May eventually go internal.
///
/// Requires the client to be Arc-wrapped.
pub struct AsyncClientEventWaiter {
    event_count: usize,

    client: AsyncGneissClient,

    listener: Option<ListenerHandle>,

    event_receiver: tokio::sync::mpsc::UnboundedReceiver<ClientEventRecord>,

    events: Vec<ClientEventRecord>,
}

impl AsyncClientEventWaiter {

    /// Creates a new ClientEventWaiter instance from full configuration
    pub fn new(client: AsyncGneissClient, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let mut waiter = AsyncClientEventWaiter {
            event_count,
            client: client.clone(),
            listener: None,
            event_receiver: rx,
            events: Vec::new(),
        };

        let listener_fn = move |event: Arc<ClientEvent>| {
            match &config.wait_type {
                ClientEventWaitType::Type(event_type) => {
                    if !client_event_matches(&event, *event_type) {
                        return;
                    }
                }
                ClientEventWaitType::Predicate(event_predicate) => {
                    if !(*event_predicate)(&event) {
                        return;
                    }
                }
            }

            let event_record = ClientEventRecord {
                event: event.clone(),
                timestamp: Instant::now(),
            };

            let _ = tx.send(event_record);
        };

        waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
        waiter
    }

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: AsyncGneissClient, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new(client, config, 1)
    }

    /// Waits for and returns an event sequence that matches the original configuration
    pub fn wait(mut self) -> Pin<Box<ClientEventWaitFuture>> {
        Box::pin(async move {
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
        })
    }
}

impl Drop for AsyncClientEventWaiter {
    fn drop(&mut self) {
        let listener_handler = self.listener.take().unwrap();

        let _ = self.client.remove_event_listener(listener_handler);
    }
}