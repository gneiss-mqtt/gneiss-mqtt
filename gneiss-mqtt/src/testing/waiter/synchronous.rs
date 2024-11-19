/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */


use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;
use crate::client::synchronous::*;
use crate::error::{MqttError, MqttResult};
use super::*;

/// Simple debug type that uses the client listener framework to allow tests to asynchronously wait for
/// configurable client event sequences.  May be useful outside of tests.  May need polish.  Currently public
/// because we use it across crates.  May eventually go internal.
///
/// Requires the client to be Arc-wrapped.
pub struct SyncClientEventWaiter {
    event_count: usize,

    client: SyncClientHandle,

    listener: Option<ListenerHandle>,

    events: Arc<Mutex<Option<Vec<ClientEventRecord>>>>,

    signal: Arc<Condvar>,
}

impl SyncClientEventWaiter {

    /// Creates a new ClientEventWaiter instance from full configuration
    pub fn new(client: SyncClientHandle, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let lock = Arc::new(Mutex::new(Some(Vec::new())));
        let signal = Arc::new(Condvar::new());

        let mut waiter = SyncClientEventWaiter {
            event_count,
            client: client.clone(),
            listener: None,
            events: lock.clone(),
            signal: signal.clone(),
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

            let mut events_guard = lock.lock().unwrap();
            let events_option = events_guard.as_mut();
            if let Some(events) = events_option {
                events.push(event_record);

                if events.len() >= event_count {
                    signal.notify_all();
                }
            }
        };

        waiter.listener = Some(client.add_event_listener(Arc::new(listener_fn)).unwrap());
        waiter
    }

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: SyncClientHandle, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new(client, config, 1)
    }

    pub fn wait(self) -> MqttResult<Vec<ClientEventRecord>> {
        let mut current_events_option = self.events.lock().unwrap();
        loop {
            match &*current_events_option {
                Some(current_events) => {
                    if current_events.len() >= self.event_count {
                        return Ok(current_events_option.take().unwrap());
                    }
                }
                None => {
                    return Err(MqttError::new_other_error("Client event waiter result already taken"));
                }
            }

            current_events_option = self.signal.wait(current_events_option).unwrap();
        }
    }
}

impl Drop for SyncClientEventWaiter {
    fn drop(&mut self) {
        let listener_handler = self.listener.take().unwrap();

        let _ = self.client.remove_event_listener(listener_handler);
    }
}