/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use gneiss_mqtt::client::*;
use gneiss_mqtt::client::waiter::*;
use gneiss_mqtt::error::*;

use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// Simple debug type that uses the client listener framework to allow tests to wait for
/// configurable client event sequences.
pub struct ThreadedClientEventWaiter {
    event_count: usize,

    listener: Option<Arc<dyn ListenerHandle>>,

    events: Arc<Mutex<Option<Vec<ClientEventRecord>>>>,

    signal: Arc<Condvar>,
}

impl ThreadedClientEventWaiter {

    fn new_internal(client: SyncClientHandle, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        let lock = Arc::new(Mutex::new(Some(Vec::new())));
        let signal = Arc::new(Condvar::new());

        let mut waiter = ThreadedClientEventWaiter {
            event_count,
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

    /// Creates a new ClientEventWaiter instance from full configuration
    #[cfg(feature = "testing")]
    pub fn new(client: SyncClientHandle, config: ClientEventWaiterOptions, event_count: usize) -> Self {
        Self::new_internal(client, config, event_count)
    }

    /// Creates a new ClientEventWaiter instance that will wait for a single occurrence of a single event type
    pub fn new_single(client: SyncClientHandle, event_type: ClientEventType) -> Self {
        let config = ClientEventWaiterOptions {
            wait_type: ClientEventWaitType::Type(event_type),
        };

        Self::new_internal(client, config, 1)
    }

    /// Waits for the configured event(s) and returns a result with them
    pub fn wait(self) -> GneissResult<Vec<ClientEventRecord>> {
        let mut current_events_option = self.events.lock().unwrap();
        loop {
            match &*current_events_option {
                Some(current_events) => {
                    if current_events.len() >= self.event_count {
                        return Ok(current_events_option.take().unwrap());
                    }
                }
                None => {
                    return Err(GneissError::new_other_error("Client event waiter result already taken"));
                }
            }

            current_events_option = self.signal.wait(current_events_option).unwrap();
        }
    }
}