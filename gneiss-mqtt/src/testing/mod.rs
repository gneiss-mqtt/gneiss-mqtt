/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub(crate) mod integration;
pub(crate) mod protocol;

use std::sync::Arc;
use crate::client::ClientEvent;

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub(crate) enum ClientEventType {
    ConnectionAttempt,
    ConnectionSuccess,
    ConnectionFailure,
    Disconnection,
    Stopped,
    PublishReceived,
}

pub(crate) fn client_event_matches(event: &Arc<ClientEvent>, event_type: ClientEventType) -> bool {
    match **event {
        ClientEvent::ConnectionAttempt(_) => { event_type == ClientEventType::ConnectionAttempt }
        ClientEvent::ConnectionSuccess(_) => { event_type == ClientEventType::ConnectionSuccess }
        ClientEvent::ConnectionFailure(_) => { event_type == ClientEventType::ConnectionFailure }
        ClientEvent::Disconnection(_) => { event_type == ClientEventType::Disconnection }
        ClientEvent::Stopped(_) => { event_type == ClientEventType::Stopped }
        ClientEvent::PublishReceived(_) => { event_type == ClientEventType::PublishReceived }
    }
}

pub(crate) type ClientEventPredicate = dyn Fn(&Arc<ClientEvent>) -> bool + Send + Sync;

pub(crate) struct ClientEventWaiterOptions {
    pub(crate) event_type: ClientEventType,

    pub(crate) event_predicate: Option<Box<ClientEventPredicate>>,
}