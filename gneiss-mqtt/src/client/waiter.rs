/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Primarily-debug functionality used throughout the gneiss ecosystem to wait for specific client event conditions.
Typically used in two scenarios:
  1. Tests - dramatically simplifies test coordination and control flow
  2. Examples - helps make control flow linear and readable

Using waiters in a real application is discouraged.  It's rare for it to be a good idea to
funnel async application control flow through single event wait points.
*/

use crate::client::*;

use std::sync::Arc;
use std::time::Instant;

/// Simple C-style enum whose entries match the ClientEvent enum.  Useful for coarse matching against event types when we don't
/// need to dig into the variant's internal event data.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[non_exhaustive]
pub enum ClientEventType {

    /// Corresponds to a ClientEvent::ConnectionAttempt, the client event emitted every time the client tries
    /// to establish a connection
    ConnectionAttempt,

    /// Corresponds to a ClientEvent::ConnectionSuccess, the client event emitted every time the client successfully
    /// connects to an MQTT broker (received a successful CONNACK)
    ConnectionSuccess,

    /// Corresponds to a ClientEvent::ConnectionFailure, the client event emitted every time a client's connection
    /// attempt results in some kind of failure
    ConnectionFailure,

    /// Corresponds to a ClientEvent::Disconnection, the client event emitted every time a successfully connected
    /// client has its connection closed for any reason
    Disconnection,

    /// Corresponds to a ClientEvent::Stopped, the client event emitted every time a previously running client
    /// settles into the Stopped state by user request.
    Stopped,

    /// Corresponds to a ClientEvent::PublishReceived, the client event emitted every time a Publish packet
    /// is received
    PublishReceived,
}

/// Checks if a ClientEvent matches a ClientEventType
pub fn client_event_matches(event: &Arc<ClientEvent>, event_type: ClientEventType) -> bool {
    match **event {
        ClientEvent::ConnectionAttempt(_) => { event_type == ClientEventType::ConnectionAttempt }
        ClientEvent::ConnectionSuccess(_) => { event_type == ClientEventType::ConnectionSuccess }
        ClientEvent::ConnectionFailure(_) => { event_type == ClientEventType::ConnectionFailure }
        ClientEvent::Disconnection(_) => { event_type == ClientEventType::Disconnection }
        ClientEvent::Stopped(_) => { event_type == ClientEventType::Stopped }
        ClientEvent::PublishReceived(_) => { event_type == ClientEventType::PublishReceived }
    }
}

/// Filter function type used to create complex waiters.
///
/// Only events that pass the filter check will be
/// passed on to the user at the conclusion of the wait() call.
pub type ClientEventPredicate = dyn Fn(&Arc<ClientEvent>) -> bool + Send + Sync;

/// Enum controlling how the waiter should filter client events.
pub enum ClientEventWaitType {

    /// Filter all client events that do not match this variant's type
    Type(ClientEventType),

    /// Filter all client events that the predicate filter function returns false for
    Predicate(Box<ClientEventPredicate>)
}

/// Configuration options for waiter creation
pub struct ClientEventWaiterOptions {

    /// How the waiter should filter client events
    pub wait_type: ClientEventWaitType,
}

/// Timestamped client event record
#[derive(Clone)]
pub struct ClientEventRecord {

    /// The event emitted by the client
    pub event : Arc<ClientEvent>,

    /// What time the event occurred at
    pub timestamp: Instant
}

#[cfg(feature="tokio")]
pub use crate::client::asynchronous::tokio::{ClientEventWaitFuture, TokioClientEventWaiter};

#[cfg(feature="threaded")]
pub use crate::client::synchronous::threaded::{ThreadedClientEventWaiter};