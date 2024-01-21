/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::error::Error;
use std::fmt;
use crate::PacketType;


#[derive(Debug)]
pub struct UnimplementedContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

#[derive(Debug)]
pub struct OperationChannelFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

#[derive(Debug)]
pub struct InboundTopicAliasNotValidContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

#[derive(Debug)]
pub struct EncodingFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

#[derive(Debug)]
pub struct DecodingFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Basic error type for the entire gneiss-mqtt crate.
#[derive(Debug)]
#[non_exhaustive]
pub enum MqttError {

    /// Functionality was invoked that has not yet been implemented.
    Unimplemented(UnimplementedContext),

    /// Failure encountered while using MQTT operation channel functionality
    OperationChannelFailure(OperationChannelFailureContext),

    /// Error encountered while attempting to encode an MQTT packet
    EncodingFailure(EncodingFailureContext),

    /// Error encountered while attempting to decode an MQTT packet.  This is distinct from
    /// errors that arise from packets that validate protocol behavior specifications.
    /// Examples include bad header flags, mismatches between remaining length
    /// fields and overall packet length, etc...
    DecodingFailure(DecodingFailureContext),

    /// Generic error emitted when the client encounters broker behavior that violates the MQTT
    /// specification in a way that cannot be safely ignored or recovered from.
    ProtocolError,

    /// Error emitted when an inbound publish arrives with an unknown topic alias.
    InboundTopicAliasNotValid(InboundTopicAliasNotValidContext),

    /// Error emitted when an Auth packet is submitted or received that violates the MQTT
    /// specification.
    PacketValidation(PacketType),

    /// Error emitted by the client when something happens that should never happen.  Always indicates
    /// a bug in the client.
    InternalStateError,

    /// Error emitted when the broker explicitly rejects a connection attempt by sending a Connack
    /// packet with a failing reason code.  The connect failure client event will contain the
    /// full Connack packet and can be inspected for further diagnostics.
    ConnectionRejected,

    /// Error emitted by the client when the broker does not respond to a Connect packet within
    /// the configured timeout interval.
    /// TODO: Add duration?
    ConnackTimeout,

    /// Error emitted when the client shuts down a connection due to the broker not responding to
    /// a Pingreq packet.  Generally indicates that connectivity between the client and broker is
    /// broken.
    /// TODO: Add duration?
    PingTimeout,

    /// Error emitted when the client's connection gets closed for some external reason.  Usually
    /// this is the broker hanging up on the client, but intermediary network failures may trigger
    /// this as well.
    /// TODO: applied to multiple, cannot add context
    ConnectionClosed,

    /// Error applied to MQTT operaations that are failed because the client is offline and the
    /// configured offline policy rejects the operation.
    /// TODO: applied to multiple, cannot add context
    OfflineQueuePolicyFailed,

    /// Error emitted when the broker sends a Disconnect packet to indicate the connection is
    /// being shut down.
    ServerSideDisconnect,

    /// Error applied to user-submitted operations that indicates the operation failed because
    /// we did not receive an Ack packet within the operation's timeout interval.
    /// TODO: Add duration?
    AckTimeout,

    /// Error indicating no more packet ids are available for outbound packets.  Should never
    /// happen; indicates a bad client bug.
    PacketIdSpaceExhausted,

    /// Error applied to all unfinished client operations when the client is closed by the user.
    OperationalStateReset,

    /// Error emitted by the client after sending a user-submitted Disconnect packet as a part
    /// of a `stop()` invocation.  Does not indicate an actual failure.
    UserInitiatedDisconnect,

    /// Error emitted by the client when a connection attempt (the interval between starting
    /// the connection and it being ready for an MQTT Connect packet) times out
    /// TODO: Add duration?
    ConnectionTimeout,

    /// Error emitted by the client when a connection attempt fails
    /// TODO: add underlying error as source
    ConnectionEstablishmentFailure,

    /// Error emitted by the client when it fails to write data to the socket.  This is a
    /// connection-fatal event; it does not represent socket-buffer-full.
    /// TODO: add underlying error as source
    StreamWriteFailure,

    /// Error emitted by the client when it fails to read data from the socket.  This is a
    /// connection-fatal event; it does not represent no-data-ready.
    /// TODO: add underlying error as source
    StreamReadFailure,

    /// Generic error associated with reading TLS configuration data from the filesystem
    /// TODO: add underlying error as source, make more specific if desc is accurate
    IoError,

    /// Generic error associated with parsing TLS configuration from memory or applying it to a
    /// TLS context
    /// TODO: add underlying error as source
    TlsError
}

impl MqttError {

    pub(crate) fn new_unimplemented(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::Unimplemented(
            UnimplementedContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_operation_channel_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::OperationChannelFailure(
            OperationChannelFailureContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_decoding_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::DecodingFailure(
            DecodingFailureContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_encoding_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::EncodingFailure(
            EncodingFailureContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_inbound_topic_alias_not_valid(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::InboundTopicAliasNotValid(
            InboundTopicAliasNotValidContext{
                source : source.into()
            }
        )
    }
}

impl Error for MqttError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            MqttError::Unimplemented(context) => {
                Some(context.source.as_ref())
            }
            MqttError::OperationChannelFailure(context) => {
                Some(context.source.as_ref())
            }
            MqttError::DecodingFailure(context) => {
                Some(context.source.as_ref())
            }
            MqttError::EncodingFailure(context) => {
                Some(context.source.as_ref())
            }
            MqttError::InboundTopicAliasNotValid(context) => {
                Some(context.source.as_ref())
            }
            _ => { None }
        }
    }
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttError::Unimplemented(_) => {
                write!(f, "attempt to invoke stubbed functionality that has not been completed")
            }
            MqttError::OperationChannelFailure(_) => {
                write!(f, "failure encountered while sending/receiving on an MQTT operation-related channel")
            }
            MqttError::DecodingFailure(_) => {
                write!(f, "failure encountered while decoding an incoming MQTT packet")
            }
            MqttError::EncodingFailure(_) => {
                write!(f, "failure encountered while encoding an outbound MQTT packet")
            }

            MqttError::ProtocolError => { write!(f, "protocol error - broker behavior disallowed by the mqtt spec") }
            MqttError::InboundTopicAliasNotValid(_) => {
                write!(f, "topic alias value on incoming publish is not valid")
            }
            MqttError::PacketValidation(packet_type) => { write!(f, "{} contains a property that violates the mqtt spec", packet_type) }
            MqttError::InternalStateError => { write!(f, "internal state error - client reached an invalid internal state, almost certainly a client bug") }
            MqttError::ConnectionRejected => { write!(f, "connack rejected - the broker explicitly rejected the connect packet") }
            MqttError::ConnackTimeout => { write!(f, "connack timeout - the broker did not respond in time to the connect packet") }
            MqttError::PingTimeout => { write!(f, "ping timeout - the broker did not response in time to the pingreq packet; likely the connection was unhealthy") }
            MqttError::ConnectionClosed => { write!(f, "connection closed - the client's connection was closed due to some external reason") }
            MqttError::OfflineQueuePolicyFailed => { write!(f, "offline queue policy failed - operation failed due to the offline queue policy and the fact that the client is currently offline") }
            MqttError::ServerSideDisconnect => { write!(f, "server side disconnect - connection was shut down after receiving a disconnect packet from the broker") }
            MqttError::AckTimeout => { write!(f, "ack timeout - the operation's timeout triggered prior to receiving an ack from the broker") }
            MqttError::PacketIdSpaceExhausted => { write!(f, "packet id space exhausted - no packet ids remain; should never happen") }
            MqttError::OperationalStateReset => { write!(f, "operational state reset - the operation was not complete prior to the client being closed") }
            MqttError::UserInitiatedDisconnect => { write!(f, "user-initiated disconnect - connection was shut down by user action") }
            MqttError::ConnectionTimeout => { write!(f, "connection timeout - a transport-level connection to the broker could not be established before timeout") }
            MqttError::ConnectionEstablishmentFailure => { write!(f, "connection establishment failure - failure to establish a transport-level connection to the broker") }
            MqttError::StreamWriteFailure => { write!(f, "stream write failure - error attempting to write or flush a connection stream") }
            MqttError::StreamReadFailure => { write!(f, "stream read failure - error when attempting to read a connection stream") }
            MqttError::IoError => { write!(f, "io error - generic error due to an error operating on the connection's network stream") }
            MqttError::TlsError => { write!(f, "tls error - generic error when setting up a tls context") }
        }
    }
}

impl From<std::io::Error> for MqttError {
    fn from(_: std::io::Error) -> Self {
        MqttError::IoError
    }
}

impl From<core::str::Utf8Error> for MqttError {
    fn from(err: core::str::Utf8Error) -> Self {
        MqttError::new_decoding_failure(err)
    }
}

/// Crate-wide result type for functions that can fail
pub type MqttResult<T> = Result<T, MqttError>;

pub(crate) fn fold_mqtt_result<T>(base: MqttResult<T>, new_result: MqttResult<T>) -> MqttResult<T> {
    new_result?;
    base
}