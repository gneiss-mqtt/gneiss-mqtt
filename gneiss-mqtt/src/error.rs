/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::error::Error;
use std::fmt;
use crate::PacketType;

#[derive(Debug)]
pub struct InboundTopicAliasNotValidContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

#[derive(Debug)]
pub struct UnknownContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Basic error type for the entire gneiss-mqtt crate.
#[derive(Debug)]
#[non_exhaustive]
pub enum MqttError {

    /// Functionality was invoked that has not yet been implemented.
    Unimplemented,

    /// A client encountered an error awaiting on its incoming operation channel.  This is usually
    /// due to the user dropping the client which in turn drops the channel sender.
    ///
    /// TODO: if this does not send the client into a terminal state, it should.
    /// TODO: add underlying error as source
    OperationChannelReceiveError,

    /// There was an error attempting to submit an operation to the client via the operation channel.
    /// This usually means the client was closed, which dropped the channel receiver.
    /// TODO: add underlying error as source
    OperationChannelSendError,

    /// Error emitted when packet's encoding would produce a VLI value that exceeds the protocol
    /// maximum (2 ^ 28 - 1)
    VariableLengthIntegerMaximumExceeded,

    /// Error emitted by the client when the encoder buffer does not have capacity to encode at
    /// least 4 bytes.  Should never be seen.
    EncodeBufferTooSmall,

    /// Error emitted when a packet is received with a variable length integer whose encoding
    /// does not conform to the MQTT spec
    DecoderInvalidVli,

    /// Error emitted when an invalid packet encoding is received.  This is distinct from
    /// errors that arise from packets that validate protocol behavior specifications.
    ///
    /// MalformedPacket examples include bad header flags, mismatches between remaining length
    /// fields and overall packet length, etc...
    MalformedPacket,

    /// Generic error emitted when the client encounters broker behavior that violates the MQTT
    /// specification in a way that cannot be safely ignored or recovered from.
    ///
    /// More specific errors exist for a variety of protocol violations.
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

    /// Error emitted by the client when a connection is rejected prior to the Connect <-> Connack
    /// handshake
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

    /// Test Only - an operation's result should have been in the output channel for the
    /// operation and was not
    #[cfg(test)]
    OperationChannelEmpty,

    /// Generic error associated with reading TLS configuration data from the filesystem
    /// TODO: add underlying error as source, make more specific if desc is accurate
    IoError,

    /// Generic error associated with parsing TLS configuration from memory or applying it to a
    /// TLS context
    /// TODO: add underlying error as source
    TlsError
}

impl MqttError {
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
            MqttError::Unimplemented => { write!(f, "attempt to invoke stubbed functionality that has not been completed") }
            MqttError::OperationChannelReceiveError => { write!(f, "operation channel receive error - client operation sender has been dropped") }
            MqttError::OperationChannelSendError => { write!(f, "operation channel send error - client has been closed") }
            MqttError::VariableLengthIntegerMaximumExceeded => { write!(f, "variable length integer maximum exceeded") }
            MqttError::EncodeBufferTooSmall => { write!(f, "encode buffer too small - mqtt encoder requires at least 4 bytes") }
            MqttError::DecoderInvalidVli => { write!(f, "decoder invalid vli - received a packet with an invalid vli encoding") }
            MqttError::MalformedPacket => { write!(f, "malformed packet - received a packet whose encoding properties violate the mqtt spec") }
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
            #[cfg(test)]
            MqttError::OperationChannelEmpty => { write!(f, "operation channel empty - testing encountered a situation where an operation result was expected to be in the output channel and was not") }
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

/// Crate-wide result type for functions that can fail
pub type MqttResult<T> = Result<T, MqttError>;

pub(crate) fn fold_mqtt_result<T>(base: MqttResult<T>, new_result: MqttResult<T>) -> MqttResult<T> {
    new_result?;
    base
}