/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
This crate provides clients for communicating with a message broker using the MQTT5 protocol.

MQTT is a publish/subscribe protocol commonly used in IoT use cases.  MQTT5 is the latest
version of the protocol.  The previous version, MQTT311, has also seen widespread adoption
and will eventually be supported in an upcoming release.

# Usage

# Example: Connect to a local Mosquitto server

# Example: React to client events

# Additional Notes

The intention is that this crate will eventually be as agnostic as possible of underlying
implementation details (async runtimes, TLS/transport implementations, etc...)
but at present it has hard dependencies on tokio, rustls, and some
associated helper libraries.  These will get feature-flag-gated before GA, allowing the user
to pare the implementation down to their exact connection needs.  In Rust's current state,
there is a fundamental tension between trying to be transport/runtime agnostic and trying
to provide an easy-to-use interface for getting successful clients set up for the many
different combinations expected by users.

This crate's public API is expected to be very unstable until v0.5.0.  See the roadmap
in the README for more details.
*/

#![warn(missing_docs)]

pub mod alias;
pub mod client;
pub mod config;
mod decode;
mod encode;
pub mod features;
mod logging;
mod operation;
pub mod spec;
mod validate;

/* Re-export all spec types at the root level */
pub use spec::QualityOfService;
pub use spec::PayloadFormatIndicator;
pub use spec::RetainHandlingType;
pub use spec::ConnectReasonCode;
pub use spec::PubackReasonCode;
pub use spec::PubrecReasonCode;
pub use spec::PubrelReasonCode;
pub use spec::PubcompReasonCode;
pub use spec::DisconnectReasonCode;
pub use spec::SubackReasonCode;
pub use spec::UnsubackReasonCode;
pub use spec::AuthenticateReasonCode;
pub use spec::UserProperty;
pub use spec::Subscription;

pub use spec::auth::AuthPacket;
pub use spec::connack::ConnackPacket;
pub use spec::connect::ConnectPacket;
pub use spec::disconnect::DisconnectPacket;
pub use spec::pingreq::PingreqPacket;
pub use spec::pingresp::PingrespPacket;
pub use spec::puback::PubackPacket;
pub use spec::pubcomp::PubcompPacket;
pub use spec::publish::PublishPacket;
pub use spec::pubrec::PubrecPacket;
pub use spec::pubrel::PubrelPacket;
pub use spec::suback::SubackPacket;
pub use spec::subscribe::SubscribePacket;
pub use spec::unsuback::UnsubackPacket;
pub use spec::unsubscribe::UnsubscribePacket;
pub use spec::utils::{
    convert_u8_to_disconnect_reason_code,
    convert_u8_to_quality_of_service,
};

use std::error::Error;
use std::fmt;
use std::time::Instant;

/// Basic error type for the entire gneiss-mqtt crate.  Currently a very simple enum with no
/// additional context.  May eventually take on additional state in certain variants, but for now
/// I don't feel comfortable committing to that.
///
/// In the meantime, configure logging for additional details recorded at the time of the error
/// emission.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MqttError {

    /// An error where no root cause could be determined.  Generally an indication of
    /// a bug in an interior system.
    Unknown,

    /// Functionality was invoked that has not yet been implemented.  Should never be seen
    /// post-GA.
    Unimplemented,

    /// A client encountered an error awaiting on its incoming operation channel.  This is usually
    /// due to the user dropping the client which in turn drops the channel sender.
    ///
    /// TODO: if this does not send the client into a terminal state, it should.
    OperationChannelReceiveError,

    /// There was an error attempting to submit an operation to the client via the operation channel.
    /// This usually means the client was closed, which dropped the channel receiver.
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
    InboundTopicAliasNotValid,

    /// Error emitted when a (user-submitted) packet is encountered whose user properties violate
    /// the MQTT specification.  Generally this is due to a maximum length violation.
    UserPropertyValidation,

    /// Error emitted when an Auth packet is submitted or received that violates the MQTT
    /// specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    AuthPacketValidation,

    /// Error emitted when a Connack packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    ConnackPacketValidation,

    /// Error emitted when a Connect packet is constructed on behalf of the user that violates the MQTT
    /// specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    ConnectPacketValidation,

    /// Error emitted when a Disconnect packet is submitted or received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    DisconnectPacketValidation,

    /// Error emitted when a Puback packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    PubackPacketValidation,

    /// Error emitted when a Pubcomp packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    PubcompPacketValidation,

    /// Error emitted when a Pubrec packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    PubrecPacketValidation,

    /// Error emitted when a Pubrel packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    PubrelPacketValidation,

    /// Error emitted when a Publish packet is submitted or received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    PublishPacketValidation,

    /// Error emitted when a Suback packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    SubackPacketValidation,

    /// Error emitted when an Unsuback packet is received that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    UnsubackPacketValidation,

    /// Error emitted when a Subscribe packet is submitted that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    SubscribePacketValidation,

    /// Error emitted when an Unsubscribe packet is submitted that violates the MQTT specification.
    ///
    /// TODO: consider consolidating into a PacketValidation variant that includes the packet type as a parameter
    UnsubscribePacketValidation,

    /// Error emitted by the client when something happens that should never happen.  Always indicates
    /// a bug in the client.
    InternalStateError,

    /// Error emitted when the broker explicitly rejects a connection attempt by sending a Connack
    /// packet with a failing reason code.  The connect failure client event will contain the
    /// full Connack packet and can be inspected for further diagnostics.
    ConnectionRejected,

    /// Error emitted by the client when the broker does not respond to a Connect packet within
    /// the configured timeout interval.
    ConnackTimeout,

    /// Error emitted when the client shuts down a connection due to the broker not responding to
    /// a Pingreq packet.  Generally indicates that connectivity between the client and broker is
    /// broken.
    PingTimeout,

    /// Error emitted when the client's connection gets closed for some external reason.  Usually
    /// this is the broker hanging up on the client, but intermediary network failures may trigger
    /// this as well.
    ConnectionClosed,

    /// Error applied to MQTT operaations that are failed because the client is offline and the
    /// configured offline policy rejects the operation.
    OfflineQueuePolicyFailed,

    /// Error emitted when the broker sends a Disconnect packet to indicate the connection is
    /// being shut down.
    ServerSideDisconnect,

    /// Error applied to user-submitted operations that indicates the operation failed because
    /// we did not receive an Ack packet within the operation's timeout interval.
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
    ConnectionTimeout,

    /// Error emitted by the client when a connection is rejected prior to the Connect <-> Connack
    /// handshake
    ConnectionEstablishmentFailure,

    /// Error emitted by the client when it fails to write data to the socket.  This is a
    /// connection-fatal event; it does not represent socket-buffer-full.
    StreamWriteFailure,

    /// Error emitted by the client when it fails to read data from the socket.  This is a
    /// connection-fatal event; it does not represent no-data-ready.
    StreamReadFailure,

    /// Umm, not sure how this is test-only
    OperationChannelEmpty,

    /// Generic error associated with reading TLS configuration data from the filesystem
    IoError,

    /// Generic error associated with parsing TLS configuration from memory or applying it to a
    /// TLS context
    TlsError
}

impl Error for MqttError {
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttError::Unknown => { write!(f, "unknown - no root cause could be determined") }
            MqttError::Unimplemented => { write!(f, "unimplemented - attempt to invoke functionality that has not been completed") }
            MqttError::OperationChannelReceiveError => { write!(f, "operation channel receive error - client operation sender has been dropped") }
            MqttError::OperationChannelSendError => { write!(f, "operation channel send error - client has been closed") }
            MqttError::VariableLengthIntegerMaximumExceeded => { write!(f, "variable length integer maximum exceeded") }
            MqttError::EncodeBufferTooSmall => { write!(f, "encode buffer too small - mqtt encoder requires at least 4 bytes") }
            MqttError::DecoderInvalidVli => { write!(f, "decoder invalid vli - received a packet with an invalid vli encoding") }
            MqttError::MalformedPacket => { write!(f, "malformed packet - received a packet whose encoding properties violate the mqtt spec") }
            MqttError::ProtocolError => { write!(f, "protocol error - broker behavior disallowed by the mqtt spec") }
            MqttError::InboundTopicAliasNotValid => { write!(f, "inbound topic alias not valid - incoming publish contained an invalid topic alias") }
            MqttError::UserPropertyValidation => { write!(f, "user property validation - mqtt packet contains user properties that violate the mqtt spec") }
            MqttError::AuthPacketValidation => { write!(f, "auth packet validation - an auth packet failed validation") }
            MqttError::ConnackPacketValidation => { write!(f, "connack packet validation - a connack packet failed validation") }
            MqttError::ConnectPacketValidation => { write!(f, "connect packet validation - a connect packet failed validation") }
            MqttError::DisconnectPacketValidation => { write!(f, "disconnect packet validation - a disconnect packet failed validation") }
            MqttError::PubackPacketValidation => { write!(f, "puback packet validation - a puback packet failed validation") }
            MqttError::PubcompPacketValidation => { write!(f, "pubcomp packet validation - a pubcomp packet failed validation") }
            MqttError::PubrecPacketValidation => { write!(f, "pubrec packet validation - a pubrec packet failed validation") }
            MqttError::PubrelPacketValidation => { write!(f, "pubrel packet validation - a pubrel packet failed validation") }
            MqttError::PublishPacketValidation => { write!(f, "publish packet validation - a publish packet failed validation") }
            MqttError::SubackPacketValidation => { write!(f, "suback packet validation - a suback packet failed validation") }
            MqttError::UnsubackPacketValidation => { write!(f, "unsuback packet validation - an unsuback packet failed validation") }
            MqttError::SubscribePacketValidation => { write!(f, "subscribe packet validation - a subscribe packet failed validation") }
            MqttError::UnsubscribePacketValidation => { write!(f, "unsubscribe packet validation - an unsubscribe packet failed validation") }
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
            MqttError::OperationChannelEmpty => { write!(f, "operation channel empty - ??") }
            MqttError::IoError => { write!(f, "io error - generic error due to an error operating on the connection's network stream") }
            MqttError::TlsError => { write!(f, "tls error - generic error when setting up a tls context") }
        }
    }
}

impl From<&MqttError> for MqttError {
    fn from(value: &MqttError) -> Self {
        *value
    }
}

impl From<std::io::Error> for MqttError {
    fn from(_: std::io::Error) -> Self {
        MqttError::IoError
    }
}

/// Crate-wide result type for functions that can fail
pub type MqttResult<T> = Result<T, MqttError>;

fn fold_mqtt_result<T>(base: MqttResult<T>, new_result: MqttResult<T>) -> MqttResult<T> {
    new_result.as_ref()?;
    base
}

fn fold_timepoint(base: &Option<Instant>, new: &Instant) -> Option<Instant> {
    if let Some(base_timepoint) = &base {
        if base_timepoint < new {
            return *base;
        }
    }

    Some(*new)
}

fn fold_optional_timepoint_min(base: &Option<Instant>, new: &Option<Instant>) -> Option<Instant> {
    if let Some(base_timepoint) = base {
        if let Some(new_timepoint) = new {
            if base_timepoint < new_timepoint {
                return *base;
            } else {
                return *new;
            }
        }

        return *base;
    }

    *new
}
