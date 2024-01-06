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

// #![warn(missing_docs)]

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

    /// An error where no basic cause could be determined.  Generally an indication of
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
            MqttError::Unknown => { write!(f, "Unknown") }
            MqttError::Unimplemented => { write!(f, "Unimplemented") }
            MqttError::OperationChannelReceiveError => { write!(f, "OperationChannelReceiveError") }
            MqttError::OperationChannelSendError => { write!(f, "OperationChannelSendError") }
            MqttError::VariableLengthIntegerMaximumExceeded => { write!(f, "VariableLengthIntegerMaximumExceeded") }
            MqttError::EncodeBufferTooSmall => { write!(f, "EncodeBufferTooSmall") }
            MqttError::DecoderInvalidVli => { write!(f, "DecoderInvalidVli") }
            MqttError::MalformedPacket => { write!(f, "MalformedPacket") }
            MqttError::ProtocolError => { write!(f, "ProtocolError") }
            MqttError::InboundTopicAliasNotValid => { write!(f, "InboundTopicAliasNotValid") }
            MqttError::UserPropertyValidation => { write!(f, "UserPropertyValidation") }
            MqttError::AuthPacketValidation => { write!(f, "AuthPacketValidation") }
            MqttError::ConnackPacketValidation => { write!(f, "ConnackPacketValidation") }
            MqttError::ConnectPacketValidation => { write!(f, "ConnectPacketValidation") }
            MqttError::DisconnectPacketValidation => { write!(f, "DisconnectPacketValidation") }
            MqttError::PubackPacketValidation => { write!(f, "PubackPacketValidation") }
            MqttError::PubcompPacketValidation => { write!(f, "PubcompPacketValidation") }
            MqttError::PubrecPacketValidation => { write!(f, "PubrecPacketValidation") }
            MqttError::PubrelPacketValidation => { write!(f, "PubrelPacketValidation") }
            MqttError::PublishPacketValidation => { write!(f, "PublishPacketValidation") }
            MqttError::SubackPacketValidation => { write!(f, "SubackPacketValidation") }
            MqttError::UnsubackPacketValidation => { write!(f, "UnsubackPacketValidation") }
            MqttError::SubscribePacketValidation => { write!(f, "SubscribePacketValidation") }
            MqttError::UnsubscribePacketValidation => { write!(f, "UnsubscribePacketValidation") }
            MqttError::InternalStateError => { write!(f, "InternalStateError") }
            MqttError::ConnectionRejected => { write!(f, "ConnectionRejected") }
            MqttError::ConnackTimeout => { write!(f, "ConnackTimeout") }
            MqttError::PingTimeout => { write!(f, "PingTimeout") }
            MqttError::ConnectionClosed => { write!(f, "ConnectionClosed") }
            MqttError::OfflineQueuePolicyFailed => { write!(f, "OfflineQueuePolicyFailed") }
            MqttError::ServerSideDisconnect => { write!(f, "ServerSideDisconnect") }
            MqttError::AckTimeout => { write!(f, "AckTimeout") }
            MqttError::PacketIdSpaceExhausted => { write!(f, "PacketIdSpaceExhausted") }
            MqttError::OperationalStateReset => { write!(f, "OperationalStateReset") }
            MqttError::UserInitiatedDisconnect => { write!(f, "UserInitiatedDisconnect") }
            MqttError::ConnectionTimeout => { write!(f, "ConnectionTimeout") }
            MqttError::ConnectionEstablishmentFailure => { write!(f, "ConnectionEstablishmentFailure") }
            MqttError::StreamWriteFailure => { write!(f, "StreamWriteFailure") }
            MqttError::StreamReadFailure => { write!(f, "StreamReadFailure") }
            MqttError::OperationChannelEmpty => { write!(f, "OperationChannelEmpty") }
            MqttError::IoError => { write!(f, "io error") }
            MqttError::TlsError => { write!(f, "tls error") }
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
