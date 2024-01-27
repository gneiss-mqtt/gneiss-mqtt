/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
A module containing the core crate error enumeration, context structures, and conversion
definitions.
 */

use crate::mqtt::PacketType;

use std::error::Error;
use std::fmt;

/// Additional details about an Unimplemented error variant
#[derive(Debug)]
pub struct UnimplementedContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about an OperationChannelFailure error variant
#[derive(Debug)]
pub struct OperationChannelFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about an EncodingFailure error variant
#[derive(Debug)]
pub struct EncodingFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a DecodingFailure error variant
#[derive(Debug)]
pub struct DecodingFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a ProtocolError error variant
#[derive(Debug)]
pub struct ProtocolErrorContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about an InboundTopicAliasNotValid error variant
#[derive(Debug)]
pub struct InboundTopicAliasNotValidContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a ConnectionEstablishmentFailure error variant
#[derive(Debug)]
pub struct ConnectionEstablishmentFailureContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about an InternalStateError error variant
#[derive(Debug)]
pub struct InternalStateErrorContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a ConnectionClosed error variant
#[derive(Debug)]
pub struct ConnectionClosedContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about an OfflineQueuePolicyFailed error variant
#[derive(Debug)]
pub struct OfflineQueuePolicyFailedContext {
}

/// Additional details about an AckTimeout error variant
#[derive(Debug)]
pub struct AckTimeoutContext {
}

/// Additional details about a ClientClosed error variant
#[derive(Debug)]
pub struct ClientClosedContext {
}

/// Additional details about a UserInitiatedDisconnection error variant
#[derive(Debug)]
pub struct UserInitiatedDisconnectContext {
}

/// Additional details about a StdIoError error variant
#[derive(Debug)]
pub struct StdIoErrorContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a TlsError error variant
#[derive(Debug)]
pub struct TlsErrorContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a TransportError error variant
#[derive(Debug)]
pub struct TransportErrorContext {
    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about a PacketValidation error variant
#[derive(Debug)]
pub struct PacketValidationContext {

    /// type of packet that failed validation
    pub packet_type: PacketType,

    source: Box<dyn Error + Send + Sync + 'static>
}

/// Additional details about an OtherError error variant
#[derive(Debug)]
pub struct OtherErrorContext {
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
    ProtocolError(ProtocolErrorContext),

    /// Error emitted when an inbound publish arrives with an unknown topic alias.
    InboundTopicAliasNotValid(InboundTopicAliasNotValidContext),

    /// Error emitted by the client when something happens that should never happen.  Always indicates
    /// a bug in the client.
    InternalStateError(InternalStateErrorContext),

    /// Error emitted when a successfully established connection subsequently gets closed for
    /// some reason.  This general error may be superseded by a more specific error (user initiated
    /// disconnect) under certain conditions.
    ConnectionClosed(ConnectionClosedContext),

    /// Error applied to MQTT operaations that are failed because the client is offline and the
    /// configured offline policy rejects the operation.
    OfflineQueuePolicyFailed(OfflineQueuePolicyFailedContext),

    /// Error applied to user-submitted operations that indicates the operation failed because
    /// we did not receive an Ack packet within the operation's timeout interval.
    AckTimeout(AckTimeoutContext),

    /// Error applied to all unfinished client operations when the client is closed by the user.
    ClientClosed(ClientClosedContext),

    /// Error emitted by the client after sending a user-submitted Disconnect packet as a part
    /// of a `stop()` invocation.  Does not indicate an actual failure.
    UserInitiatedDisconnect(UserInitiatedDisconnectContext),

    /// Error emitted by the client when a connection attempt fails.  Failure is defined as
    /// "the attempt is finished for any reason prior to receipt of a successful Connack packet."
    ConnectionEstablishmentFailure(ConnectionEstablishmentFailureContext),

    /// Generic error wrapping std::io::Error
    StdIoError(StdIoErrorContext),

    /// Generic error associated with parsing TLS configuration from memory or applying it to a
    /// TLS context
    TlsError(TlsErrorContext),

    /// Generic error associated with feature-selected transport options.  For now, this mostly
    /// wraps websocket implementation specific errors
    TransportError(TransportErrorContext),

    /// Error emitted when an Auth packet is submitted or received that violates the MQTT
    /// specification.
    PacketValidation(PacketValidationContext),

    /// Error to be used when no other error variant is appropriate.  Generally used by
    /// auxiliary crates whose error category doesn't match anything but that want to restrict
    /// results to MqttError.
    OtherError(OtherErrorContext),
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

    pub(crate) fn new_protocol_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::ProtocolError(
            ProtocolErrorContext {
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

    pub(crate) fn new_connection_establishment_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::ConnectionEstablishmentFailure(
            ConnectionEstablishmentFailureContext{
                source : source.into()
            }
        )
    }

    pub(crate) fn new_internal_state_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::InternalStateError(
            InternalStateErrorContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_connection_closed(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::ConnectionClosed(
            ConnectionClosedContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_offline_queue_policy_failed() -> Self {
        MqttError::OfflineQueuePolicyFailed(
            OfflineQueuePolicyFailedContext {
            }
        )
    }

    pub(crate) fn new_ack_timeout() -> Self {
        MqttError::AckTimeout(
            AckTimeoutContext {
            }
        )
    }

    pub(crate) fn new_client_closed() -> Self {
        MqttError::ClientClosed(
            ClientClosedContext {
            }
        )
    }

    pub(crate) fn new_user_initiated_disconnect() -> Self {
        MqttError::UserInitiatedDisconnect(
            UserInitiatedDisconnectContext {
            }
        )
    }

    /// Constructs a StdIoError variant from an existing error.  Typically this should be a
    /// std::io::Error
    pub fn new_std_io_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::StdIoError(
            StdIoErrorContext {
                source : source.into()
            }
        )
    }

    /// Constructs a new TlsError variant from an existing error.  Typically this should be
    /// an error surfacing from a third-party TLS library or an attempt to initialize configuration
    /// for one.
    pub fn new_tls_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::TlsError(
            TlsErrorContext {
                source : source.into()
            }
        )
    }

    /// Constructs a new TransportError variant from an existing error.  Typically this should be
    /// an error surfacing from a third-party transport library.
    pub fn new_transport_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::TransportError(
            TransportErrorContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_packet_validation(packet_type: PacketType, source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::PacketValidation(
            PacketValidationContext {
                packet_type,
                source : source.into()
            }
        )
    }

    /// Constructs a new OtherError variant from an existing error.  Use this to wrap errors that
    /// do not fall into any appropriate existing category.
    pub fn new_other_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        MqttError::OtherError (
            OtherErrorContext {
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
            MqttError::ProtocolError(context) => {
                Some(context.source.as_ref())
            }
            MqttError::InboundTopicAliasNotValid(context) => {
                Some(context.source.as_ref())
            }
            MqttError::ConnectionEstablishmentFailure(context) => {
                Some(context.source.as_ref())
            }
            MqttError::InternalStateError(context) => {
                Some(context.source.as_ref())
            }
            MqttError::ConnectionClosed(context) => {
                Some(context.source.as_ref())
            }
            MqttError::StdIoError(context) => {
                Some(context.source.as_ref())
            }
            MqttError::TlsError(context) => {
                Some(context.source.as_ref())
            }
            MqttError::TransportError(context) => {
                Some(context.source.as_ref())
            }
            MqttError::PacketValidation(context) => {
                Some(context.source.as_ref())
            }
            MqttError::OtherError(context) => {
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
            MqttError::ProtocolError(_) => {
                write!(f, "broker behavior disallowed by the mqtt spec")
            }
            MqttError::InboundTopicAliasNotValid(_) => {
                write!(f, "topic alias value on incoming publish is not valid")
            }
            MqttError::InternalStateError(_) => {
                write!(f, "client reached an invalid internal state; almost certainly a client bug")
            }
            MqttError::ConnectionClosed(_) => {
                write!(f, "client connection was closed; source contains further details")
            }
            MqttError::OfflineQueuePolicyFailed(_) => {
                write!(f, "operation failed due to the offline queue policy and the fact that the client is currently offline")
            }
            MqttError::AckTimeout(_) => {
                write!(f, "the operation's timeout triggered prior to receiving an ack from the broker")
            }
            MqttError::ClientClosed(_) => {
                write!(f, "the operation was incomplete prior to the client being closed")
            }
            MqttError::UserInitiatedDisconnect(_) => {
                write!(f, "connection was shut down by user action")
            }
            MqttError::ConnectionEstablishmentFailure(_) => {
                write!(f, "failed to establish an MQTT connection to the broker")
            }
            MqttError::StdIoError(_) => {
                write!(f, "generic error wrapper for std::io::Error when no more specialized error is appropriate; source contains further details")
            }
            MqttError::TlsError(_) => {
                write!(f, "generic error when setting up a tls context")
            }
            MqttError::TransportError(_) => {
                write!(f, "transport error; source contains further details")
            }
            MqttError::PacketValidation(context) => {
                write!(f, "{} contains a property that violates the mqtt spec", context.packet_type)
            }
            MqttError::OtherError(_) => {
                write!(f, "fallback error type; source contains further details")
            }
        }
    }
}

impl From<std::io::Error> for MqttError {
    fn from(error: std::io::Error) -> Self {
        MqttError::new_std_io_error(error)
    }
}

impl From<core::str::Utf8Error> for MqttError {
    fn from(err: core::str::Utf8Error) -> Self {
        MqttError::new_decoding_failure(err)
    }
}

impl From<rustls_pki_types::InvalidDnsNameError> for MqttError {
    fn from(err: rustls_pki_types::InvalidDnsNameError) -> Self {
        MqttError::new_connection_establishment_failure(err)
    }
}

impl From<tungstenite::error::Error> for MqttError {
    fn from(err: tungstenite::error::Error) -> Self {
        MqttError::new_transport_error(err)
    }
}

/// Crate-wide result type for functions that can fail
pub type MqttResult<T> = Result<T, MqttError>;


pub(crate) fn fold_mqtt_result<T>(base: MqttResult<T>, new_result: MqttResult<T>) -> MqttResult<T> {
    new_result?;
    base
}