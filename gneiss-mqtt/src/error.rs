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
pub enum GneissError {

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
    /// results to GneissError.
    OtherError(OtherErrorContext),
}

impl GneissError {

    pub(crate) fn new_unimplemented(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::Unimplemented(
            UnimplementedContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_operation_channel_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::OperationChannelFailure(
            OperationChannelFailureContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_decoding_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::DecodingFailure(
            DecodingFailureContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_encoding_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::EncodingFailure(
            EncodingFailureContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_protocol_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::ProtocolError(
            ProtocolErrorContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_inbound_topic_alias_not_valid(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::InboundTopicAliasNotValid(
            InboundTopicAliasNotValidContext{
                source : source.into()
            }
        )
    }

    pub(crate) fn new_connection_establishment_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::ConnectionEstablishmentFailure(
            ConnectionEstablishmentFailureContext{
                source : source.into()
            }
        )
    }

    pub(crate) fn new_internal_state_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::InternalStateError(
            InternalStateErrorContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_connection_closed(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::ConnectionClosed(
            ConnectionClosedContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_offline_queue_policy_failed() -> Self {
        GneissError::OfflineQueuePolicyFailed(
            OfflineQueuePolicyFailedContext {
            }
        )
    }

    pub(crate) fn new_ack_timeout() -> Self {
        GneissError::AckTimeout(
            AckTimeoutContext {
            }
        )
    }

    pub(crate) fn new_client_closed() -> Self {
        GneissError::ClientClosed(
            ClientClosedContext {
            }
        )
    }

    pub(crate) fn new_user_initiated_disconnect() -> Self {
        GneissError::UserInitiatedDisconnect(
            UserInitiatedDisconnectContext {
            }
        )
    }

    /// Constructs a StdIoError variant from an existing error.  Typically this should be a
    /// std::io::Error
    #[doc(hidden)]
    pub fn new_std_io_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::StdIoError(
            StdIoErrorContext {
                source : source.into()
            }
        )
    }

    /// Constructs a new TlsError variant from an existing error.  Typically this should be
    /// an error surfacing from a third-party TLS library or an attempt to initialize configuration
    /// for one.
    #[doc(hidden)]
    pub fn new_tls_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::TlsError(
            TlsErrorContext {
                source : source.into()
            }
        )
    }

    /// Constructs a new TransportError variant from an existing error.  Typically this should be
    /// an error surfacing from a third-party transport library.
    #[doc(hidden)]
    pub fn new_transport_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::TransportError(
            TransportErrorContext {
                source : source.into()
            }
        )
    }

    pub(crate) fn new_packet_validation(packet_type: PacketType, source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::PacketValidation(
            PacketValidationContext {
                packet_type,
                source : source.into()
            }
        )
    }

    /// Constructs a new OtherError variant from an existing error.  Use this to wrap errors that
    /// do not fall into any appropriate existing category.
    #[doc(hidden)]
    pub fn new_other_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        GneissError::OtherError (
            OtherErrorContext {
                source : source.into()
            }
        )
    }
}

impl Error for GneissError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GneissError::Unimplemented(context) => {
                Some(context.source.as_ref())
            }
            GneissError::OperationChannelFailure(context) => {
                Some(context.source.as_ref())
            }
            GneissError::DecodingFailure(context) => {
                Some(context.source.as_ref())
            }
            GneissError::EncodingFailure(context) => {
                Some(context.source.as_ref())
            }
            GneissError::ProtocolError(context) => {
                Some(context.source.as_ref())
            }
            GneissError::InboundTopicAliasNotValid(context) => {
                Some(context.source.as_ref())
            }
            GneissError::ConnectionEstablishmentFailure(context) => {
                Some(context.source.as_ref())
            }
            GneissError::InternalStateError(context) => {
                Some(context.source.as_ref())
            }
            GneissError::ConnectionClosed(context) => {
                Some(context.source.as_ref())
            }
            GneissError::StdIoError(context) => {
                Some(context.source.as_ref())
            }
            GneissError::TlsError(context) => {
                Some(context.source.as_ref())
            }
            GneissError::TransportError(context) => {
                Some(context.source.as_ref())
            }
            GneissError::PacketValidation(context) => {
                Some(context.source.as_ref())
            }
            GneissError::OtherError(context) => {
                Some(context.source.as_ref())
            }
            _ => { None }
        }
    }
}

impl fmt::Display for GneissError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GneissError::Unimplemented(_) => {
                write!(f, "attempt to invoke stubbed functionality that has not been completed")
            }
            GneissError::OperationChannelFailure(_) => {
                write!(f, "failure encountered while sending/receiving on an MQTT operation-related channel")
            }
            GneissError::DecodingFailure(_) => {
                write!(f, "failure encountered while decoding an incoming MQTT packet")
            }
            GneissError::EncodingFailure(_) => {
                write!(f, "failure encountered while encoding an outbound MQTT packet")
            }
            GneissError::ProtocolError(_) => {
                write!(f, "broker behavior disallowed by the mqtt spec")
            }
            GneissError::InboundTopicAliasNotValid(_) => {
                write!(f, "topic alias value on incoming publish is not valid")
            }
            GneissError::InternalStateError(_) => {
                write!(f, "client reached an invalid internal state; almost certainly a client bug")
            }
            GneissError::ConnectionClosed(_) => {
                write!(f, "client connection was closed; source contains further details")
            }
            GneissError::OfflineQueuePolicyFailed(_) => {
                write!(f, "operation failed due to the offline queue policy and the fact that the client is currently offline")
            }
            GneissError::AckTimeout(_) => {
                write!(f, "the operation's timeout triggered prior to receiving an ack from the broker")
            }
            GneissError::ClientClosed(_) => {
                write!(f, "the operation was incomplete prior to the client being closed")
            }
            GneissError::UserInitiatedDisconnect(_) => {
                write!(f, "connection was shut down by user action")
            }
            GneissError::ConnectionEstablishmentFailure(_) => {
                write!(f, "failed to establish an MQTT connection to the broker")
            }
            GneissError::StdIoError(_) => {
                write!(f, "generic error wrapper for std::io::Error when no more specialized error is appropriate; source contains further details")
            }
            GneissError::TlsError(_) => {
                write!(f, "generic error when setting up a tls context")
            }
            GneissError::TransportError(_) => {
                write!(f, "transport error; source contains further details")
            }
            GneissError::PacketValidation(context) => {
                write!(f, "{} contains a property that violates the mqtt spec", context.packet_type)
            }
            GneissError::OtherError(_) => {
                write!(f, "fallback error type; source contains further details")
            }
        }
    }
}

impl From<std::io::Error> for GneissError {
    fn from(error: std::io::Error) -> Self {
        GneissError::new_std_io_error(error)
    }
}

impl From<core::str::Utf8Error> for GneissError {
    fn from(err: core::str::Utf8Error) -> Self {
        GneissError::new_decoding_failure(err)
    }
}

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
impl From<rustls_pki_types::InvalidDnsNameError> for GneissError {
    fn from(err: rustls_pki_types::InvalidDnsNameError) -> Self {
        GneissError::new_connection_establishment_failure(err)
    }
}

#[cfg(any(feature = "tokio-rustls", feature = "threaded-rustls"))]
impl From<rustls::Error> for GneissError {
    fn from(err: rustls::Error) -> Self {
        GneissError::new_tls_error(err)
    }
}

#[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
impl From<native_tls::Error> for GneissError {
    fn from(err: native_tls::Error) -> Self {
        GneissError::new_tls_error(err)
    }
}

#[cfg(any(feature = "tokio-native-tls", feature = "threaded-native-tls"))]
impl<S> From<native_tls::HandshakeError<S>> for GneissError {
    fn from(_err: native_tls::HandshakeError<S>) -> Self {
        // TODO: is there a better way of handling this?  S is the transport stream which
        // isn't copy/clone so it doesn't seem like we can wrap it
        GneissError::new_tls_error("native-tls handshake error")
    }
}

#[cfg(feature="tokio-websockets")]
impl From<tungstenite::error::Error> for GneissError {
    fn from(err: tungstenite::error::Error) -> Self {
        GneissError::new_transport_error(err)
    }
}

#[cfg(feature="threaded-websockets")]
use std::io::{Read, Write};

#[cfg(feature="threaded-websockets")]
impl <S> From<tungstenite::HandshakeError<tungstenite::ClientHandshake<S>>>  for GneissError
where S : Read + Write {
    fn from(err: tungstenite::HandshakeError<tungstenite::ClientHandshake<S>>) -> Self {
        let message = format!("websocket handshake error: {}", err);
        GneissError::new_transport_error(message)
    }
}

#[cfg(feature="tokio")]
impl From<tokio::sync::oneshot::error::RecvError> for GneissError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        GneissError::new_operation_channel_failure(err)
    }
}

impl <T> From<std::sync::mpsc::SendError<T>> for GneissError
where T : Send + Sync + 'static {
    fn from(err: std::sync::mpsc::SendError<T>) -> Self {
        GneissError::new_operation_channel_failure(err)
    }
}

impl From<std::sync::mpsc::RecvError> for GneissError {
    fn from(err: std::sync::mpsc::RecvError) -> Self {
        GneissError::new_operation_channel_failure(err)
    }
}

impl From<std::sync::mpsc::TryRecvError> for GneissError {
    fn from(err: std::sync::mpsc::TryRecvError) -> Self {
        GneissError::new_operation_channel_failure(err)
    }
}


/// Crate-wide result type for functions that can fail
pub type GneissResult<T> = Result<T, GneissError>;


pub(crate) fn fold_mqtt_result<T>(base: GneissResult<T>, new_result: GneissResult<T>) -> GneissResult<T> {
    new_result?;
    base
}