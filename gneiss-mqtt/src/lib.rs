/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

pub mod alias;
pub mod client;
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
    convert_u8_to_authenticate_reason_code,
    convert_u8_to_quality_of_service,
    convert_u8_to_retain_handling_type
};

pub use client::*;

use std::error::Error;
use std::fmt;
use std::time::Instant;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MqttError {
    Unknown,
    Unimplemented,
    OperationChannelReceiveError,
    OperationChannelSendError,
    VariableLengthIntegerMaximumExceeded,
    EncodeBufferTooSmall,
    DecoderInvalidVli,
    MalformedPacket,
    ProtocolError,
    InboundTopicAliasNotAllowed,
    InboundTopicAliasNotValid,
    OutboundTopicAliasNotAllowed,
    OutboundTopicAliasInvalid,
    UserPropertyValidation,
    AuthPacketValidation,
    ConnackPacketValidation,
    ConnectPacketValidation,
    DisconnectPacketValidation,
    PubackPacketValidation,
    PubcompPacketValidation,
    PubrecPacketValidation,
    PubrelPacketValidation,
    PublishPacketValidation,
    SubackPacketValidation,
    UnsubackPacketValidation,
    SubscribePacketValidation,
    UnsubscribePacketValidation,
    InternalStateError,
    ConnectionRejected,
    ConnackTimeout,
    PingTimeout,
    ConnectionClosed,
    OfflineQueuePolicyFailed,
    ServerSideDisconnect,
    AckTimeout,
    PacketIdSpaceExhausted,
    OperationalStateReset,
    UserInitiatedDisconnect,
    ClientClosed,
    ConnectionTimeout,
    UserRequestedStop,
    ConnectionEstablishmentFailure,
    StreamWriteFailure,
    StreamReadFailure,
    OperationChannelEmpty,
    IoError,
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
            MqttError::InboundTopicAliasNotAllowed => { write!(f, "InboundTopicAliasNotAllowed") }
            MqttError::InboundTopicAliasNotValid => { write!(f, "InboundTopicAliasNotValid") }
            MqttError::OutboundTopicAliasNotAllowed => { write!(f, "OutboundTopicAliasNotAllowed") }
            MqttError::OutboundTopicAliasInvalid => { write!(f, "OutboundTopicAliasInvalid") }
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
            MqttError::ClientClosed => { write!(f, "ClientClosed") }
            MqttError::ConnectionTimeout => { write!(f, "ConnectionTimeout") }
            MqttError::UserRequestedStop => { write!(f, "UserRequestedStop") }
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
