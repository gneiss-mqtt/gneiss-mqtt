/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing a set of structured data types that model the MQTT specification.
 */

use std::fmt;
use log::error;
use crate::client::config::ProtocolMode;
use crate::error::{GneissError, GneissResult};

pub(crate) mod auth;
pub(crate) mod connack;
pub(crate) mod connect;
pub(crate) mod disconnect;
pub(crate) mod pingreq;
pub(crate) mod pingresp;
pub(crate) mod puback;
pub(crate) mod pubcomp;
pub(crate) mod publish;
pub(crate) mod pubrec;
pub(crate) mod pubrel;
pub(crate) mod suback;
pub(crate) mod subscribe;
pub(crate) mod unsuback;
pub(crate) mod unsubscribe;
pub(crate) mod utils;

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(crate) enum ProtocolVersion {
    #[default]
    Mqtt5,

    Mqtt311
}

pub(crate) fn convert_protocol_mode_to_protocol_version(mode: ProtocolMode) -> ProtocolVersion {
    match mode {
        ProtocolMode::Mqtt5 => ProtocolVersion::Mqtt5,
        ProtocolMode::Mqtt311 => ProtocolVersion::Mqtt311,
    }
}

/// MQTT message delivery quality of service.
///
/// Enum values match [MQTT spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QualityOfService {

    /// The message is delivered according to the capabilities of the underlying network.
    ///
    /// No response is sent by the receiver and no retry is performed by the sender. The message
    /// arrives at the receiver either once or not at all.
    #[default]
    AtMostOnce = 0,

    /// A level of service that ensures that the message arrives at the receiver at least once.
    AtLeastOnce = 1,

    /// A level of service that ensures that the message arrives at the receiver exactly once.
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QualityOfService {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(QualityOfService::AtMostOnce) }
            1 => { Ok(QualityOfService::AtLeastOnce) }
            2 => { Ok(QualityOfService::ExactlyOnce) }
            _ => {
                let message = format!("QualityOfService::try_from - Invalid quality of service value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for QualityOfService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                QualityOfService::AtMostOnce => { "0 - AtMostOnce" }
                QualityOfService::AtLeastOnce => { "1 - AtLeastOnce" }
                QualityOfService::ExactlyOnce => { "2 - ExactlyOnce" }
            };

        write!(f, "{}", format_string)
    }
}

/// Optional property describing a PUBLISH payload's format.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PayloadFormatIndicator {

    /// The payload is arbitrary binary data
    #[default]
    Bytes = 0,

    /// The payload is a well-formed utf-8 string value.
    Utf8 = 1,
}

impl TryFrom<u8> for PayloadFormatIndicator {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(PayloadFormatIndicator::Bytes) }
            1 => { Ok(PayloadFormatIndicator::Utf8) }
            _ => {
                let message = format!("PayloadFormatIndicator::try_from - invalid payload format indicator value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for PayloadFormatIndicator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                PayloadFormatIndicator::Bytes => { "0 - Bytes" }
                PayloadFormatIndicator::Utf8 => { "1 - Utf8" }
            };

        write!(f, "{}", format_string)
    }
}

/// Configures how retained messages should be handled when subscribing with a topic filter that matches topics with
/// associated retained messages.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetainHandlingType {

    /// The server should always send all retained messages on topics that match a subscription's filter.
    #[default]
    SendOnSubscribe = 0,

    /// The server should send retained messages on topics that match the subscription's filter, but only for the
    /// first matching subscription, per session.
    SendOnSubscribeIfNew = 1,

    /// Subscriptions must not trigger any retained message publishes from the server.
    DontSend = 2,
}

#[cfg(test)]
impl TryFrom<u8> for RetainHandlingType {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(RetainHandlingType::SendOnSubscribe) }
            1 => { Ok(RetainHandlingType::SendOnSubscribeIfNew) }
            2 => { Ok(RetainHandlingType::DontSend) }
            _ => {
                let message = format!("RetainHandlingType::try_from - Invalid retain handling type value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for RetainHandlingType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                RetainHandlingType::SendOnSubscribe => { "0 - SendOnSubscribe" }
                RetainHandlingType::SendOnSubscribeIfNew => { "1 - SendOnSubscribeIfNew" }
                RetainHandlingType::DontSend => { "2 - DontSend" }
            };

        write!(f, "{}", format_string)
    }
}

/// Server return code for connection attempts.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConnectReasonCode {

    /// Returned when the connection is accepted.
    #[default]
    Success = 0,

    /// Returned when the server has a failure but does not want to specify a reason or none
    /// of the other reason codes apply.
    UnspecifiedError = 128,

    /// Returned when data in the CONNECT packet could not be correctly parsed by the server.
    MalformedPacket = 129,

    /// Returned when data in the CONNECT packet does not conform to the MQTT5 specification requirements.
    ProtocolError = 130,

    /// Returned when the CONNECT packet is valid but was not accepted by the server.
    ImplementationSpecificError = 131,

    /// Returned when the server does not support MQTT protocol version specified in the connection.
    UnsupportedProtocolVersion = 132,

    /// Returned when the client identifier in the CONNECT packet is a valid string but not one that
    /// is allowed on the server.
    ClientIdentifierNotValid = 133,

    /// Returned when the server does not accept the username and/or password specified by the client
    /// in the connection packet.
    BadUsernameOrPassword = 134,

    /// Returned when the client is not authorized to connect to the server.
    NotAuthorized = 135,

    /// Returned when the MQTT server is not available.
    ServerUnavailable = 136,

    /// Returned when the server is too busy to make a connection.
    ///
    /// It is recommended that the client try again later.
    ServerBusy = 137,

    /// Returned when the client has been banned from the server.
    Banned = 138,

    /// Returned when the authentication method used in the connection is either not supported on the server or it does
    /// not match the authentication method currently in use in the CONNECT packet.
    BadAuthenticationMethod = 140,

    /// Returned when the Will topic name sent in the connection packet is correctly formed, but is not accepted by
    /// the server.
    TopicNameInvalid = 144,

    /// Returned when the connection packet exceeded the maximum permissible size on the server.
    PacketTooLarge = 149,

    /// Returned when the quota limits set on the server have been met and/or exceeded.
    QuotaExceeded = 151,

    /// Returned when the Will payload in the CONNECT packet does not match the specified payload format indicator.
    PayloadFormatInvalid = 153,

    /// Returned when the server does not retain messages but the connection packet on the client had Will retain enabled.
    RetainNotSupported = 154,

    /// Returned when the server does not support the QOS setting in the Will QOS in the connection packet.
    QosNotSupported = 155,

    /// Returned when the server is telling the client to temporarily use another server instead of the one they
    /// are trying to connect to.
    UseAnotherServer = 156,

    /// Returned when the server is telling the client to permanently use another server instead of the one they
    /// are trying to connect to.
    ServerMoved = 157,

    /// Returned when the server connection rate limit has been exceeded.
    ConnectionRateExceeded = 159,
}

impl ConnectReasonCode {
    /// Returns whether or not the reason code represents a successful connect
    pub fn is_success(&self) -> bool {
        matches!(self, ConnectReasonCode::Success)
    }
}

impl TryFrom<u8> for ConnectReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(ConnectReasonCode::Success) }
            128 => { Ok(ConnectReasonCode::UnspecifiedError) }
            129 => { Ok(ConnectReasonCode::MalformedPacket) }
            130 => { Ok(ConnectReasonCode::ProtocolError) }
            131 => { Ok(ConnectReasonCode::ImplementationSpecificError) }
            132 => { Ok(ConnectReasonCode::UnsupportedProtocolVersion) }
            133 => { Ok(ConnectReasonCode::ClientIdentifierNotValid) }
            134 => { Ok(ConnectReasonCode::BadUsernameOrPassword) }
            135 => { Ok(ConnectReasonCode::NotAuthorized) }
            136 => { Ok(ConnectReasonCode::ServerUnavailable) }
            137 => { Ok(ConnectReasonCode::ServerBusy) }
            138 => { Ok(ConnectReasonCode::Banned) }
            140 => { Ok(ConnectReasonCode::BadAuthenticationMethod) }
            144 => { Ok(ConnectReasonCode::TopicNameInvalid) }
            149 => { Ok(ConnectReasonCode::PacketTooLarge) }
            151 => { Ok(ConnectReasonCode::QuotaExceeded) }
            153 => { Ok(ConnectReasonCode::PayloadFormatInvalid) }
            154 => { Ok(ConnectReasonCode::RetainNotSupported) }
            155 => { Ok(ConnectReasonCode::QosNotSupported) }
            156 => { Ok(ConnectReasonCode::UseAnotherServer) }
            157 => { Ok(ConnectReasonCode::ServerMoved) }
            159 => { Ok(ConnectReasonCode::ConnectionRateExceeded) }
            _ => {
                let message = format!("ConnectReasonCode::try_from - Invalid connect reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for ConnectReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                ConnectReasonCode::Success => { "0 - Success" }
                ConnectReasonCode::UnspecifiedError => { "128 - UnspecifiedError" }
                ConnectReasonCode::MalformedPacket => { "129 - MalformedPacket" }
                ConnectReasonCode::ProtocolError => { "130 - ProtocolError" }
                ConnectReasonCode::ImplementationSpecificError => { "131 - ImplementationSpecificError" }
                ConnectReasonCode::UnsupportedProtocolVersion => { "132 - UnsupportedProtocolVersion" }
                ConnectReasonCode::ClientIdentifierNotValid => { "133 - ClientIdentifierNotValid" }
                ConnectReasonCode::BadUsernameOrPassword => { "134 - BadUsernameOrPassword" }
                ConnectReasonCode::NotAuthorized => { "135 - NotAuthorized" }
                ConnectReasonCode::ServerUnavailable => { "136 - ServerUnavailable" }
                ConnectReasonCode::ServerBusy => { "137 - ServerBusy" }
                ConnectReasonCode::Banned => { "138 - Banned" }
                ConnectReasonCode::BadAuthenticationMethod => { "140 - BadAuthenticationMethod" }
                ConnectReasonCode::TopicNameInvalid => { "144 - TopicNameInvalid" }
                ConnectReasonCode::PacketTooLarge => { "149 - PacketTooLarge" }
                ConnectReasonCode::QuotaExceeded => { "151 - QuotaExceeded" }
                ConnectReasonCode::PayloadFormatInvalid => { "153 - PayloadFormatInvalid" }
                ConnectReasonCode::RetainNotSupported => { "154 - RetainNotSupported" }
                ConnectReasonCode::QosNotSupported => { "155 - QosNotSupported" }
                ConnectReasonCode::UseAnotherServer => { "156 - UseAnotherServer" }
                ConnectReasonCode::ServerMoved => { "157 - ServerMoved" }
                ConnectReasonCode::ConnectionRateExceeded => { "159 - ConnectionRateExceeded" }
            };

        write!(f, "{}", format_string)
    }
}

#[cfg(test)]
pub(crate) fn convert_connect_reason_code_to_311_encoding(reason_code: ConnectReasonCode) -> GneissResult<u8> {
    match reason_code {
        ConnectReasonCode::Success => Ok(0),
        ConnectReasonCode::UnsupportedProtocolVersion => Ok(1),
        ConnectReasonCode::ClientIdentifierNotValid => Ok(2),
        ConnectReasonCode::ServerUnavailable => Ok(3),
        ConnectReasonCode::BadUsernameOrPassword => Ok(4),
        ConnectReasonCode::NotAuthorized => Ok(5),
        _ => Err(GneissError::new_protocol_error("convert_connect_reason_code_to_311_encoding - invalid ConnectReasonCode"))
    }
}

pub(crate) fn convert_311_encoding_to_connect_reason_code(value: u8) -> GneissResult<ConnectReasonCode> {
    match value {
        0 => { Ok(ConnectReasonCode::Success) },
        1 => { Ok(ConnectReasonCode::UnsupportedProtocolVersion) },
        2 => { Ok(ConnectReasonCode::ClientIdentifierNotValid) },
        3 => { Ok(ConnectReasonCode::ServerUnavailable) },
        4 => { Ok(ConnectReasonCode::BadUsernameOrPassword) },
        5 => { Ok(ConnectReasonCode::NotAuthorized) },
        _ => Err(GneissError::new_decoding_failure("convert_311_encoding_to_connect_reason_code - invalid 311 encoding of a ConnectReasonCode"))
    }
}

/// Reason code inside PUBACK packets that indicates the result of the associated PUBLISH request.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubackReasonCode {

    /// Returned when the (QoS 1) publish was accepted by the recipient.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the (QoS 1) publish was accepted but there were no matching subscribers.
    ///
    /// May only be sent by the server.
    NoMatchingSubscribers = 16,

    /// Returned when the (QoS 1) publish was not accepted and the receiver does not want to specify a reason or none
    /// of the other reason codes apply.
    ///
    /// May be sent by the client or the server.
    UnspecifiedError = 128,

    /// Returned when the (QoS 1) publish was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    ImplementationSpecificError = 131,

    /// Returned when the (QoS 1) publish was not authorized by the receiver.
    ///
    /// May be sent by the client or the server.
    NotAuthorized = 135,

    /// Returned when the topic name was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    TopicNameInvalid = 144,

    /// Returned when the packet identifier used in the associated PUBLISH was already in use.
    ///
    /// This can indicate a mismatch in the session state between client and server.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierInUse = 145,

    /// Returned when the associated PUBLISH failed because an internal quota on the recipient was exceeded.
    ///
    /// May be sent by the client or the server.
    QuotaExceeded = 151,

    /// Returned when the PUBLISH packet's payload format did not match its payload format indicator property.
    ///
    /// May be sent by the client or the server.
    PayloadFormatInvalid = 153,
}

impl PubackReasonCode {
    /// Returns whether or not the reason code represents a successful publish
    pub fn is_success(&self) -> bool {
        matches!(self, PubackReasonCode::Success | PubackReasonCode::NoMatchingSubscribers)
    }
}

impl TryFrom<u8> for PubackReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(PubackReasonCode::Success) }
            16 => { Ok(PubackReasonCode::NoMatchingSubscribers) }
            128 => { Ok(PubackReasonCode::UnspecifiedError) }
            131 => { Ok(PubackReasonCode::ImplementationSpecificError) }
            135 => { Ok(PubackReasonCode::NotAuthorized) }
            144 => { Ok(PubackReasonCode::TopicNameInvalid) }
            145 => { Ok(PubackReasonCode::PacketIdentifierInUse) }
            151 => { Ok(PubackReasonCode::QuotaExceeded) }
            153 => { Ok(PubackReasonCode::PayloadFormatInvalid) }
            _ => {
                let message = format!("PubackReasonCode::try_from - invalid puback reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for PubackReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                PubackReasonCode::Success => { "0 - Success" }
                PubackReasonCode::NoMatchingSubscribers => { "16 - NoMatchingSubscribers" }
                PubackReasonCode::UnspecifiedError => { "128 - UnspecifiedError" }
                PubackReasonCode::ImplementationSpecificError => { "131 - ImplementationSpecificError" }
                PubackReasonCode::NotAuthorized => { "135 - NotAuthorized" }
                PubackReasonCode::TopicNameInvalid => { "144 - TopicNameInvalid)" }
                PubackReasonCode::PacketIdentifierInUse => { "145 - PacketIdentifierInUse" }
                PubackReasonCode::QuotaExceeded => { "151 - QuotaExceeded" }
                PubackReasonCode::PayloadFormatInvalid => { "153 - PayloadFormatInvalid" }
            };

        write!(f, "{}", format_string)
    }
}

/// Reason code inside PUBREC packets that indicates the result of the associated QoS 2 PUBLISH request.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901134) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubrecReasonCode {

    /// Returned when the (QoS 2) publish was accepted by the recipient.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the (QoS 2) publish was accepted but there were no matching subscribers.
    ///
    /// May only be sent by the server.
    NoMatchingSubscribers = 16,

    /// Returned when the (QoS 2) publish was not accepted and the receiver does not want to specify a reason or none
    /// of the other reason codes apply.
    ///
    /// May be sent by the client or the server.
    UnspecifiedError = 128,

    /// Returned when the (QoS 2) publish was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    ImplementationSpecificError = 131,

    /// Returned when the (QoS 2) publish was not authorized by the receiver.
    ///
    /// May be sent by the client or the server.
    NotAuthorized = 135,

    /// Returned when the topic name was valid but the receiver was not willing to accept it.
    ///
    /// May be sent by the client or the server.
    TopicNameInvalid = 144,

    /// Returned when the packet identifier used in the associated PUBLISH was already in use.
    ///
    /// This can indicate a mismatch in the session state between client and server.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierInUse = 145,

    /// Returned when the associated PUBLISH failed because an internal quota on the recipient was exceeded.
    ///
    /// May be sent by the client or the server.
    QuotaExceeded = 151,

    /// Returned when the PUBLISH packet's payload format did not match its payload format indicator property.
    ///
    /// May be sent by the client or the server.
    PayloadFormatInvalid = 153,
}

impl PubrecReasonCode {
    /// Returns whether or not the reason code represents a successful publish
    pub fn is_success(&self) -> bool {
        matches!(self, PubrecReasonCode::Success | PubrecReasonCode::NoMatchingSubscribers)
    }
}

impl TryFrom<u8> for PubrecReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(PubrecReasonCode::Success) }
            16 => { Ok(PubrecReasonCode::NoMatchingSubscribers) }
            128 => { Ok(PubrecReasonCode::UnspecifiedError) }
            131 => { Ok(PubrecReasonCode::ImplementationSpecificError) }
            135 => { Ok(PubrecReasonCode::NotAuthorized) }
            144 => { Ok(PubrecReasonCode::TopicNameInvalid) }
            145 => { Ok(PubrecReasonCode::PacketIdentifierInUse) }
            151 => { Ok(PubrecReasonCode::QuotaExceeded) }
            153 => { Ok(PubrecReasonCode::PayloadFormatInvalid) }
            _ => {
                let message = format!("Packet Decode - Invalid pubrec reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for PubrecReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                PubrecReasonCode::Success => { "0 - Success" }
                PubrecReasonCode::NoMatchingSubscribers => { "16 - NoMatchingSubscribers" }
                PubrecReasonCode::UnspecifiedError => { "128 - UnspecifiedError" }
                PubrecReasonCode::ImplementationSpecificError => { "131 - ImplementationSpecificError" }
                PubrecReasonCode::NotAuthorized => { "135 - NotAuthorized" }
                PubrecReasonCode::TopicNameInvalid => { "144 - TopicNameInvalid" }
                PubrecReasonCode::PacketIdentifierInUse => { "145 - PacketIdentifierInUse" }
                PubrecReasonCode::QuotaExceeded => { "151 - QuotaExceeded" }
                PubrecReasonCode::PayloadFormatInvalid => { "153 - PayloadFormatInvalid" }
            };

        write!(f, "{}", format_string)
    }
}

/// Reason code inside PUBREL packets that indicates the result of receiving a PUBREC packet as part of the QoS 2 PUBLISH delivery process.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubrelReasonCode {

    /// Returned when the associated PUBREC was successfully accepted by the recipient.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the associated PUBREC's packet id was not being tracked by the recipient as an in-progress QoS 2 delivery.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierNotFound = 146,
}

impl PubrelReasonCode {
    /// Returns whether the reason code represents a successful pubrec
    pub fn is_success(&self) -> bool {
        matches!(self, PubrelReasonCode::Success)
    }
}

impl TryFrom<u8> for PubrelReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(PubrelReasonCode::Success) }
            146 => { Ok(PubrelReasonCode::PacketIdentifierNotFound) }
            _ => {
                let message = format!("PubrelReasonCode::try_from - Invalid pubrel reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for PubrelReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                PubrelReasonCode::Success => { "0 - Success" }
                PubrelReasonCode::PacketIdentifierNotFound => { "146 - PacketIdentifierNotFound" }
            };

        write!(f, "{}", format_string)
    }
}

/// Reason code inside PUBCOMP packets that indicates the result of receiving a PUBREL packet as part of the QoS 2 publish delivery process.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901154) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubcompReasonCode {

    /// Returned when the associated PUBREL was successfully accepted by the recipient.
    ///
    /// Marks a successful conclusion to the QoS 2 delivery sequence.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the associated PUBREL's packet id was not being tracked by the recipient as an in-progress QoS 2 delivery.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierNotFound = 146,
}

impl PubcompReasonCode {
    /// Returns whether the reason code represents a successful pubrel
    pub fn is_success(&self) -> bool {
        matches!(self, PubcompReasonCode::Success)
    }
}

impl TryFrom<u8> for PubcompReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(PubcompReasonCode::Success) }
            146 => { Ok(PubcompReasonCode::PacketIdentifierNotFound) }
            _ => {
                let message = format!("PubcompReasonCode::try_from - Invalid pubcomp reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for PubcompReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                PubcompReasonCode::Success => { "0 - Success" }
                PubcompReasonCode::PacketIdentifierNotFound => { "146 - PacketIdentifierNotFound" }
            };

        write!(f, "{}", format_string)
    }
}

/// Reason code inside DISCONNECT packets indicating why the connection is being terminated.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisconnectReasonCode {

    /// Returned when the remote endpoint wishes to disconnect normally.
    ///
    /// Will not trigger the publish of a Will message if a Will message was configured on the connection.
    ///
    /// May be sent by the client or server.
    #[default]
    NormalDisconnection = 0,

    /// Returns that the client wants to disconnect but requires that the server publish the Will message configured
    /// on the connection.
    ///
    /// May only be sent by the client.
    DisconnectWithWillMessage = 4,

    /// Returned when the connection was closed but the sender does not want to specify a reason or none
    /// of the other reason codes apply.
    ///
    /// May be sent by the client or the server.
    UnspecifiedError = 128,

    /// Indicates the remote endpoint received a packet that does not conform to the MQTT specification.
    ///
    /// May be sent by the client or the server.
    MalformedPacket = 129,

    /// Returned when an unexpected or out-of-order packet was received by the remote endpoint.
    ///
    /// May be sent by the client or the server.
    ProtocolError = 130,

    /// Returned when a valid packet was received by the remote endpoint, but could not be processed by the current implementation.
    ///
    /// May be sent by the client or the server.
    ImplementationSpecificError = 131,

    /// Returned when the remote endpoint received a packet that represented an operation that was not authorized within
    /// the current connection.
    ///
    /// May only be sent by the server.
    NotAuthorized = 135,

    /// Returned when the server is busy and cannot continue processing packets from the client.
    ///
    /// May only be sent by the server.
    ServerBusy = 137,

    /// Returned when the server is shutting down.
    ///
    /// May only be sent by the server.
    ServerShuttingDown = 139,

    /// Returned when the server closes the connection because no packet from the client has been received in
    /// 1.5 times the KeepAlive time set when the connection was established.
    ///
    /// May only be sent by the server.
    KeepAliveTimeout = 141,

    /// Returned when the server has established another connection with the same client ID as a client's current
    /// connection, causing the current client to become disconnected.
    ///
    /// May only be sent by the server.
    SessionTakenOver = 142,

    /// Returned when the topic filter name is correctly formed but not accepted by the server.
    ///
    /// May only be sent by the server.
    TopicFilterInvalid = 143,

    /// Returned when topic name is correctly formed, but is not accepted.
    ///
    /// May be sent by the client or the server.
    TopicNameInvalid = 144,

    /// Returned when the remote endpoint reached a state where there were more in-progress QoS1+ publishes then the
    /// limit it established for itself when the connection was opened.
    ///
    /// May be sent by the client or the server.
    ReceiveMaximumExceeded = 147,

    /// Returned when the remote endpoint receives a PUBLISH packet that contained a topic alias greater than the
    /// maximum topic alias limit that it established for itself when the connection was opened.
    ///
    /// May be sent by the client or the server.
    TopicAliasInvalid = 148,

    /// Returned when the remote endpoint received a packet whose size was greater than the maximum packet size limit
    /// it established for itself when the connection was opened.
    ///
    /// May be sent by the client or the server.
    PacketTooLarge = 149,

    /// Returned when the remote endpoint's incoming data rate was too high.
    ///
    /// May be sent by the client or the server.
    MessageRateTooHigh = 150,

    /// Returned when an internal quota of the remote endpoint was exceeded.
    ///
    /// May be sent by the client or the server.
    QuotaExceeded = 151,

    /// Returned when the connection was closed due to an administrative action.
    ///
    /// May be sent by the client or the server.
    AdministrativeAction = 152,

    /// Returned when the remote endpoint received a packet where payload format did not match the format specified
    /// by the payload format indicator.
    ///
    /// May be sent by the client or the server.
    PayloadFormatInvalid = 153,

    /// Returned when the server does not support retained messages.
    ///
    /// May only be sent by the server.
    RetainNotSupported = 154,

    /// Returned when the client sends a QOS that is greater than the maximum QOS established when the connection was
    /// opened.
    ///
    /// May only be sent by the server.
    QosNotSupported = 155,

    /// Returned by the server to tell the client to temporarily use a different server.
    ///
    /// May only be sent by the server.
    UseAnotherServer = 156,

    /// Returned by the server to tell the client to permanently use a different server.
    ///
    /// May only be sent by the server.
    ServerMoved = 157,

    /// Returned by the server to tell the client that shared subscriptions are not supported on the server.
    ///
    /// May only be sent by the server.
    SharedSubscriptionsNotSupported = 158,

    /// Returned when the server disconnects the client due to the connection rate being too high.
    ///
    /// May only be sent by the server.
    ConnectionRateExceeded = 159,

    /// Returned by the server when the maximum connection time authorized for the connection was exceeded.
    ///
    /// May only be sent by the server.
    MaximumConnectTime = 160,

    /// Returned by the server when it received a SUBSCRIBE packet with a subscription identifier, but the server does
    /// not support subscription identifiers.
    ///
    /// May only be sent by the server.
    SubscriptionIdentifiersNotSupported = 161,

    /// Returned by the server when it received a SUBSCRIBE packet with a wildcard topic filter, but the server does
    /// not support wildcard topic filters.
    ///
    /// May only be sent by the server.
    WildcardSubscriptionsNotSupported = 162,
}

impl DisconnectReasonCode {
    /// Returns whether the reason code represents a normal disconnect or an error
    pub fn is_success(&self) -> bool {
        matches!(self, DisconnectReasonCode::NormalDisconnection | DisconnectReasonCode::DisconnectWithWillMessage)
    }
}

impl TryFrom<u8> for DisconnectReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(DisconnectReasonCode::NormalDisconnection) }
            4 => { Ok(DisconnectReasonCode::DisconnectWithWillMessage) }
            128 => { Ok(DisconnectReasonCode::UnspecifiedError) }
            129 => { Ok(DisconnectReasonCode::MalformedPacket) }
            130 => { Ok(DisconnectReasonCode::ProtocolError) }
            131 => { Ok(DisconnectReasonCode::ImplementationSpecificError) }
            135 => { Ok(DisconnectReasonCode::NotAuthorized) }
            137 => { Ok(DisconnectReasonCode::ServerBusy) }
            139 => { Ok(DisconnectReasonCode::ServerShuttingDown) }
            141 => { Ok(DisconnectReasonCode::KeepAliveTimeout) }
            142 => { Ok(DisconnectReasonCode::SessionTakenOver) }
            143 => { Ok(DisconnectReasonCode::TopicFilterInvalid) }
            144 => { Ok(DisconnectReasonCode::TopicNameInvalid) }
            147 => { Ok(DisconnectReasonCode::ReceiveMaximumExceeded) }
            148 => { Ok(DisconnectReasonCode::TopicAliasInvalid) }
            149 => { Ok(DisconnectReasonCode::PacketTooLarge) }
            150 => { Ok(DisconnectReasonCode::MessageRateTooHigh) }
            151 => { Ok(DisconnectReasonCode::QuotaExceeded) }
            152 => { Ok(DisconnectReasonCode::AdministrativeAction) }
            153 => { Ok(DisconnectReasonCode::PayloadFormatInvalid) }
            154 => { Ok(DisconnectReasonCode::RetainNotSupported) }
            155 => { Ok(DisconnectReasonCode::QosNotSupported) }
            156 => { Ok(DisconnectReasonCode::UseAnotherServer) }
            157 => { Ok(DisconnectReasonCode::ServerMoved) }
            158 => { Ok(DisconnectReasonCode::SharedSubscriptionsNotSupported) }
            159 => { Ok(DisconnectReasonCode::ConnectionRateExceeded) }
            160 => { Ok(DisconnectReasonCode::MaximumConnectTime) }
            161 => { Ok(DisconnectReasonCode::SubscriptionIdentifiersNotSupported) }
            162 => { Ok(DisconnectReasonCode::WildcardSubscriptionsNotSupported) }
            _ => {
                let message = format!("DisconnectReasonCode::try_from - Invalid disconnect reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for DisconnectReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                DisconnectReasonCode::NormalDisconnection => { "0 - NormalDisconnection" }
                DisconnectReasonCode::DisconnectWithWillMessage => { "4 - DisconnectWithWillMessage" }
                DisconnectReasonCode::UnspecifiedError => { "128 - UnspecifiedError" }
                DisconnectReasonCode::MalformedPacket => { "129 - MalformedPacket" }
                DisconnectReasonCode::ProtocolError => { "130 - ProtocolError" }
                DisconnectReasonCode::ImplementationSpecificError => { "131 - ImplementationSpecificError" }
                DisconnectReasonCode::NotAuthorized => { "135 - NotAuthorized" }
                DisconnectReasonCode::ServerBusy => { "137 - ServerBusy" }
                DisconnectReasonCode::ServerShuttingDown => { "139 - ServerShuttingDown" }
                DisconnectReasonCode::KeepAliveTimeout => { "141 - KeepAliveTimeout" }
                DisconnectReasonCode::SessionTakenOver => { "142 - SessionTakenOver" }
                DisconnectReasonCode::TopicFilterInvalid => { "143 - TopicFilterInvalid" }
                DisconnectReasonCode::TopicNameInvalid => { "144 - TopicNameInvalid" }
                DisconnectReasonCode::ReceiveMaximumExceeded => { "147 - ReceiveMaximumExceeded" }
                DisconnectReasonCode::TopicAliasInvalid => { "148 - TopicAliasInvalid" }
                DisconnectReasonCode::PacketTooLarge => { "149 - PacketTooLarge" }
                DisconnectReasonCode::MessageRateTooHigh => { "150 - MessageRateTooHigh" }
                DisconnectReasonCode::QuotaExceeded => { "151 - QuotaExceeded" }
                DisconnectReasonCode::AdministrativeAction => { "152 - AdministrativeAction" }
                DisconnectReasonCode::PayloadFormatInvalid => { "153 - PayloadFormatInvalid" }
                DisconnectReasonCode::RetainNotSupported => { "154 - RetainNotSupported" }
                DisconnectReasonCode::QosNotSupported => { "155 - QosNotSupported" }
                DisconnectReasonCode::UseAnotherServer => { "156 - UseAnotherServer" }
                DisconnectReasonCode::ServerMoved => { "157 - ServerMoved" }
                DisconnectReasonCode::SharedSubscriptionsNotSupported => { "158 - SharedSubscriptionsNotSupported" }
                DisconnectReasonCode::ConnectionRateExceeded => { "159 - ConnectionRateExceeded" }
                DisconnectReasonCode::MaximumConnectTime => { "160 - MaximumConnectTime" }
                DisconnectReasonCode::SubscriptionIdentifiersNotSupported => { "161 - SubscriptionIdentifiersNotSupported" }
                DisconnectReasonCode::WildcardSubscriptionsNotSupported => { "162 - WildcardSubscriptionsNotSupported" }
            };

        write!(f, "{}", format_string)
    }
}

/// Reason codes inside SUBACK packet payloads that specify the results for each subscription in the associated
/// SUBSCRIBE packet.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SubackReasonCode {

    /// Returned when the subscription was accepted and the maximum QOS sent will be QOS 0.
    #[default]
    GrantedQos0 = 0,

    /// Returned when the subscription was accepted and the maximum QOS sent will be QOS 1.
    GrantedQos1 = 1,

    /// Returned when the subscription was accepted and the maximum QOS sent will be QOS 2.
    GrantedQos2 = 2,

    /// Returned when the connection was closed but the sender does not want to specify a reason or none
    /// of the other reason codes apply.
    UnspecifiedError = 128,

    /// Returned when the subscription was valid but the server did not accept it.
    ImplementationSpecificError = 131,

    /// Returned when the client was not authorized to make the subscription on the server.
    NotAuthorized = 135,

    /// Returned when the subscription topic filter was correctly formed but not allowed for the client.
    TopicFilterInvalid = 143,

    /// Returned when the packet identifier was already in use on the server.
    PacketIdentifierInUse = 145,

    /// Returned when a subscribe-related quota set on the server was exceeded.
    QuotaExceeded = 151,

    /// Returned when the subscription's topic filter was a shared subscription and the server does not support
    /// shared subscriptions.
    SharedSubscriptionsNotSupported = 158,

    /// Returned when the SUBSCRIBE packet contained a subscription identifier and the server does not support
    /// subscription identifiers.
    SubscriptionIdentifiersNotSupported = 161,

    /// Returned when the subscription's topic filter contains a wildcard but the server does not support
    /// wildcard subscriptions.
    WildcardSubscriptionsNotSupported = 162,
}

impl SubackReasonCode {
    /// Returns whether or not the reason code represents a successful subscription
    pub fn is_success(&self) -> bool {
        matches!(self, SubackReasonCode::GrantedQos0 | SubackReasonCode::GrantedQos1 | SubackReasonCode::GrantedQos2)
    }
}

impl TryFrom<u8> for SubackReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(SubackReasonCode::GrantedQos0) }
            1 => { Ok(SubackReasonCode::GrantedQos1) }
            2 => { Ok(SubackReasonCode::GrantedQos2) }
            128 => { Ok(SubackReasonCode::UnspecifiedError) }
            131 => { Ok(SubackReasonCode::ImplementationSpecificError) }
            135 => { Ok(SubackReasonCode::NotAuthorized) }
            143 => { Ok(SubackReasonCode::TopicFilterInvalid) }
            145 => { Ok(SubackReasonCode::PacketIdentifierInUse) }
            151 => { Ok(SubackReasonCode::QuotaExceeded) }
            158 => { Ok(SubackReasonCode::SharedSubscriptionsNotSupported) }
            161 => { Ok(SubackReasonCode::SubscriptionIdentifiersNotSupported) }
            162 => { Ok(SubackReasonCode::WildcardSubscriptionsNotSupported) }
            _ => {
                let message = format!("SubackReasonCode::try_from - invalid suback reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for SubackReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason_string =
            match self {
                SubackReasonCode::GrantedQos0 => { "0 - GrantedQos0" }
                SubackReasonCode::GrantedQos1 => { "1 - GrantedQos1" }
                SubackReasonCode::GrantedQos2 => { "2 - GrantedQos2" }
                SubackReasonCode::UnspecifiedError => { "128 - UnspecifiedError" }
                SubackReasonCode::ImplementationSpecificError => { "131 - ImplementationSpecificError" }
                SubackReasonCode::NotAuthorized => { "135 - NotAuthorized" }
                SubackReasonCode::TopicFilterInvalid => { "143 - TopicFilterInvalid" }
                SubackReasonCode::PacketIdentifierInUse => { "145 - PacketIdentifierInUse" }
                SubackReasonCode::QuotaExceeded => { "151 - QuotaExceeded" }
                SubackReasonCode::SharedSubscriptionsNotSupported => { "158 - SharedSubscriptionsNotSupported" }
                SubackReasonCode::SubscriptionIdentifiersNotSupported => { "161 - SubscriptionIdentifiersNotSupported" }
                SubackReasonCode::WildcardSubscriptionsNotSupported => { "162 - WildcardSubscriptionsNotSupported" }
            };

        write!(f, "{}", reason_string)
    }
}

#[cfg(test)]
pub(crate) fn convert_suback_reason_code_to_311_encoding(reason_code: SubackReasonCode) -> GneissResult<u8> {
    match reason_code {
        SubackReasonCode::GrantedQos0 => Ok(0),
        SubackReasonCode::GrantedQos1 => Ok(1),
        SubackReasonCode::GrantedQos2 => Ok(2),
        _ => Ok(128),
    }
}

pub(crate) fn convert_311_encoding_to_suback_reason_code(value: u8) -> GneissResult<SubackReasonCode> {
    match value {
        0 => Ok(SubackReasonCode::GrantedQos0),
        1 => Ok(SubackReasonCode::GrantedQos1),
        2 => Ok(SubackReasonCode::GrantedQos2),
        128 => Ok(SubackReasonCode::UnspecifiedError),
        _ => {
            let message = format!("convert_311_encoding_to_suback_reason_code - invalid suback reason code value ({})", value);
            error!("{}", message);
            Err(GneissError::new_decoding_failure(message))
        }
    }
}

/// Reason codes inside UNSUBACK packet payloads that specify the results for each topic filter in the associated
/// UNSUBSCRIBE packet.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnsubackReasonCode {

    /// Returned when the unsubscribe was successful and the client is no longer subscribed to the topic filter on the server.
    #[default]
    Success = 0,

    /// Returned when the topic filter did not match one of the client's existing subscriptions on the server.
    NoSubscriptionExisted = 17,

    /// Returned when the unsubscribe of the topic filter was not accepted and the server does not want to specify a
    /// reason or none of the other reason codes apply.
    UnspecifiedError = 128,

    /// Returned when the topic filter was valid but the server does not accept an unsubscribe for it.
    ImplementationSpecificError = 131,

    /// Returned when the client was not authorized to unsubscribe from that topic filter on the server.
    NotAuthorized = 135,

    /// Returned when the topic filter was correctly formed but is not allowed for the client on the server.
    TopicNameInvalid = 144,

    /// Returned when the packet identifier was already in use on the server.
    PacketIdentifierInUse = 145,
}

impl UnsubackReasonCode {
    /// Returns whether or not the reason code represents a successful unsubscribe
    pub fn is_success(&self) -> bool {
        matches!(self, UnsubackReasonCode::Success | UnsubackReasonCode::NoSubscriptionExisted)
    }
}

impl TryFrom<u8> for UnsubackReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(UnsubackReasonCode::Success) }
            17 => { Ok(UnsubackReasonCode::NoSubscriptionExisted) }
            128 => { Ok(UnsubackReasonCode::UnspecifiedError) }
            131 => { Ok(UnsubackReasonCode::ImplementationSpecificError) }
            135 => { Ok(UnsubackReasonCode::NotAuthorized) }
            144 => { Ok(UnsubackReasonCode::TopicNameInvalid) }
            145 => { Ok(UnsubackReasonCode::PacketIdentifierInUse) }
            _ => {
                let message = format!("UnsubackReasonCode::try_from - Invalid unsuback reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for UnsubackReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason_string =
            match self {
                UnsubackReasonCode::Success => { "0 - Success" }
                UnsubackReasonCode::NoSubscriptionExisted => { "17 - NoSubscriptionExisted" }
                UnsubackReasonCode::UnspecifiedError => { "128 - UnspecifiedError" }
                UnsubackReasonCode::ImplementationSpecificError => { "131 - ImplementationSpecificError" }
                UnsubackReasonCode::NotAuthorized => { "135 - NotAuthorized" }
                UnsubackReasonCode::TopicNameInvalid => { "144 - TopicNameInvalid" }
                UnsubackReasonCode::PacketIdentifierInUse => { "145 - PacketIdentifierInUse" }
            };

        write!(f, "{}", reason_string)
    }
}

/// Reason code that specifies the response to a received AUTH packet.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901220) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AuthenticateReasonCode {

    /// Notification that the authentication exchange is both complete and considered successful.
    ///
    /// Only the server may send this code.
    #[default]
    Success = 0,

    /// A request that the recipient should continue the authentication exchange.
    ///
    /// Either the client or server may send this code.
    ContinueAuthentication = 24,

    /// The associated packet represents an attempt to reauthenticate an established connection.
    ///
    /// Only the client may send this code.
    ReAuthenticate = 25,
}

impl TryFrom<u8> for AuthenticateReasonCode {
    type Error = GneissError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => { Ok(AuthenticateReasonCode::Success) }
            24 => { Ok(AuthenticateReasonCode::ContinueAuthentication) }
            25 => { Ok(AuthenticateReasonCode::ReAuthenticate) }
            _ => {
                let message = format!("AuthenticateReasonCode::try_from - Invalid authenticate reason code value ({})", value);
                error!("{}", message);
                Err(GneissError::new_decoding_failure(message))
            }
        }
    }
}

impl fmt::Display for AuthenticateReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let format_string =
            match self {
                AuthenticateReasonCode::Success => { "0 - Success" }
                AuthenticateReasonCode::ContinueAuthentication => { "24 - ContinueAuthentication" }
                AuthenticateReasonCode::ReAuthenticate => { "25 - ReAuthenticate" }
            };

        write!(f, "{}", format_string)
    }
}

/// Data model for MQTT5 user properties.
///
/// A user property is a name-value pair of utf-8 strings that can be added to MQTT5 packets. Names are
/// not unique; a given name value can appear more than once in a packet.
///
/// User properties are required to be a utf-8 string pair, as specified by the
/// [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901013).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UserProperty {
    pub(crate) name: String,
    pub(crate) value: String,
}

impl UserProperty {

    /// Creates a new user property
    pub fn new(name: String, value: String) -> Self {
        Self {name, value}
    }

    /// Returns the user property name
    pub fn name(&self) -> &str { &self.name }

    /// Returns the user property value
    pub fn value(&self) -> &str { &self.value }
}

/// Specifies a single subscription within a Subscribe operation
///
/// See [MQTT Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Subscription {
    pub(crate) topic_filter: String,
    pub(crate) qos: QualityOfService,
    pub(crate) no_local: bool,
    pub(crate) retain_as_published: bool,
    pub(crate) retain_handling_type: RetainHandlingType,
}

impl Subscription {

    /// Creates a new builder for a Subscription
    pub fn builder(topic_filter: String, qos: QualityOfService) -> SubscriptionBuilder {
        SubscriptionBuilder::new(topic_filter, qos)
    }

    /// Creates a new subscription using only required options
    pub fn new_simple(topic_filter: String, qos: QualityOfService) -> Subscription {
        SubscriptionBuilder::new(topic_filter, qos).build()
    }

    /// Returns the topic filter to subscribe to
    ///
    /// See [MQTT Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn topic_filter(&self) -> &str { self.topic_filter.as_str() }

    /// Returns the maximum QoS on which the subscriber will accept publish messages.
    ///
    /// Negotiated QoS may be different.
    ///
    /// See [MQTT Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn qos(&self) -> QualityOfService { self.qos }

    /// Returns if the server should not send publishes to a client when that client was the one who sent the publish?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn no_local(&self) -> bool { self.no_local }

    /// Returns if messages sent due to this subscription should keep the retain flag preserved on the message
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn retain_as_published(&self) -> bool { self.retain_as_published }

    /// Returns if retained messages on matching topics should be sent in reaction to this subscription
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn retain_handling_type(&self) -> RetainHandlingType { self.retain_handling_type }
}

/// Builder type for Subscription instances
pub struct SubscriptionBuilder {
    subscription: Subscription
}

impl SubscriptionBuilder {
    pub(crate) fn new(topic_filter: String, qos: QualityOfService) -> Self {
        SubscriptionBuilder {
            subscription: Subscription {
                topic_filter,
                qos,
                ..Default::default()
            }
        }
    }

    /// Sets if the server should not send publishes to a client when that client was the one who sent the publish?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn with_no_local(mut self, no_local: bool) -> Self {
        self.subscription.no_local = no_local;
        self
    }

    /// Sets if messages sent due to this subscription should keep the retain flag preserved on the message
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn with_retain_as_published(mut self, retain_as_published: bool) -> Self {
        self.subscription.retain_as_published = retain_as_published;
        self
    }

    /// Sets if retained messages on matching topics should be sent in reaction to this subscription
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub fn retain_handling_type(mut self, retain_handling_type: RetainHandlingType) -> Self {
        self.subscription.retain_handling_type = retain_handling_type;
        self
    }

    /// Builds a new Subscription.  Consumes the builder in the process.
    pub fn build(self) -> Subscription {
        self.subscription
    }
}

/// Data model of an [MQTT5 AUTH](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct AuthPacket {

    /// Specifies an endpoint's response to a previously-received AUTH packet as part of an authentication exchange.
    ///
    /// See [MQTT5 Authenticate Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901220)
    pub(crate) reason_code: AuthenticateReasonCode,

    /// Authentication method this packet corresponds to.  The authentication method must remain the
    /// same for the entirety of an authentication exchange.
    ///
    /// The MQTT5 specification lists the authentication method property as required, from a
    /// protocol perspective.  At the same time it specifies that it is valid to short-circuit the
    /// packet encoding if the reason is Success and there are no properties.  This is a bit
    /// self-contradictory, but we resolve it by modeling the authentication method as an optional
    /// string (so supporting the short-circuited encode/decode) but -- despite the option type --
    /// the packet fails validation if authentication_method is None.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901223)
    pub(crate) authentication_method: Option<String>,

    /// Method-specific binary data included in this step of an authentication exchange.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901224)
    pub(crate) authentication_data: Option<Vec<u8>>,

    /// Additional diagnostic information or context.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901225)
    pub(crate) reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901226)
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT CONNACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901074) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnackPacket {
    pub(crate) session_present: bool,
    pub(crate) reason_code: ConnectReasonCode,
    pub(crate) session_expiry_interval: Option<u32>,
    pub(crate) receive_maximum: Option<u16>,
    pub(crate) maximum_qos: Option<QualityOfService>,
    pub(crate) retain_available: Option<bool>,
    pub(crate) maximum_packet_size: Option<u32>,
    pub(crate) assigned_client_identifier: Option<String>,
    pub(crate) topic_alias_maximum: Option<u16>,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
    pub(crate) wildcard_subscriptions_available: Option<bool>,
    pub(crate) subscription_identifiers_available: Option<bool>,
    pub(crate) shared_subscriptions_available: Option<bool>,
    pub(crate) server_keep_alive: Option<u16>,
    pub(crate) response_information: Option<String>,
    pub(crate) server_reference: Option<String>,
    pub(crate) authentication_method: Option<String>,
    pub(crate) authentication_data: Option<Vec<u8>>,
}

impl ConnackPacket {

    /// Returns true if the client rejoined an existing session on the server, false otherwise.
    ///
    /// See [MQTT Session Present](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901078)
    pub fn session_present(&self) -> bool { self.session_present }

    /// Returns a result value that indicates either success or the reason for failure for the connection attempt.
    ///
    /// See [MQTT Connect Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079)
    pub fn reason_code(&self) -> ConnectReasonCode { self.reason_code }

    /// Returns the time interval, in seconds, that the server will persist this connection's MQTT session state
    /// for.  If present, this value overrides any session expiry specified in the preceding CONNECT packet.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901082)
    pub fn session_expiry_interval(&self) -> Option<u32> { self.session_expiry_interval }

    /// Returns the maximum amount of in-flight QoS 1 or 2 messages that the server is willing to handle at once.  If omitted,
    /// the limit is based on the valid MQTT packet id space (65535).
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083)
    pub fn receive_maximum(&self) -> Option<u16> { self.receive_maximum }

    /// Returns the maximum message delivery quality of service that the server will allow on this connection.
    ///
    /// See [MQTT5 Maximum QoS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901084)
    pub fn maximum_qos(&self) -> Option<QualityOfService> { self.maximum_qos }

    /// Returns whether the server supports retained messages.  If undefined, retained messages are
    /// supported.
    ///
    /// See [MQTT5 Retain Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901085)
    pub fn retain_available(&self) -> Option<bool> { self.retain_available }

    /// Returns the maximum packet size, in bytes, that the server is willing to accept.  If undefined, there
    /// is no limit beyond what is imposed by the MQTT spec itself.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086)
    pub fn maximum_packet_size(&self) -> Option<u32> { self.maximum_packet_size }

    /// Returns the client identifier assigned to this connection by the server.  Only valid when the client id of
    /// the preceding CONNECT packet was left empty.
    ///
    /// See [MQTT5 Assigned Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901087)
    pub fn assigned_client_identifier(&self) -> Option<&str> { self.assigned_client_identifier.as_deref() }

    /// Returns the maximum topic alias value that the server will accept from the client.  If undefined, then
    /// the server does not allow topic aliases in publishes sent to it.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088)
    pub fn topic_alias_maximum(&self) -> Option<u16> { self.topic_alias_maximum }

    /// Returns any additional diagnostic information about the result of the connection attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901089)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901090)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }

    /// Returns whether the server supports wildcard subscriptions.  If undefined, wildcard subscriptions
    /// are supported.
    ///
    /// See [MQTT5 Wildcard Subscriptions Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091)
    pub fn wildcard_subscriptions_available(&self) -> Option<bool> { self.wildcard_subscriptions_available }

    /// Returns whether the server supports subscription identifiers.  If undefined, subscription identifiers
    /// are supported.
    ///
    /// See [MQTT5 Subscription Identifiers Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092)
    pub fn subscription_identifiers_available(&self) -> Option<bool> { self.subscription_identifiers_available }

    /// Returns whether the server supports shared subscription topic filters.  If undefined, shared subscriptions
    /// are supported.
    ///
    /// See [MQTT5 Shared Subscriptions Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093)
    pub fn shared_subscriptions_available(&self) -> Option<bool> { self.shared_subscriptions_available }

    /// Returns the server-requested override of the keep alive interval, in seconds.  If undefined, the keep alive value sent
    /// by the client should be used.
    ///
    /// See [MQTT5 Server Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094)
    pub fn server_keep_alive(&self) -> Option<u16> { self.server_keep_alive }

    /// Returns a value that can be used in the creation of a response topic associated with this connection.  MQTT5-based
    /// request/response is outside the purview of the MQTT5 spec and this client.
    ///
    /// See [MQTT5 Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901095)
    pub fn response_information(&self) -> Option<&str> { self.response_information.as_deref() }

    /// Returns the name of an alternate server that the client may temporarily or permanently attempt
    /// to connect to instead of the configured endpoint.  Will only be set if the reason code indicates another
    /// server may be used (ServerMoved, UseAnotherServer).
    ///
    /// See [MQTT5 Server Reference](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901096)
    pub fn server_reference(&self) -> Option<&str> { self.server_reference.as_deref() }

    /// Returns the authentication method used in the authentication exchange that led to this CONNACK packet being sent.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901097)
    pub fn authentication_method(&self) -> Option<&str> { self.authentication_method.as_deref() }

    /// Returns any authentication method specific binary data associated with the authentication exchange that led to this
    /// CONNACK packet being sent.
    ///
    /// Developer Note: It is likely that this field is only relevant in authentication exchanges that *DO NOT*
    /// need AUTH packets to reach a successful conclusion, otherwise the final server->client authentication
    /// data would have been sent with the final server->client AUTH packet that included the success reason code.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901098)
    pub fn authentication_data(&self) -> Option<&[u8]> { self.authentication_data.as_deref() }
}

/// Data model of an [MQTT CONNECT](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901033) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ConnectPacket {
    pub(crate) keep_alive_interval_seconds: u16,
    pub(crate) clean_start: bool,
    pub(crate) client_id: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<Vec<u8>>,
    pub(crate) session_expiry_interval_seconds: Option<u32>,
    pub(crate) request_response_information: Option<bool>,
    pub(crate) request_problem_information: Option<bool>,
    pub(crate) receive_maximum: Option<u16>,
    pub(crate) topic_alias_maximum: Option<u16>,
    pub(crate) maximum_packet_size_bytes: Option<u32>,
    pub(crate) authentication_method: Option<String>,
    pub(crate) authentication_data: Option<Vec<u8>>,
    pub(crate) will_delay_interval_seconds: Option<u32>,
    pub(crate) will: Option<PublishPacket>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

/// Data model of an [MQTT DISCONNECT](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901205) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DisconnectPacket {
    pub(crate) reason_code: DisconnectReasonCode,
    pub(crate) session_expiry_interval_seconds: Option<u32>,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
    pub(crate) server_reference: Option<String>,
}

impl DisconnectPacket {

    /// Creates a new builder for a DisconnectPacket.
    pub fn builder() -> DisconnectPacketBuilder {
        DisconnectPacketBuilder::new()
    }

    /// Returns a result value indicating the reason that the sender is closing the connection
    ///
    /// See [MQTT5 Disconnect Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208)
    pub fn reason_code(&self) -> DisconnectReasonCode { self.reason_code }

    /// Returns a change to the session expiry interval negotiated at connection time as part of the disconnect.  Only
    /// valid for  DISCONNECT packets sent from client to server.  It is not valid to attempt to change session expiry
    /// from zero to a non-zero value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211)
    pub fn session_expiry_interval_seconds(&self) -> Option<u32> { self.session_expiry_interval_seconds }

    /// Returns any additional diagnostic information about the reason that the sender is closing the connection
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901212)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901213)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }

    /// Returns an alternate server that the client may temporarily or permanently attempt
    /// to connect to instead of the configured endpoint.  Will only be set if the reason code indicates another
    /// server may be used (ServerMoved, UseAnotherServer).
    ///
    /// See [MQTT5 Server Reference](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901214)
    pub fn server_reference(&self) -> Option<&str> { self.server_reference.as_deref() }
}

/// Builder type for DisconnectPacket instances
pub struct DisconnectPacketBuilder {
    packet: DisconnectPacket
}

impl DisconnectPacketBuilder {
    pub(crate) fn new() -> Self {
        DisconnectPacketBuilder {
            packet: DisconnectPacket {
                ..Default::default()
            }
        }
    }

    /// Sets a result value indicating the reason that the sender is closing the connection
    ///
    /// See [MQTT5 Disconnect Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208)
    pub fn with_reason_code(mut self, reason_code: DisconnectReasonCode) -> Self {
        self.packet.reason_code = reason_code;
        self
    }

    /// Requests a change to the session expiry interval negotiated at connection time as part of the disconnect.  Only
    /// valid for  DISCONNECT packets sent from client to server.  It is not valid to attempt to change session expiry
    /// from zero to a non-zero value.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211)
    pub fn with_session_expiry_interval_seconds(mut self, session_expiry_interval_seconds: u32) -> Self {
        self.packet.session_expiry_interval_seconds = Some(session_expiry_interval_seconds);
        self
    }

    /// Sets any additional diagnostic information about the reason that the sender is closing the connection
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901212)
    pub fn with_reason_string(mut self, reason_string: String) -> Self {
        self.packet.reason_string = Some(reason_string);
        self
    }

    /// Adds a user property to the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901184)
    pub fn with_user_property(mut self, property: UserProperty) -> Self {
        if let Some(user_properties) = &mut self.packet.user_properties {
            user_properties.push(property);
        } else {
            self.packet.user_properties = Some(vec!(property));
        }

        self
    }

    /// Builds a new DisconnectPacket.  Consumes the builder in the process.
    pub fn build(self) -> DisconnectPacket {
        self.packet
    }
}

/// Data model of an [MQTT PINGREQ](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901195) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PingreqPacket {}

/// Data model of an [MQTT PINGRESP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901200) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PingrespPacket {}

/// Data model of an [MQTT PUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubackPacket {
    pub(crate) packet_id: u16,
    pub(crate) reason_code: PubackReasonCode,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl PubackPacket {

    /// Returns success indicator or failure reason for the associated PUBLISH packet.
    ///
    /// See [MQTT5 PUBACK Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124)
    pub fn reason_code(&self) -> PubackReasonCode { self.reason_code }

    /// Returns any additional diagnostic information about the result of the PUBLISH attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901127)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901128)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }
}

/// Data model of an [MQTT PUBCOMP](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901151) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubcompPacket {
    pub(crate) packet_id: u16,
    pub(crate) reason_code: PubcompReasonCode,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl PubcompPacket {

    /// Returns success indicator or failure reason for the final step of a QoS 2 PUBLISH delivery.
    ///
    /// See [MQTT5 PUBCOMP Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901154)
    pub fn reason_code(&self) -> PubcompReasonCode { self.reason_code }

    /// Returns any additional diagnostic information about the final step of a QoS 2 PUBLISH delivery.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901157)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901128)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }
}

/// Data model of an [MQTT PUBLISH](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901100) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PublishPacket {
    pub(crate) packet_id: u16,
    pub(crate) topic: String,
    pub(crate) qos: QualityOfService,
    pub(crate) duplicate: bool,
    pub(crate) retain: bool,
    pub(crate) payload: Option<Vec<u8>>,
    pub(crate) payload_format: Option<PayloadFormatIndicator>,
    pub(crate) message_expiry_interval_seconds: Option<u32>,
    pub(crate) topic_alias: Option<u16>,
    pub(crate) response_topic: Option<String>,
    pub(crate) correlation_data: Option<Vec<u8>>,
    pub(crate) subscription_identifiers: Option<Vec<u32>>,
    pub(crate) content_type: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl PublishPacket {

    /// Creates a new builder for a PublishPacket.
    pub fn builder(topic: String, qos: QualityOfService) -> PublishPacketBuilder {
        PublishPacketBuilder::new(topic, qos)
    }

    /// Sent publishes - returns the topic this message should be published to.
    ///
    /// Received publishes - returns the topic this message was published to.
    ///
    /// See [MQTT Topic Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901107)
    pub fn topic(&self) -> &str { self.topic.as_str() }

    /// Sent publishes - returns the MQTT quality of service level this message should be delivered with.
    ///
    /// Received publishes - returns the MQTT quality of service level this message was delivered at.
    ///
    /// See [MQTT QoS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901103)
    pub fn qos(&self) -> QualityOfService { self.qos }

    /// Returns whether this packet is a resend of a previously-submitted Publish
    pub fn duplicate(&self) -> bool { self.duplicate }

    /// Returns true if this is a retained message, false otherwise.
    ///
    /// Always set on received publishes; on sent publishes, undefined implies false.
    ///
    /// See [MQTT Retain](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901104)
    pub fn retain(&self) -> bool { self.retain }

    /// Returns the payload of the publish message.
    ///
    /// See [MQTT Publish Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901119)
    pub fn payload(&self) -> Option<&[u8]> { self.payload.as_deref() }

    /// Returns a property specifying the format of the payload data.
    ///
    /// The client does not enforce or use this value in a meaningful way.
    ///
    /// See [MQTT5 Payload Format Indicator](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111)
    pub fn payload_format(&self) -> Option<PayloadFormatIndicator> { self.payload_format }

    /// Sent publishes - returns the maximum amount of time allowed to elapse for message delivery before the server
    /// should instead delete the message (relative to a recipient).
    ///
    /// Received publishes - returns the remaining amount of time (from the server's perspective) before the message would
    /// have been deleted relative to the subscribing client.
    ///
    /// If left undefined, indicates no expiration timeout.
    ///
    /// See [MQTT5 Message Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901112)
    pub fn message_expiry_interval_seconds(&self) -> Option<u32> { self.message_expiry_interval_seconds }

    /// Returns an opaque topic string intended to assist with request/response implementations.  Not internally
    /// meaningful to MQTT5 or this client.
    ///
    /// See [MQTT5 Response Topic](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901114)
    pub fn response_topic(&self) -> Option<&str> { self.response_topic.as_deref() }

    /// Returns opaque binary data used to correlate between publish messages, as a potential avenue for request-response
    /// implementation.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Correlation Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901115)
    pub fn correlation_data(&self) -> Option<&[u8]> { self.correlation_data.as_deref() }

    /// Returns the subscription identifiers of all the subscriptions this message matched (inbound publishes only).
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901117)
    pub fn subscription_identifiers(&self) -> Option<&[u32]> { self.subscription_identifiers.as_deref() }

    /// Returns the content type of the payload.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Content Type](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901118)
    pub fn content_type(&self) -> Option<&str> { self.content_type.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901116)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }
}

/// Builder type for PublishPacket instances
pub struct PublishPacketBuilder {
    packet: PublishPacket
}

impl PublishPacketBuilder {
    pub(crate) fn new(topic: String, qos: QualityOfService) -> Self {
        PublishPacketBuilder {
            packet: PublishPacket {
                topic,
                qos,
                ..Default::default()
            }
        }
    }

    /// Sets if this should be a retained message
    ///
    /// See [MQTT Retain](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901104)
    pub fn with_retain(mut self, retain: bool) -> Self {
        self.packet.retain = retain;
        self
    }

    /// Sets the payload of the publish message.
    ///
    /// See [MQTT Publish Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901119)
    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.packet.payload = Some(payload);
        self
    }

    /// Sets a property specifying the format of the payload data.
    ///
    /// The client does not enforce or use this value in a meaningful way.
    ///
    /// See [MQTT5 Payload Format Indicator](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111)
    pub fn with_payload_format(mut self, payload_format: PayloadFormatIndicator) -> Self {
        self.packet.payload_format = Some(payload_format);
        self
    }

    /// Sets the maximum amount of time allowed to elapse for message delivery before the server
    /// should instead delete the message (relative to a recipient).
    ///
    /// If left undefined, indicates no expiration timeout.
    ///
    /// See [MQTT5 Message Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901112)
    pub fn with_message_expiry_interval_seconds(mut self, message_expiry_interval_seconds: u32) -> Self {
        self.packet.message_expiry_interval_seconds = Some(message_expiry_interval_seconds);
        self
    }

    /// Sets an opaque topic string intended to assist with request/response implementations.  Not internally
    /// meaningful to MQTT5 or this client.
    ///
    /// See [MQTT5 Response Topic](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901114)
    pub fn with_response_topic(mut self, response_topic: String) -> Self {
        self.packet.response_topic = Some(response_topic);
        self
    }

    /// Returns opaque binary data used to correlate between publish messages, as a potential avenue for request-response
    /// implementation.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Correlation Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901115)
    pub fn with_correlation_data(mut self, correlation_data: Vec<u8>) -> Self {
        self.packet.correlation_data = Some(correlation_data);
        self
    }

    /// Sets the content type of the payload.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Content Type](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901118)
    pub fn with_content_type(mut self, content_type: String) -> Self {
        self.packet.content_type = Some(content_type);
        self
    }

    /// Adds a user property to the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901116)
    pub fn with_user_property(mut self, property: UserProperty) -> Self {
        if let Some(user_properties) = &mut self.packet.user_properties {
            user_properties.push(property);
        } else {
            self.packet.user_properties = Some(vec!(property));
        }

        self
    }

    /// Builds a new PublishPacket.  Consumes the builder in the process.
    pub fn build(self) -> PublishPacket {
        self.packet
    }
}

/// Data model of an [MQTT PUBREC](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901131) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubrecPacket {
    pub(crate) packet_id: u16,
    pub(crate) reason_code: PubrecReasonCode,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl PubrecPacket {

    /// Returns success indicator or failure reason for the initial step of the QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 PUBREC Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901134)
    pub fn reason_code(&self) -> PubrecReasonCode { self.reason_code }

    /// Returns any additional diagnostic information about the result of the PUBLISH attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901147)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901128)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }
}

/// Data model of an [MQTT PUBREL](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901141) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PubrelPacket {
    pub(crate) packet_id: u16,
    pub(crate) reason_code: PubrelReasonCode,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl PubrelPacket {

    /// Returns success indicator or failure reason for the middle step of the QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 PUBREL Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901144)
    pub fn reason_code(&self) -> PubrelReasonCode { self.reason_code }

    /// Returns any additional diagnostic information about the ongoing QoS 2 PUBLISH delivery process.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901147)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901128)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }
}

/// Data model of an [MQTT SUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901171) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubackPacket {
    pub(crate) packet_id: u16,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
    pub(crate) reason_codes: Vec<SubackReasonCode>,
}

impl SubackPacket {

    /// Returns any additional diagnostic information about the result of the SUBSCRIBE attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901176)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901177)
    pub fn user_properties(&self) -> Option<&[UserProperty]> { self.user_properties.as_deref() }

    /// Returns a list of reason codes indicating the result of each individual subscription entry in the
    /// associated SUBSCRIBE packet.
    ///
    /// See [MQTT Suback Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901178)
    pub fn reason_codes(&self) -> &[SubackReasonCode] { self.reason_codes.as_slice() }
}

/// Data model of an [MQTT SUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SubscribePacket {
    pub(crate) packet_id: u16,
    pub(crate) subscriptions: Vec<Subscription>,
    pub(crate) subscription_identifier: Option<u32>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl SubscribePacket {

    /// Creates a new builder for a SubscribePacket.
    pub fn builder() -> SubscribePacketBuilder {
        SubscribePacketBuilder::new()
    }
}

/// Builder type for SubscribePacket instances
pub struct SubscribePacketBuilder {
    packet: SubscribePacket
}

impl SubscribePacketBuilder {
    pub(crate) fn new() -> Self {
        SubscribePacketBuilder {
            packet: SubscribePacket::default()
        }
    }

    /// Adds a subscription to the list of subscriptions that the client wishes to listen to
    ///
    /// See [MQTT Subscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901168)
    pub fn with_subscription(mut self, subscription: Subscription) -> Self {
        self.packet.subscriptions.push(subscription);
        self
    }

    /// Adds a subscription to the list of subscriptions that the client wishes to listen to
    ///
    /// See [MQTT Subscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901168)
    pub fn with_subscription_simple(mut self, topic_filter: String, qos: QualityOfService) -> Self {
        self.packet.subscriptions.push(SubscriptionBuilder::new(topic_filter, qos).build());
        self
    }

    /// Sets a positive integer to associate with all subscriptions in this request.
    ///
    /// Publish packets that match
    /// a subscription in this request will include this identifier in the resulting message.
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901166)
    pub fn with_subscription_identifier(mut self, subscription_identifier: u32) -> Self {
        self.packet.subscription_identifier = Some(subscription_identifier);
        self
    }

    /// Adds a user property to the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901184)
    pub fn with_user_property(mut self, property: UserProperty) -> Self {
        if let Some(user_properties) = &mut self.packet.user_properties {
            user_properties.push(property);
        } else {
            self.packet.user_properties = Some(vec!(property));
        }

        self
    }

    /// Builds a new SubscribePacket.  Consumes the builder in the process.
    pub fn build(self) -> SubscribePacket {
        self.packet
    }
}

/// Data model of an [MQTT UNSUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901187) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnsubackPacket {
    pub(crate) packet_id: u16,
    pub(crate) reason_string: Option<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
    pub(crate) reason_codes: Vec<UnsubackReasonCode>,
}

impl UnsubackPacket {

    /// Returns the list of reason codes indicating the result of unsubscribing from each individual topic filter entry in the
    /// associated UNSUBSCRIBE packet.
    ///
    /// See [MQTT5 Unsuback Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194)
    pub fn reason_codes(&self) -> &[UnsubackReasonCode] {
        self.reason_codes.as_slice()
    }

    /// Returns any additional diagnostic information about the result of the UNSUBSCRIBE attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901192)
    pub fn reason_string(&self) -> Option<&str> { self.reason_string.as_deref() }

    /// Returns the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901184)
    pub fn user_properties(&self) -> Option<&[UserProperty]> {
        self.user_properties.as_deref()
    }
}

/// Data model of an [MQTT UNSUBSCRIBE](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901179) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UnsubscribePacket {
    pub(crate) packet_id: u16,
    pub(crate) topic_filters: Vec<String>,
    pub(crate) user_properties: Option<Vec<UserProperty>>,
}

impl UnsubscribePacket {

    /// Creates a new builder for an UnsubscribePacket.
    pub fn builder() -> UnsubscribePacketBuilder {
        UnsubscribePacketBuilder::new()
    }
}

/// Builder type for UnsubscribePacket instances
pub struct UnsubscribePacketBuilder {
    packet: UnsubscribePacket
}

impl UnsubscribePacketBuilder {

    pub(crate) fn new() -> Self {
        UnsubscribePacketBuilder {
            packet: UnsubscribePacket::default()
        }
    }

    /// Adds a topic filter to the list of topic filters that the client wishes to unsubscribe from.
    ///
    /// See [MQTT Unsubscribe Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901185)
    pub fn with_topic_filter(mut self, topic_filter: String) -> Self {
        self.packet.topic_filters.push(topic_filter);
        self
    }

    /// Adds a user property to the set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901184)
    pub fn with_user_property(mut self, property: UserProperty) -> Self {
        if let Some(user_properties) = &mut self.packet.user_properties {
            user_properties.push(property);
        } else {
            self.packet.user_properties = Some(vec!(property));
        }

        self
    }

    /// Builds a new UnsubscribePacket.  Consumes the builder in the process.
    pub fn build(self) -> UnsubscribePacket {
        self.packet
    }
}


/// Algebraic union of all MQTT packet types.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "testing", derive(PartialEq, Eq))]
pub(crate) enum MqttPacket {
    Connect(ConnectPacket),
    Connack(ConnackPacket),
    Publish(PublishPacket),
    Puback(PubackPacket),
    Pubrec(PubrecPacket),
    Pubrel(PubrelPacket),
    Pubcomp(PubcompPacket),
    Subscribe(SubscribePacket),
    Suback(SubackPacket),
    Unsubscribe(UnsubscribePacket),
    Unsuback(UnsubackPacket),
    Pingreq(PingreqPacket),
    Pingresp(PingrespPacket),
    Disconnect(DisconnectPacket),
    Auth(AuthPacket),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
/// An enum indicating the kind of MQTT packet
pub enum PacketType {
    /// A [Connect](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901033) packet
    Connect,

    /// A [Connack](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901074) packet
    Connack,

    /// A [Publish](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901100) packet
    Publish,

    /// A [Puback](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121) packet
    Puback,

    /// A [Pubrec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901131) packet
    Pubrec,

    /// A [Pubrel](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901141) packet
    Pubrel,

    /// A [Pubcomp](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901151) packet
    Pubcomp,

    /// A [Subscribe](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901161) packet
    Subscribe,

    /// A [Suback](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901171) packet
    Suback,

    /// An [Unsubscribe](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901179) packet
    Unsubscribe,

    /// An [Unsuback](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901187) packet
    Unsuback,

    /// A [Pingreq](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901195) packet
    Pingreq,

    /// A [Pingresp](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901200) packet
    Pingresp,

    /// A [Disconnect](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901205) packet
    Disconnect,

    /// An [Auth](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901217) packet
    Auth,
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketType::Connect => { write!(f, "ConnectPacket") }
            PacketType::Connack => { write!(f, "ConnackPacket") }
            PacketType::Publish => { write!(f, "PublishPacket") }
            PacketType::Puback => { write!(f, "PubackPacket") }
            PacketType::Pubrec => { write!(f, "PubrecPacket") }
            PacketType::Pubrel => { write!(f, "PubrelPacket") }
            PacketType::Pubcomp => { write!(f, "PubcompPacket") }
            PacketType::Subscribe => { write!(f, "SubscribePacket") }
            PacketType::Suback => { write!(f, "SubackPacket") }
            PacketType::Unsubscribe => { write!(f, "UnsubscribePacket") }
            PacketType::Unsuback => { write!(f, "UnsubackPacket") }
            PacketType::Pingreq => { write!(f, "PingreqPacket") }
            PacketType::Pingresp => { write!(f, "PingrespPacket") }
            PacketType::Disconnect => { write!(f, "DisconnectPacket") }
            PacketType::Auth => { write!(f, "AuthPacket") }
        }
    }
}