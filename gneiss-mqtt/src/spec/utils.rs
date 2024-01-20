/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate log;

use crate::*;
use crate::spec::*;

use log::*;

pub const PACKET_TYPE_CONNECT: u8 = 1;
pub const PACKET_TYPE_CONNACK: u8 = 2;
pub const PACKET_TYPE_PUBLISH: u8 = 3;
pub const PACKET_TYPE_PUBACK: u8 = 4;
pub const PACKET_TYPE_PUBREC: u8 = 5;
pub const PACKET_TYPE_PUBREL: u8 = 6;
pub const PACKET_TYPE_PUBCOMP: u8 = 7;
pub const PACKET_TYPE_SUBSCRIBE: u8 = 8;
pub const PACKET_TYPE_SUBACK: u8 = 9;
pub const PACKET_TYPE_UNSUBSCRIBE: u8 = 10;
pub const PACKET_TYPE_UNSUBACK: u8 = 11;
pub const PACKET_TYPE_PINGREQ: u8 = 12;
pub const PACKET_TYPE_PINGRESP: u8 = 13;
pub const PACKET_TYPE_DISCONNECT: u8 = 14;
pub const PACKET_TYPE_AUTH: u8 = 15;

pub const PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR: u8 = 1;
pub const PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL: u8 = 2;
pub const PROPERTY_KEY_CONTENT_TYPE: u8 = 3;
pub const PROPERTY_KEY_RESPONSE_TOPIC: u8 = 8;
pub const PROPERTY_KEY_CORRELATION_DATA: u8 = 9;
pub const PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER: u8 = 11;
pub const PROPERTY_KEY_SESSION_EXPIRY_INTERVAL: u8 = 17;
pub const PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER: u8 = 18;
pub const PROPERTY_KEY_SERVER_KEEP_ALIVE: u8 = 19;
pub const PROPERTY_KEY_AUTHENTICATION_METHOD: u8 = 21;
pub const PROPERTY_KEY_AUTHENTICATION_DATA: u8 = 22;
pub const PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION: u8 = 23;
pub const PROPERTY_KEY_WILL_DELAY_INTERVAL: u8 = 24;
pub const PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION: u8 = 25;
pub const PROPERTY_KEY_RESPONSE_INFORMATION: u8 = 26;
pub const PROPERTY_KEY_SERVER_REFERENCE: u8 = 28;
pub const PROPERTY_KEY_REASON_STRING: u8 = 31;
pub const PROPERTY_KEY_RECEIVE_MAXIMUM: u8 = 33;
pub const PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM: u8 = 34;
pub const PROPERTY_KEY_TOPIC_ALIAS: u8 = 35;
pub const PROPERTY_KEY_MAXIMUM_QOS: u8 = 36;
pub const PROPERTY_KEY_RETAIN_AVAILABLE: u8 = 37;
pub const PROPERTY_KEY_USER_PROPERTY: u8 = 38;
pub const PROPERTY_KEY_MAXIMUM_PACKET_SIZE: u8 = 39;
pub const PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE: u8 = 40;
pub const PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE: u8 = 41;
pub const PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE: u8 = 42;

pub const PUBLISH_PACKET_FIXED_HEADER_DUPLICATE_FLAG : u8 = 8;
pub const PUBLISH_PACKET_FIXED_HEADER_RETAIN_FLAG : u8 = 1;
pub const QOS_MASK : u8 = 3;

pub const CONNECT_PACKET_CLEAN_START_FLAG_MASK : u8 = 1 << 1;
pub const CONNECT_PACKET_HAS_WILL_FLAG_MASK : u8 = 1 << 2;
pub const CONNECT_PACKET_WILL_RETAIN_FLAG_MASK : u8 = 1 << 5;
pub const CONNECT_PACKET_WILL_QOS_FLAG_SHIFT : u8 = 3;
pub const CONNECT_PACKET_HAS_USERNAME_FLAG_MASK : u8 = 1 << 7;
pub const CONNECT_PACKET_HAS_PASSWORD_FLAG_MASK : u8 = 1 << 6;

pub const UNSUBSCRIBE_FIRST_BYTE : u8 = (PACKET_TYPE_UNSUBSCRIBE << 4) | (0x02u8);
pub const UNSUBACK_FIRST_BYTE : u8 = PACKET_TYPE_UNSUBACK << 4;
pub const SUBSCRIBE_FIRST_BYTE : u8 = (PACKET_TYPE_SUBSCRIBE << 4) | (0x02u8);
pub const SUBACK_FIRST_BYTE : u8 = PACKET_TYPE_SUBACK << 4;
pub const PUBREL_FIRST_BYTE : u8 = (PACKET_TYPE_PUBREL << 4) | (0x02u8);
pub const PUBACK_FIRST_BYTE : u8 = PACKET_TYPE_PUBACK << 4;
pub const PUBREC_FIRST_BYTE : u8 = PACKET_TYPE_PUBREC << 4;
pub const PUBCOMP_FIRST_BYTE : u8 = PACKET_TYPE_PUBCOMP << 4;

pub const SUBSCRIPTION_OPTIONS_NO_LOCAL_MASK : u8 = 1u8 << 2;
pub const SUBSCRIPTION_OPTIONS_RETAIN_AS_PUBLISHED_MASK : u8 = 1u8 << 3;
pub const SUBSCRIPTION_OPTIONS_RETAIN_HANDLING_SHIFT : u8 = 4;

/// Converts an integer to a modeled QualityOfService value
pub fn convert_u8_to_quality_of_service(value: u8) -> MqttResult<QualityOfService> {
    match value {
        0 => { Ok(QualityOfService::AtMostOnce) }
        1 => { Ok(QualityOfService::AtLeastOnce) }
        2 => { Ok(QualityOfService::ExactlyOnce) }
        _ => {
            error!("Packet Decode - Invalid quality of service value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn quality_of_service_to_str (qos: QualityOfService) -> &'static str {
    match qos {
        QualityOfService::AtMostOnce => { "0 (AtMostOnce)" }
        QualityOfService::AtLeastOnce => { "1 (AtLeastOnce)" }
        QualityOfService::ExactlyOnce => { "2 (ExactlyOnce)" }
    }
}

pub fn convert_u8_to_payload_format_indicator(value: u8) -> MqttResult<PayloadFormatIndicator> {
    match value {
        0 => { Ok(PayloadFormatIndicator::Bytes) }
        1 => { Ok(PayloadFormatIndicator::Utf8) }
        _ => {
            error!("Packet Decode - Invalid payload format indicator value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn payload_format_indicator_to_str (pfi: PayloadFormatIndicator) -> &'static str {
    match pfi {
        PayloadFormatIndicator::Bytes => { "0 (Bytes)" }
        PayloadFormatIndicator::Utf8 => { "1 (Utf8)" }
    }
}

pub(crate) fn convert_u8_to_puback_reason_code(value: u8) -> MqttResult<PubackReasonCode> {
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
            error!("Packet Decode - Invalid puback reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn puback_reason_code_to_str (reason_code: PubackReasonCode) -> &'static str {
    match reason_code {
        PubackReasonCode::Success => { "0 (Success)" }
        PubackReasonCode::NoMatchingSubscribers => { "16 (NoMatchingSubscribers)" }
        PubackReasonCode::UnspecifiedError => { "128 (UnspecifiedError)" }
        PubackReasonCode::ImplementationSpecificError => { "131 (ImplementationSpecificError)" }
        PubackReasonCode::NotAuthorized => { "135 (NotAuthorized)" }
        PubackReasonCode::TopicNameInvalid => { "144 (TopicNameInvalid)" }
        PubackReasonCode::PacketIdentifierInUse => { "145 (PacketIdentifierInUse)" }
        PubackReasonCode::QuotaExceeded => { "151 (QuotaExceeded)" }
        PubackReasonCode::PayloadFormatInvalid => { "153 (PayloadFormatInvalid)" }
    }
}

pub(crate) fn convert_u8_to_pubrec_reason_code(value: u8) -> MqttResult<PubrecReasonCode> {
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
            error!("Packet Decode - Invalid pubrec reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn pubrec_reason_code_to_str (reason_code: PubrecReasonCode) -> &'static str {
    match reason_code {
        PubrecReasonCode::Success => { "0 (Success)" }
        PubrecReasonCode::NoMatchingSubscribers => { "16 (NoMatchingSubscribers)" }
        PubrecReasonCode::UnspecifiedError => { "128 (UnspecifiedError)" }
        PubrecReasonCode::ImplementationSpecificError => { "131 (ImplementationSpecificError)" }
        PubrecReasonCode::NotAuthorized => { "135 (NotAuthorized)" }
        PubrecReasonCode::TopicNameInvalid => { "144 (TopicNameInvalid)" }
        PubrecReasonCode::PacketIdentifierInUse => { "145 (PacketIdentifierInUse)" }
        PubrecReasonCode::QuotaExceeded => { "151 (QuotaExceeded)" }
        PubrecReasonCode::PayloadFormatInvalid => { "153 (PayloadFormatInvalid)" }
    }
}

pub(crate) fn convert_u8_to_pubrel_reason_code(value: u8) -> MqttResult<PubrelReasonCode> {
    match value {
        0 => { Ok(PubrelReasonCode::Success) }
        146 => { Ok(PubrelReasonCode::PacketIdentifierNotFound) }
        _ => {
            error!("Packet Decode - Invalid pubrel reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn pubrel_reason_code_to_str (reason_code: PubrelReasonCode) -> &'static str {
    match reason_code {
        PubrelReasonCode::Success => { "0 (Success)" }
        PubrelReasonCode::PacketIdentifierNotFound => { "146 (PacketIdentifierNotFound)" }
    }
}

pub(crate) fn convert_u8_to_pubcomp_reason_code(value: u8) -> MqttResult<PubcompReasonCode> {
    match value {
        0 => { Ok(PubcompReasonCode::Success) }
        146 => { Ok(PubcompReasonCode::PacketIdentifierNotFound) }
        _ => {
            error!("Packet Decode - Invalid pubcomp reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn pubcomp_reason_code_to_str (reason_code: PubcompReasonCode) -> &'static str {
    match reason_code {
        PubcompReasonCode::Success => { "0 (Success)" }
        PubcompReasonCode::PacketIdentifierNotFound => { "146 (PacketIdentifierNotFound)" }
    }
}

pub(crate) fn convert_u8_to_connect_reason_code(value: u8) -> MqttResult<ConnectReasonCode> {
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
            error!("Packet Decode - Invalid connect reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn connect_reason_code_to_str (reason_code: ConnectReasonCode) -> &'static str {
    match reason_code {
        ConnectReasonCode::Success => { "0 (Success)" }
        ConnectReasonCode::UnspecifiedError => { "128 (UnspecifiedError)" }
        ConnectReasonCode::MalformedPacket => { "129 (MalformedPacket)" }
        ConnectReasonCode::ProtocolError => { "130 (ProtocolError)" }
        ConnectReasonCode::ImplementationSpecificError => { "131 (ImplementationSpecificError)" }
        ConnectReasonCode::UnsupportedProtocolVersion => { "132 (UnsupportedProtocolVersion)" }
        ConnectReasonCode::ClientIdentifierNotValid => { "133 (ClientIdentifierNotValid)" }
        ConnectReasonCode::BadUsernameOrPassword => { "134 (BadUsernameOrPassword)" }
        ConnectReasonCode::NotAuthorized => { "135 (NotAuthorized)" }
        ConnectReasonCode::ServerUnavailable => { "136 (ServerUnavailable)" }
        ConnectReasonCode::ServerBusy => { "137 (ServerBusy)" }
        ConnectReasonCode::Banned => { "138 (Banned)" }
        ConnectReasonCode::BadAuthenticationMethod => { "140 (BadAuthenticationMethod)" }
        ConnectReasonCode::TopicNameInvalid => { "144 (TopicNameInvalid)" }
        ConnectReasonCode::PacketTooLarge => { "149 (PacketTooLarge)" }
        ConnectReasonCode::QuotaExceeded => { "151 (QuotaExceeded)" }
        ConnectReasonCode::PayloadFormatInvalid => { "153 (PayloadFormatInvalid)" }
        ConnectReasonCode::RetainNotSupported => { "154 (RetainNotSupported)" }
        ConnectReasonCode::QosNotSupported => { "155 (QosNotSupported)" }
        ConnectReasonCode::UseAnotherServer => { "156 (UseAnotherServer)" }
        ConnectReasonCode::ServerMoved => { "157 (ServerMoved)" }
        ConnectReasonCode::ConnectionRateExceeded => { "159 (ConnectionRateExceeded)" }
    }
}

/// Converts an integer to a modeled DisconnectReasonCode value
pub fn convert_u8_to_disconnect_reason_code(value: u8) -> MqttResult<DisconnectReasonCode> {
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
            error!("Packet Decode - Invalid disconnect reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn disconnect_reason_code_to_str (reason_code: DisconnectReasonCode) -> &'static str {
    match reason_code {
        DisconnectReasonCode::NormalDisconnection => { "0 (NormalDisconnection)" }
        DisconnectReasonCode::DisconnectWithWillMessage => { "4 (DisconnectWithWillMessage)" }
        DisconnectReasonCode::UnspecifiedError => { "128 (UnspecifiedError)" }
        DisconnectReasonCode::MalformedPacket => { "129 (MalformedPacket)" }
        DisconnectReasonCode::ProtocolError => { "130 (ProtocolError)" }
        DisconnectReasonCode::ImplementationSpecificError => { "131 (ImplementationSpecificError)" }
        DisconnectReasonCode::NotAuthorized => { "135 (NotAuthorized)" }
        DisconnectReasonCode::ServerBusy => { "137 (ServerBusy)" }
        DisconnectReasonCode::ServerShuttingDown => { "139 (ServerShuttingDown)" }
        DisconnectReasonCode::KeepAliveTimeout => { "141 (KeepAliveTimeout)" }
        DisconnectReasonCode::SessionTakenOver => { "142 (SessionTakenOver)" }
        DisconnectReasonCode::TopicFilterInvalid => { "143 (TopicFilterInvalid)" }
        DisconnectReasonCode::TopicNameInvalid => { "144 (TopicNameInvalid)" }
        DisconnectReasonCode::ReceiveMaximumExceeded => { "147 (ReceiveMaximumExceeded)" }
        DisconnectReasonCode::TopicAliasInvalid => { "148 (TopicAliasInvalid)" }
        DisconnectReasonCode::PacketTooLarge => { "149 (PacketTooLarge)" }
        DisconnectReasonCode::MessageRateTooHigh => { "150 (MessageRateTooHigh)" }
        DisconnectReasonCode::QuotaExceeded => { "151 (QuotaExceeded)" }
        DisconnectReasonCode::AdministrativeAction => { "152 (AdministrativeAction)" }
        DisconnectReasonCode::PayloadFormatInvalid => { "153 (PayloadFormatInvalid)" }
        DisconnectReasonCode::RetainNotSupported => { "154 (RetainNotSupported)" }
        DisconnectReasonCode::QosNotSupported => { "155 (QosNotSupported)" }
        DisconnectReasonCode::UseAnotherServer => { "156 (UseAnotherServer)" }
        DisconnectReasonCode::ServerMoved => { "157 (ServerMoved)" }
        DisconnectReasonCode::SharedSubscriptionsNotSupported => { "158 (SharedSubscriptionsNotSupported)" }
        DisconnectReasonCode::ConnectionRateExceeded => { "159 (ConnectionRateExceeded)" }
        DisconnectReasonCode::MaximumConnectTime => { "160 (MaximumConnectTime)" }
        DisconnectReasonCode::SubscriptionIdentifiersNotSupported => { "161 (SubscriptionIdentifiersNotSupported)" }
        DisconnectReasonCode::WildcardSubscriptionsNotSupported => { "162 (WildcardSubscriptionsNotSupported)" }
    }
}

pub fn convert_u8_to_authenticate_reason_code(value: u8) -> MqttResult<AuthenticateReasonCode> {
    match value {
        0 => { Ok(AuthenticateReasonCode::Success) }
        24 => { Ok(AuthenticateReasonCode::ContinueAuthentication) }
        25 => { Ok(AuthenticateReasonCode::ReAuthenticate) }
        _ => {
            error!("Packet Decode - Invalid authenticate reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn authenticate_reason_code_to_str (reason_code: AuthenticateReasonCode) -> &'static str {
    match reason_code {
        AuthenticateReasonCode::Success => { "0 (Success)" }
        AuthenticateReasonCode::ContinueAuthentication => { "24 (ContinueAuthentication)" }
        AuthenticateReasonCode::ReAuthenticate => { "25 (ReAuthenticate)" }
    }
}

pub(crate) fn convert_u8_to_unsuback_reason_code(value: u8) -> MqttResult<UnsubackReasonCode> {
    match value {
        0 => { Ok(UnsubackReasonCode::Success) }
        17 => { Ok(UnsubackReasonCode::NoSubscriptionExisted) }
        128 => { Ok(UnsubackReasonCode::UnspecifiedError) }
        131 => { Ok(UnsubackReasonCode::ImplementationSpecificError) }
        135 => { Ok(UnsubackReasonCode::NotAuthorized) }
        144 => { Ok(UnsubackReasonCode::TopicNameInvalid) }
        145 => { Ok(UnsubackReasonCode::PacketIdentifierInUse) }
        _ => {
            error!("Packet Decode - Invalid unsuback reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn unsuback_reason_code_to_str (reason_code: UnsubackReasonCode) -> &'static str {
    match reason_code {
        UnsubackReasonCode::Success => { "0 (Success)" }
        UnsubackReasonCode::NoSubscriptionExisted => { "17 (NoSubscriptionExisted)" }
        UnsubackReasonCode::UnspecifiedError => { "128 (UnspecifiedError)" }
        UnsubackReasonCode::ImplementationSpecificError => { "131 (ImplementationSpecificError)" }
        UnsubackReasonCode::NotAuthorized => { "135 (NotAuthorized)" }
        UnsubackReasonCode::TopicNameInvalid => { "144 (TopicNameInvalid)" }
        UnsubackReasonCode::PacketIdentifierInUse => { "145 (PacketIdentifierInUse)" }
    }
}

pub(crate) fn convert_u8_to_suback_reason_code(value: u8) -> MqttResult<SubackReasonCode> {
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
            error!("Packet Decode - Invalid suback reason code value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn suback_reason_code_to_str (reason_code: SubackReasonCode) -> &'static str {
    match reason_code {
        SubackReasonCode::GrantedQos0 => { "0 (GrantedQos0)" }
        SubackReasonCode::GrantedQos1 => { "1 (GrantedQos1)" }
        SubackReasonCode::GrantedQos2 => { "2 (GrantedQos2)" }
        SubackReasonCode::UnspecifiedError => { "128 (UnspecifiedError)" }
        SubackReasonCode::ImplementationSpecificError => { "131 (ImplementationSpecificError)" }
        SubackReasonCode::NotAuthorized => { "135 (NotAuthorized)" }
        SubackReasonCode::TopicFilterInvalid => { "143 (TopicFilterInvalid)" }
        SubackReasonCode::PacketIdentifierInUse => { "145 (PacketIdentifierInUse)" }
        SubackReasonCode::QuotaExceeded => { "151 (QuotaExceeded)" }
        SubackReasonCode::SharedSubscriptionsNotSupported => { "158 (SharedSubscriptionsNotSupported)" }
        SubackReasonCode::SubscriptionIdentifiersNotSupported => { "161 (SubscriptionIdentifiersNotSupported)" }
        SubackReasonCode::WildcardSubscriptionsNotSupported => { "162 (WildcardSubscriptionsNotSupported)" }
    }
}

pub fn convert_u8_to_retain_handling_type(value: u8) -> MqttResult<RetainHandlingType> {
    match value {
        0 => { Ok(RetainHandlingType::SendOnSubscribe) }
        1 => { Ok(RetainHandlingType::SendOnSubscribeIfNew) }
        2 => { Ok(RetainHandlingType::DontSend) }
        _ => {
            error!("Packet Decode - Invalid retain handling type value ({})", value);
            Err(MqttError::MalformedPacket)
        }
    }
}

pub(crate) fn retain_handling_type_to_str (rht: RetainHandlingType) -> &'static str {
    match rht {
        RetainHandlingType::SendOnSubscribe => { "0 (SendOnSubscribe)" }
        RetainHandlingType::SendOnSubscribeIfNew => { "1 (SendOnSubscribeIfNew)" }
        RetainHandlingType::DontSend => { "2 (DontSend)" }
    }
}

pub(crate) fn mqtt_packet_to_packet_type(packet: &MqttPacket) -> PacketType {
    match packet {
        MqttPacket::Connect(_) => { PacketType::Connect }
        MqttPacket::Connack(_) => { PacketType::Connack }
        MqttPacket::Publish(_) => { PacketType::Publish}
        MqttPacket::Puback(_) => { PacketType::Puback }
        MqttPacket::Pubrec(_) => { PacketType::Pubrec }
        MqttPacket::Pubrel(_) => { PacketType::Pubrel }
        MqttPacket::Pubcomp(_) => { PacketType::Pubcomp }
        MqttPacket::Subscribe(_) => { PacketType::Subscribe }
        MqttPacket::Suback(_) => { PacketType::Suback }
        MqttPacket::Unsubscribe(_) => { PacketType::Unsubscribe }
        MqttPacket::Unsuback(_) => { PacketType::Unsuback }
        MqttPacket::Pingreq(_) => { PacketType::Pingreq }
        MqttPacket::Pingresp(_) => { PacketType::Pingresp }
        MqttPacket::Disconnect(_) => { PacketType::Disconnect }
        MqttPacket::Auth(_) => { PacketType::Auth }
    }
}

pub(crate) fn packet_type_to_str(packet_type: u8) -> &'static str {
    match packet_type {
        PACKET_TYPE_CONNECT => { "Connect" }
        PACKET_TYPE_CONNACK => { "Connack" }
        PACKET_TYPE_PUBLISH => { "Publish" }
        PACKET_TYPE_PUBACK => { "Puback" }
        PACKET_TYPE_PUBREC => { "Pubrec" }
        PACKET_TYPE_PUBREL => { "Pubrel" }
        PACKET_TYPE_PUBCOMP => { "Pubcomp" }
        PACKET_TYPE_SUBSCRIBE => { "Subscribe" }
        PACKET_TYPE_SUBACK => { "Suback" }
        PACKET_TYPE_UNSUBSCRIBE => { "Unsubscribe" }
        PACKET_TYPE_UNSUBACK => { "Unsuback" }
        PACKET_TYPE_PINGREQ => { "Pingreq" }
        PACKET_TYPE_PINGRESP => { "Pingresp" }
        PACKET_TYPE_DISCONNECT => { "Disconnect" }
        PACKET_TYPE_AUTH => { "Auth" }
        _ => {
            "Unknown"
        }
    }
}

pub(crate) fn mqtt_packet_to_str(packet: &MqttPacket) -> &'static str {
    match packet {
        MqttPacket::Connect(_) => { "CONNECT" }
        MqttPacket::Connack(_) => { "CONNACK" }
        MqttPacket::Publish(_) => { "PUBLISH" }
        MqttPacket::Puback(_) => { "PUBACK" }
        MqttPacket::Pubrec(_) => { "PUBREC" }
        MqttPacket::Pubrel(_) => { "PUBREL" }
        MqttPacket::Pubcomp(_) => { "PUBCOMP" }
        MqttPacket::Subscribe(_) => { "SUBSCRIBE" }
        MqttPacket::Suback(_) => { "SUBACK" }
        MqttPacket::Unsubscribe(_) => { "UNSUBSCRIBE" }
        MqttPacket::Unsuback(_) => { "UNSUBACK" }
        MqttPacket::Pingreq(_) => { "PINGREQ" }
        MqttPacket::Pingresp(_) => { "PINGRESP" }
        MqttPacket::Disconnect(_) => { "DISCONNECT" }
        MqttPacket::Auth(_) => { "AUTH" }
    }
}