/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate log;

use crate::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::logging::*;
use crate::spec::*;
use crate::spec::utils::*;
use crate::validate::utils::*;

use log::*;
use std::collections::VecDeque;
use std::fmt;

/// Data model of an [MQTT5 CONNACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901074) packet.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnackPacket {

    /// True if the client rejoined an existing session on the server, false otherwise.
    ///
    /// See [MQTT5 Session Present](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901078)
    pub session_present: bool,

    /// Indicates either success or the reason for failure for the connection attempt.
    ///
    /// See [MQTT5 Connect Reason Code](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079)
    pub reason_code: ConnectReasonCode,

    /// A time interval, in seconds, that the server will persist this connection's MQTT session state
    /// for.  If present, this value overrides any session expiry specified in the preceding CONNECT packet.
    ///
    /// See [MQTT5 Session Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901082)
    pub session_expiry_interval: Option<u32>,

    /// The maximum amount of in-flight QoS 1 or 2 messages that the server is willing to handle at once.  If omitted,
    /// the limit is based on the valid MQTT packet id space (65535).
    ///
    /// See [MQTT5 Receive Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901083)
    pub receive_maximum: Option<u16>,

    /// The maximum message delivery quality of service that the server will allow on this connection.
    ///
    /// See [MQTT5 Maximum QoS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901084)
    pub maximum_qos: Option<QualityOfService>,

    /// Indicates whether the server supports retained messages.  If undefined, retained messages are
    /// supported.
    ///
    /// See [MQTT5 Retain Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901085)
    pub retain_available: Option<bool>,

    /// Specifies the maximum packet size, in bytes, that the server is willing to accept.  If undefined, there
    /// is no limit beyond what is imposed by the MQTT spec itself.
    ///
    /// See [MQTT5 Maximum Packet Size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086)
    pub maximum_packet_size: Option<u32>,

    /// Specifies a client identifier assigned to this connection by the server.  Only valid when the client id of
    /// the preceding CONNECT packet was left empty.
    ///
    /// See [MQTT5 Assigned Client Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901087)
    pub assigned_client_identifier: Option<String>,

    /// Specifies the maximum topic alias value that the server will accept from the client.
    ///
    /// See [MQTT5 Topic Alias Maximum](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901088)
    pub topic_alias_maximum: Option<u16>,

    /// Additional diagnostic information about the result of the connection attempt.
    ///
    /// See [MQTT5 Reason String](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901089)
    pub reason_string: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901090)
    pub user_properties: Option<Vec<UserProperty>>,

    /// Indicates whether the server supports wildcard subscriptions.  If undefined, wildcard subscriptions
    /// are supported.
    ///
    /// See [MQTT5 Wildcard Subscriptions Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091)
    pub wildcard_subscriptions_available: Option<bool>,

    /// Indicates whether the server supports subscription identifiers.  If undefined, subscription identifiers
    /// are supported.
    ///
    /// See [MQTT5 Subscription Identifiers Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092)
    pub subscription_identifiers_available: Option<bool>,

    /// Indicates whether the server supports shared subscription topic filters.  If undefined, shared subscriptions
    /// are supported.
    ///
    /// See [MQTT5 Shared Subscriptions Available](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901093)
    pub shared_subscriptions_available: Option<bool>,

    /// Server-requested override of the keep alive interval, in seconds.  If undefined, the keep alive value sent
    /// by the client should be used.
    ///
    /// See [MQTT5 Server Keep Alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094)
    pub server_keep_alive: Option<u16>,

    /// A value that can be used in the creation of a response topic associated with this connection.  MQTT5-based
    /// request/response is outside the purview of the MQTT5 spec and this client.
    ///
    /// See [MQTT5 Response Information](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901095)
    pub response_information: Option<String>,

    /// Property indicating an alternate server that the client may temporarily or permanently attempt
    /// to connect to instead of the configured endpoint.  Will only be set if the reason code indicates another
    /// server may be used (ServerMoved, UseAnotherServer).
    ///
    /// See [MQTT5 Server Reference](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901096)
    pub server_reference: Option<String>,

    /// Authentication method used in the authentication exchange that led to this CONNACK packet being sent.
    ///
    /// See [MQTT5 Authentication Method](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901097)
    pub authentication_method: Option<String>,

    /// Authentication method specific binary data associated with the authentication exchange that led to this
    /// CONNACK packet being sent.
    ///
    /// Developer Note: It is likely that this field is only relevant in authentication exchanges that *DO NOT*
    /// need AUTH packets to reach a successful conclusion, otherwise the final server->client authentication
    /// data would have been sent with the final server->client AUTH packet that included the success reason code.
    ///
    /// See [MQTT5 Authentication Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901098)
    pub authentication_data: Option<Vec<u8>>,
}

#[rustfmt::skip]
fn compute_connack_packet_length_properties(packet: &ConnackPacket) -> Mqtt5Result<(u32, u32)> {

    let mut connack_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u32_property_length!(connack_property_section_length, packet.session_expiry_interval);
    add_optional_u16_property_length!(connack_property_section_length, packet.receive_maximum);
    add_optional_u8_property_length!(connack_property_section_length, packet.maximum_qos);
    add_optional_u8_property_length!(connack_property_section_length, packet.retain_available);
    add_optional_u32_property_length!(connack_property_section_length, packet.maximum_packet_size);
    add_optional_string_property_length!(connack_property_section_length, packet.assigned_client_identifier);
    add_optional_u16_property_length!(connack_property_section_length, packet.topic_alias_maximum);
    add_optional_string_property_length!(connack_property_section_length, packet.reason_string);
    add_optional_u8_property_length!(connack_property_section_length, packet.wildcard_subscriptions_available);
    add_optional_u8_property_length!(connack_property_section_length, packet.subscription_identifiers_available);
    add_optional_u8_property_length!(connack_property_section_length, packet.shared_subscriptions_available);
    add_optional_u16_property_length!(connack_property_section_length, packet.server_keep_alive);
    add_optional_string_property_length!(connack_property_section_length, packet.response_information);
    add_optional_string_property_length!(connack_property_section_length, packet.server_reference);
    add_optional_string_property_length!(connack_property_section_length, packet.authentication_method);
    add_optional_bytes_property_length!(connack_property_section_length, packet.authentication_data);

    let mut total_remaining_length : usize = compute_variable_length_integer_encode_size(connack_property_section_length)?;

    total_remaining_length += 2;
    total_remaining_length += connack_property_section_length;

    Ok((total_remaining_length as u32, connack_property_section_length as u32))
}

fn get_connack_packet_assigned_client_identifier(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, assigned_client_identifier)
}

fn get_connack_packet_reason_string(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, reason_string)
}

fn get_connack_packet_response_information(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, response_information)
}

fn get_connack_packet_server_reference(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, server_reference)
}

fn get_connack_packet_authentication_method(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Connack, authentication_method)
}

fn get_connack_packet_authentication_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Connack, authentication_data)
}

fn get_connack_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Connack(connack) = packet {
        if let Some(properties) = &connack.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

#[rustfmt::skip]
pub(crate) fn write_connack_encoding_steps(packet: &ConnackPacket, _: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {
    let (total_remaining_length, connack_property_length) = compute_connack_packet_length_properties(packet)?;

    encode_integral_expression!(steps, Uint8, PACKET_TYPE_CONNACK << 4);
    encode_integral_expression!(steps, Vli, total_remaining_length);

    /*
     * Variable Header
     * 1 byte flags
     * 1 byte reason code
     * 1-4 byte Property Length as Variable Byte Integer
     * n bytes Properties
     */
    encode_integral_expression!(steps, Uint8, if packet.session_present { 1 } else { 0 });
    encode_enum!(steps, Uint8, u8, packet.reason_code);
    encode_integral_expression!(steps, Vli, connack_property_length);

    encode_optional_property!(steps, Uint32, PROPERTY_KEY_SESSION_EXPIRY_INTERVAL, packet.session_expiry_interval);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_RECEIVE_MAXIMUM, packet.receive_maximum);
    encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_MAXIMUM_QOS, u8, packet.maximum_qos);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_RETAIN_AVAILABLE, packet.retain_available);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_MAXIMUM_PACKET_SIZE, packet.maximum_packet_size);
    encode_optional_string_property!(steps, get_connack_packet_assigned_client_identifier, PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER, packet.assigned_client_identifier);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM, packet.topic_alias_maximum);
    encode_optional_string_property!(steps, get_connack_packet_reason_string, PROPERTY_KEY_REASON_STRING, packet.reason_string);
    encode_user_properties!(steps, get_connack_packet_user_property, packet.user_properties);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE, packet.wildcard_subscriptions_available);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE, packet.subscription_identifiers_available);
    encode_optional_boolean_property!(steps, PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE, packet.shared_subscriptions_available);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_SERVER_KEEP_ALIVE, packet.server_keep_alive);
    encode_optional_string_property!(steps, get_connack_packet_response_information, PROPERTY_KEY_RESPONSE_INFORMATION, packet.response_information);
    encode_optional_string_property!(steps, get_connack_packet_server_reference, PROPERTY_KEY_SERVER_REFERENCE, packet.server_reference);
    encode_optional_string_property!(steps, get_connack_packet_authentication_method, PROPERTY_KEY_AUTHENTICATION_METHOD, packet.authentication_method);
    encode_optional_bytes_property!(steps, get_connack_packet_authentication_data, PROPERTY_KEY_AUTHENTICATION_DATA, packet.authentication_data);

    Ok(())
}


fn decode_connack_properties(property_bytes: &[u8], packet : &mut ConnackPacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_SESSION_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.session_expiry_interval)?; }
            PROPERTY_KEY_RECEIVE_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.receive_maximum)?; }
            PROPERTY_KEY_MAXIMUM_QOS => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut packet.maximum_qos, convert_u8_to_quality_of_service)?; }
            PROPERTY_KEY_RETAIN_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.retain_available)?; }
            PROPERTY_KEY_MAXIMUM_PACKET_SIZE => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.maximum_packet_size)?; }
            PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.assigned_client_identifier)?; }
            PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias_maximum)?; }
            PROPERTY_KEY_REASON_STRING => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.reason_string)?; }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.wildcard_subscriptions_available)?; }
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.subscription_identifiers_available)?; }
            PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE => { mutable_property_bytes = decode_optional_u8_as_bool(mutable_property_bytes, &mut packet.shared_subscriptions_available)?; }
            PROPERTY_KEY_SERVER_KEEP_ALIVE => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.server_keep_alive)?; }
            PROPERTY_KEY_RESPONSE_INFORMATION => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.response_information)?; }
            PROPERTY_KEY_SERVER_REFERENCE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.server_reference)?; }
            PROPERTY_KEY_AUTHENTICATION_METHOD => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.authentication_method)?; }
            PROPERTY_KEY_AUTHENTICATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.authentication_data)?; }
            _ => {
                error!("ConnackPacket Decode - Invalid property type ({})", property_key);
                return Err(Mqtt5Error::MalformedPacket);
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_connack_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<MqttPacket>> {

    if first_byte != (PACKET_TYPE_CONNACK << 4) {
        error!("ConnackPacket Decode - invalid first byte");
        return Err(Mqtt5Error::MalformedPacket);
    }

    let mut box_packet = Box::new(MqttPacket::Connack(ConnackPacket { ..Default::default() }));

    if let MqttPacket::Connack(packet) = box_packet.as_mut() {
        let mut mutable_body = packet_body;
        if mutable_body.is_empty() {
            error!("ConnackPacket Decode - packet too short");
            return Err(Mqtt5Error::MalformedPacket);
        }

        let flags: u8 = mutable_body[0];
        mutable_body = &mutable_body[1..];

        if flags == 1 {
            packet.session_present = true;
        } else if flags != 0 {
            error!("ConnackPacket Decode - invalid value for session_present field");
            return Err(Mqtt5Error::MalformedPacket);
        }

        mutable_body = decode_u8_as_enum(mutable_body, &mut packet.reason_code, convert_u8_to_connect_reason_code)?;

        let mut properties_length: usize = 0;
        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length != mutable_body.len() {
            error!("ConnackPacket Decode - property length does not match expected overall packet length");
            return Err(Mqtt5Error::MalformedPacket);
        }

        decode_connack_properties(mutable_body, packet)?;

        return Ok(box_packet);
    }

    panic!("ConnackPacket Decode - Internal error");
}

pub(crate) fn validate_connack_packet_inbound_internal(packet: &ConnackPacket) -> Mqtt5Result<()> {

    if packet.session_present && packet.reason_code != ConnectReasonCode::Success {
        error!("ConnackPacket Inbound Validation - session present on unsuccessful connect");
        return Err(Mqtt5Error::ConnackPacketValidation);
    }

    validate_optional_integer_non_zero!(receive_maximum, packet.receive_maximum, ConnackPacketValidation, "Connack", "receive_maximum");

    if let Some(maximum_qos) = packet.maximum_qos {
        if maximum_qos == QualityOfService::ExactlyOnce {
            error!("ConnackPacket Inbound Validation - maximum qos should never be Qos2");
            return Err(Mqtt5Error::ConnackPacketValidation);
        }
    }

    validate_optional_integer_non_zero!(maximum_packet_size, packet.maximum_packet_size, ConnackPacketValidation, "Connack", "maximum_packet_size");

    Ok(())
}

impl fmt::Display for ConnackPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "ConnackPacket {{")?;
        log_primitive_value!(self.session_present, f, "session_present");
        log_enum!(self.reason_code, f, "reason_code", connect_reason_code_to_str);
        log_optional_primitive_value!(self.session_expiry_interval, f, "session_expiry_interval", value);
        log_optional_primitive_value!(self.receive_maximum, f, "receive_maximum", value);
        log_optional_enum!(self.maximum_qos, f, "maximum_qos", value, quality_of_service_to_str);
        log_optional_primitive_value!(self.retain_available, f, "retain_available", value);
        log_optional_primitive_value!(self.maximum_packet_size, f, "maximum_packet_size", value);
        log_optional_string!(self.assigned_client_identifier, f, "assigned_client_identifier", value);
        log_optional_primitive_value!(self.topic_alias_maximum, f, "topic_alias_maximum", value);
        log_optional_string!(self.reason_string, f, "reason_string", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        log_optional_primitive_value!(self.wildcard_subscriptions_available, f, "wildcard_subscriptions_available", value);
        log_optional_primitive_value!(self.subscription_identifiers_available, f, "subscription_identifiers_available", value);
        log_optional_primitive_value!(self.shared_subscriptions_available, f, "shared_subscriptions_available", value);
        log_optional_primitive_value!(self.server_keep_alive, f, "server_keep_alive", value);
        log_optional_primitive_value!(self.response_information, f, "response_information", value);
        log_optional_string!(self.server_reference, f, "server_reference", value);
        log_optional_string!(self.authentication_method, f, "authentication_method", value);
        log_optional_binary_data_sensitive!(self.authentication_data, f, "authentication_data");
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::testing::*;
    use crate::validate::*;

    #[test]
    fn connack_round_trip_encode_decode_default() {
        let packet = ConnackPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connack(packet)));
    }

    #[test]
    fn connack_round_trip_encode_decode_required() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Banned,
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connack(packet)));
    }

    fn create_all_properties_connack_packet() -> ConnackPacket {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,

            session_expiry_interval: Some(7200),
            receive_maximum: Some(200),
            maximum_qos: Some(QualityOfService::AtLeastOnce),
            retain_available: Some(true),
            maximum_packet_size: Some(256 * 1024),
            assigned_client_identifier: Some("I dub thee Stinky".to_string()),
            topic_alias_maximum: Some(30),
            reason_string: Some("You're sketchy.".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "Go".to_string(), value: "Away".to_string()},
                UserProperty{name: "".to_string(), value: "Uff da".to_string()},
            )),
            wildcard_subscriptions_available: Some(true),
            subscription_identifiers_available:Some(false),
            shared_subscriptions_available: Some(true),
            server_keep_alive: Some(1600),
            response_information: Some("We/care/a/lot".to_string()),
            server_reference: Some("lolcats.com".to_string()),
            authentication_method: Some("Sekrit".to_string()),
            authentication_data: Some("TopSekrit".as_bytes().to_vec()),
            ..Default::default()
        };

        packet
    }

    #[test]
    fn connack_round_trip_encode_decode_all() {
        let packet = create_all_properties_connack_packet();

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Connack(packet)));
    }

    #[test]
    fn connack_decode_failure_bad_fixed_header() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Banned,
            ..Default::default()
        };

        do_fixed_header_flag_decode_failure_test(&MqttPacket::Connack(packet), 5);
    }

    #[test]
    fn connack_decode_failure_bad_variable_header_flags() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Banned,
            ..Default::default()
        };

        let corrupt_variable_header_flags = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for this packet, the flags are byte 2
            clone[2] |= 8;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), corrupt_variable_header_flags);
    }

    #[test]
    fn connack_decode_failure_bad_reason_code() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            ..Default::default()
        };

        let corrupt_reason_code = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // for this packet, the reason code is in byte 3
            clone[3] = 255;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), corrupt_reason_code);
    }

    #[test]
    fn connack_decode_failure_duplicate_session_expiry_interval() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            session_expiry_interval : Some(3600),
            ..Default::default()
        };

        let duplicate_session_expiry_interval = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 5
            clone[1] += 5;

            // increase property section length by 5
            clone[4] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SESSION_EXPIRY_INTERVAL);
            clone.push(255);
            clone.push(255);
            clone.push(0);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_session_expiry_interval);
    }

    #[test]
    fn connack_decode_failure_duplicate_receive_maximum() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            receive_maximum : Some(10),
            ..Default::default()
        };

        let duplicate_receive_maximum = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 3
            clone[1] += 3;

            // increase property section length by 3
            clone[4] += 3;

            // add the duplicate property
            clone.push(PROPERTY_KEY_RECEIVE_MAXIMUM);
            clone.push(5);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_receive_maximum);
    }

    #[test]
    fn connack_decode_failure_duplicate_maximum_qos() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            maximum_qos : Some(QualityOfService::AtLeastOnce),
            ..Default::default()
        };

        let duplicate_maximum_qos = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 2
            clone[1] += 2;

            // increase property section length by 2
            clone[4] += 2;

            // add the duplicate property
            clone.push(PROPERTY_KEY_MAXIMUM_QOS);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_maximum_qos);
    }

    #[test]
    fn connack_decode_failure_invalid_maximum_qos() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            maximum_qos : Some(QualityOfService::AtLeastOnce),
            ..Default::default()
        };

        let invalidate_maximum_qos = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[6] = 3;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), invalidate_maximum_qos);
    }

    #[test]
    fn connack_decode_failure_duplicate_retain_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            retain_available : Some(true),
            ..Default::default()
        };

        let duplicate_retain_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 2
            clone[1] += 2;

            // increase property section length by 2
            clone[4] += 2;

            // add the duplicate property
            clone.push(PROPERTY_KEY_RETAIN_AVAILABLE);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_retain_available);
    }

    #[test]
    fn connack_decode_failure_invalid_retain_available() {
        let packet = ConnackPacket {
            session_present: true,
            reason_code: ConnectReasonCode::Success,
            retain_available: Some(true),
            ..Default::default()
        };

        let invalidate_retain_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[6] = 2;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), invalidate_retain_available);
    }

    #[test]
    fn connack_decode_failure_duplicate_maximum_packet_size() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            maximum_packet_size : Some(128 * 1024),
            ..Default::default()
        };

        let duplicate_maximum_packet_size = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 5
            clone[1] += 5;

            // increase property section length by 5
            clone[4] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_MAXIMUM_PACKET_SIZE);
            clone.push(1);
            clone.push(0);
            clone.push(2);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_maximum_packet_size);
    }

    #[test]
    fn connack_decode_failure_duplicate_assigned_client_identifier() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            assigned_client_identifier : Some("a".to_string()),
            ..Default::default()
        };

        let duplicate_assigned_client_identifier = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 5
            clone[1] += 5;

            // increase property section length by 5
            clone[4] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER);
            clone.push(2);
            clone.push(0);
            clone.push(65);
            clone.push(65);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_assigned_client_identifier);
    }

    #[test]
    fn connack_decode_failure_duplicate_topic_alias_maximum() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            topic_alias_maximum : Some(12),
            ..Default::default()
        };

        let duplicate_topic_alias_maximum = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 3
            clone[1] += 3;

            // increase property section length by 3
            clone[4] += 3;

            // add the duplicate property
            clone.push(PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM);
            clone.push(15);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_topic_alias_maximum);
    }

    #[test]
    fn connack_decode_failure_duplicate_reason_string() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            reason_string : Some("What".to_string()),
            ..Default::default()
        };

        let duplicate_reason_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 5
            clone[1] += 5;

            // increase property section length by 5
            clone[4] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_REASON_STRING);
            clone.push(2);
            clone.push(0);
            clone.push(78);
            clone.push(111);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_reason_string);
    }

    #[test]
    fn connack_decode_failure_duplicate_wildcard_subscription_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            wildcard_subscriptions_available : Some(true),
            ..Default::default()
        };

        let duplicate_wildcard_subscriptions_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 2
            clone[1] += 2;

            // increase property section length by 2
            clone[4] += 2;

            // add the duplicate property
            clone.push(PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_wildcard_subscriptions_available);
    }

    #[test]
    fn connack_decode_failure_invalid_wildcard_subscription_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            wildcard_subscriptions_available : Some(true),
            ..Default::default()
        };

        let invalidate_wildcard_subscriptions_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[6] = 255;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), invalidate_wildcard_subscriptions_available);
    }

    #[test]
    fn connack_decode_failure_duplicate_subscription_identifiers_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            subscription_identifiers_available : Some(true),
            ..Default::default()
        };

        let duplicate_subscription_identifiers_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 2
            clone[1] += 2;

            // increase property section length by 2
            clone[4] += 2;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE);
            clone.push(1);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_subscription_identifiers_available);
    }

    #[test]
    fn connack_decode_failure_invalid_subscription_identifiers_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            subscription_identifiers_available : Some(true),
            ..Default::default()
        };

        let invalidate_subscription_identifiers_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[6] = 31;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), invalidate_subscription_identifiers_available);
    }

    #[test]
    fn connack_decode_failure_duplicate_shared_subscription_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            shared_subscriptions_available : Some(true),
            ..Default::default()
        };

        let duplicate_shared_subscriptions_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 2
            clone[1] += 2;

            // increase property section length by 2
            clone[4] += 2;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE);
            clone.push(1);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_shared_subscriptions_available);
    }

    #[test]
    fn connack_decode_failure_invalid_shared_subscription_available() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            shared_subscriptions_available : Some(true),
            ..Default::default()
        };

        let invalidate_shared_subscriptions_available = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[6] = 2;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), invalidate_shared_subscriptions_available);
    }

    #[test]
    fn connack_decode_failure_duplicate_server_keep_alive() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            server_keep_alive : Some(1200),
            ..Default::default()
        };

        let duplicate_server_keep_alive = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 3
            clone[1] += 3;

            // increase property section length by 3
            clone[4] += 3;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SERVER_KEEP_ALIVE);
            clone.push(0);
            clone.push(20);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_server_keep_alive);
    }

    #[test]
    fn connack_decode_failure_duplicate_response_information() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            response_information : Some("A/topic".to_string()),
            ..Default::default()
        };

        let duplicate_response_information = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            // increase total remaining length by 5
            clone[1] += 5;

            // increase property section length by 5
            clone[4] += 5;

            // add the duplicate property
            clone.push(PROPERTY_KEY_RESPONSE_INFORMATION);
            clone.push(2);
            clone.push(0);
            clone.push(78);
            clone.push(111);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_response_information);
    }

    #[test]
    fn connack_decode_failure_duplicate_server_reference() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            server_reference : Some("fail.com".to_string()),
            ..Default::default()
        };

        let duplicate_server_reference = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            let property_value = "derp.com".as_bytes();
            let expansion = (property_value.len() + 3) as u8;

            // increase total remaining length
            clone[1] += expansion;

            // increase property section length
            clone[4] += expansion;

            // add the duplicate property
            clone.push(PROPERTY_KEY_SERVER_REFERENCE);
            clone.push(property_value.len() as u8);
            clone.push(0);
            clone.extend_from_slice(property_value);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_server_reference);
    }

    #[test]
    fn connack_decode_failure_duplicate_authentication_method() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            authentication_method : Some("rock-paper-scissors".to_string()),
            ..Default::default()
        };

        let duplicate_authentication_method = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            let property_value = "123".as_bytes();
            let expansion = (property_value.len() + 3) as u8;

            // increase total remaining length
            clone[1] += expansion;

            // increase property section length
            clone[4] += expansion;

            // add the duplicate property
            clone.push(PROPERTY_KEY_AUTHENTICATION_METHOD);
            clone.push(property_value.len() as u8);
            clone.push(0);
            clone.extend_from_slice(property_value);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_authentication_method);
    }

    #[test]
    fn connack_decode_failure_duplicate_authentication_data() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            authentication_data : Some("privatekey".as_bytes().to_vec()),
            ..Default::default()
        };

        let duplicate_authentication_data = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            let property_value = "deadbeef".as_bytes();
            let expansion = (property_value.len() + 3) as u8;

            // increase total remaining length
            clone[1] += expansion;

            // increase property section length
            clone[4] += expansion;

            // add the duplicate property
            clone.push(PROPERTY_KEY_AUTHENTICATION_DATA);
            clone.push(property_value.len() as u8);
            clone.push(0);
            clone.extend_from_slice(property_value);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Connack(packet), duplicate_authentication_data);
    }

    #[test]
    fn connack_decode_failure_packet_size() {
        let packet = ConnackPacket {
            session_present : true,
            reason_code : ConnectReasonCode::Success,
            authentication_data : Some("privatekey".as_bytes().to_vec()),
            ..Default::default()
        };

        do_inbound_size_decode_failure_test(&MqttPacket::Connack(packet));
    }

    use crate::validate::testing::*;

    fn do_connack_validate_failure_test(packet: ConnackPacket) {
        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_inbound_internal(&MqttPacket::Connack(packet), &validation_context), Err(Mqtt5Error::ConnackPacketValidation));
    }

    #[test]
    fn connack_validate_success_all_properties() {
        let packet = create_all_properties_connack_packet();

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_inbound_internal(&MqttPacket::Connack(packet), &validation_context), Ok(()));
    }

    #[test]
    fn connack_validate_failure_session_present_failed_reason_code() {
        let mut packet = create_all_properties_connack_packet();
        packet.session_present = true;
        packet.reason_code = ConnectReasonCode::BadUsernameOrPassword;

        do_connack_validate_failure_test(packet);
    }

    #[test]
    fn connack_validate_failure_receive_maximum_zero() {
        let mut packet = create_all_properties_connack_packet();
        packet.receive_maximum = Some(0);

        do_connack_validate_failure_test(packet);
    }

    #[test]
    fn connack_validate_failure_maximum_qos_2() {
        let mut packet = create_all_properties_connack_packet();
        packet.maximum_qos = Some(QualityOfService::ExactlyOnce);

        do_connack_validate_failure_test(packet);
    }

    #[test]
    fn connack_validate_failure_maximum_packet_size_zero() {
        let mut packet = create_all_properties_connack_packet();
        packet.maximum_packet_size = Some(0);

        do_connack_validate_failure_test(packet);
    }
}
