/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate log;

use crate::*;
use crate::alias::*;
use crate::decode::utils::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::logging::*;
use crate::spec::*;
use crate::spec::utils::*;
use crate::validate::*;
use crate::validate::utils::*;

use log::*;
use std::collections::VecDeque;
use std::fmt;

/// Data model of an [MQTT5 PUBLISH](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901100) packet
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PublishPacket {

    /// Packet Id of the publish.  Setting this value on an outbound publish has no effect on the
    /// actual packet id used by the client.
    pub packet_id: u16,

    /// Sent publishes - The topic this message should be published to.
    ///
    /// Received publishes - The topic this message was published to.
    ///
    /// See [MQTT5 Topic Name](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901107)
    pub topic: String,

    /// Sent publishes - The MQTT quality of service level this message should be delivered with.
    ///
    /// Received publishes - The MQTT quality of service level this message was delivered at.
    ///
    /// See [MQTT5 QoS](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901103)
    pub qos: QualityOfService,

    /// Indicates to the recipient that this packet is a resend of a previously-submitted
    /// Publish
    pub duplicate: bool,

    /// True if this is a retained message, false otherwise.
    ///
    /// Always set on received publishes; on sent publishes, undefined implies false.
    ///
    /// See [MQTT5 Retain](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901104)
    pub retain: bool,

    /// The payload of the publish message.
    ///
    /// See [MQTT5 Publish Payload](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901119)
    pub payload: Option<Vec<u8>>,

    /// Property specifying the format of the payload data.  The mqtt5 client does not enforce or use this
    /// value in a meaningful way.
    ///
    /// See [MQTT5 Payload Format Indicator](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111)
    pub payload_format: Option<PayloadFormatIndicator>,

    /// Sent publishes - indicates the maximum amount of time allowed to elapse for message delivery before the server
    /// should instead delete the message (relative to a recipient).
    ///
    /// Received publishes - indicates the remaining amount of time (from the server's perspective) before the message would
    /// have been deleted relative to the subscribing client.
    ///
    /// If left undefined, indicates no expiration timeout.
    ///
    /// See [MQTT5 Message Expiry Interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901112)
    pub message_expiry_interval_seconds: Option<u32>,

    /// If the topic field is non-empty:
    ///   Tells the recipient to bind this id to the topic field's value within its alias cache
    ///
    /// If the topic field is empty:
    ///   Tells the recipient to lookup the topic in their alias cache based on this id.
    ///
    /// See [MQTT5 Topic Alias](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901113)
    pub topic_alias: Option<u16>,

    /// Opaque topic string intended to assist with request/response implementations.  Not internally meaningful to
    /// MQTT5 or this client.
    ///
    /// See [MQTT5 Response Topic](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901114)
    pub response_topic: Option<String>,

    /// Opaque binary data used to correlate between publish messages, as a potential method for request-response
    /// implementation.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Correlation Data](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901115)
    pub correlation_data: Option<Vec<u8>>,

    /// Sent publishes - setting this fails client-side packet validation
    ///
    /// Received publishes - the subscription identifiers of all the subscriptions this message matched.
    ///
    /// See [MQTT5 Subscription Identifier](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901117)
    pub subscription_identifiers: Option<Vec<u32>>,

    /// Property specifying the content type of the payload.  Not internally meaningful to MQTT5.
    ///
    /// See [MQTT5 Content Type](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901118)
    pub content_type: Option<String>,

    /// Set of MQTT5 user properties included with the packet.
    ///
    /// See [MQTT5 User Property](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901116)
    pub user_properties: Option<Vec<UserProperty>>,
}


#[rustfmt::skip]
fn compute_publish_packet_length_properties(packet: &PublishPacket, alias_resolution: &OutboundAliasResolution) -> Mqtt5Result<(u32, u32)> {
    let mut publish_property_section_length = compute_user_properties_length(&packet.user_properties);

    add_optional_u8_property_length!(publish_property_section_length, packet.payload_format);
    add_optional_u32_property_length!(publish_property_section_length, packet.message_expiry_interval_seconds);
    add_optional_u16_property_length!(publish_property_section_length, alias_resolution.alias);
    add_optional_string_property_length!(publish_property_section_length, packet.content_type);
    add_optional_string_property_length!(publish_property_section_length, packet.response_topic);
    add_optional_bytes_property_length!(publish_property_section_length, packet.correlation_data);

    /* should never happen on the client, but just to be complete */
    if let Some(subscription_identifiers) = &packet.subscription_identifiers {
        for val in subscription_identifiers.iter() {
            let encoding_size = compute_variable_length_integer_encode_size(*val as usize)?;
            publish_property_section_length += 1 + encoding_size;
        }
    }

    /*
     * Remaining Length:
     * Variable Header
     *  - Topic Name
     *  - Packet Identifier
     *  - Property Length as VLI x
     *  - All Properties x
     * Payload
     */

    let mut total_remaining_length = compute_variable_length_integer_encode_size(publish_property_section_length)?;

    /* Topic name */
    total_remaining_length += 2;
    if !alias_resolution.skip_topic {
        total_remaining_length += packet.topic.len();
    }

    /* Optional (qos1+) packet id */
    if packet.qos != QualityOfService::AtMostOnce {
        total_remaining_length += 2;
    }

    total_remaining_length += publish_property_section_length;

    if let Some(payload) = &packet.payload {
        total_remaining_length += payload.len();
    }

    Ok((total_remaining_length as u32, publish_property_section_length as u32))
}

/*
 * Fixed Header
 * byte 1:
 *  bits 4-7: MQTT Control Packet Type
 *  bit 3: DUP flag
 *  bit 1-2: QoS level
 *  bit 0: RETAIN
 * byte 2-x: Remaining Length as Variable Byte Integer (1-4 bytes)
 */
fn compute_publish_fixed_header_first_byte(packet: &PublishPacket) -> u8 {
    let mut first_byte: u8 = PACKET_TYPE_PUBLISH << 4;

    if packet.duplicate {
        first_byte |= 1u8 << 3;
    }

    first_byte |= (packet.qos as u8) << 1;

    if packet.retain {
        first_byte |= 1u8;
    }

    first_byte
}

fn get_publish_packet_response_topic(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Publish, response_topic)
}

fn get_publish_packet_correlation_data(packet: &MqttPacket) -> &[u8] {
    get_optional_packet_field!(packet, MqttPacket::Publish, correlation_data)
}

fn get_publish_packet_content_type(packet: &MqttPacket) -> &str {
    get_optional_packet_field!(packet, MqttPacket::Publish, content_type)
}

fn get_publish_packet_topic(packet: &MqttPacket) -> &str {
    get_packet_field!(packet, MqttPacket::Publish, topic)
}

fn get_publish_packet_user_property(packet: &MqttPacket, index: usize) -> &UserProperty {
    if let MqttPacket::Publish(publish) = packet {
        if let Some(properties) = &publish.user_properties {
            return &properties[index];
        }
    }

    panic!("Internal encoding error: invalid user property state");
}

fn get_publish_packet_payload(packet: &MqttPacket) -> &[u8] {
    if let MqttPacket::Publish(publish) = packet {
        if let Some(bytes) = &publish.payload {
            return bytes;
        }
    }

    panic!("Internal encoding error: invalid publish payload state");
}

#[rustfmt::skip]
pub(crate) fn write_publish_encoding_steps(packet: &PublishPacket, context: &EncodingContext, steps: &mut VecDeque<EncodingStep>) -> Mqtt5Result<()> {

    let resolution = &context.outbound_alias_resolution;

    let (total_remaining_length, publish_property_length) = compute_publish_packet_length_properties(packet, resolution)?;

    encode_integral_expression!(steps, Uint8, compute_publish_fixed_header_first_byte(packet));
    encode_integral_expression!(steps, Vli, total_remaining_length);

    if resolution.skip_topic {
        // empty topic since an existing alias binding was used.
        encode_integral_expression!(steps, Uint16, 0);
    } else {
        // Add the topic since the outbound alias resolution did not use an existing binding
        encode_length_prefixed_string!(steps, get_publish_packet_topic, packet.topic);
    }

    if packet.qos != QualityOfService::AtMostOnce {
        encode_integral_expression!(steps, Uint16, packet.packet_id);
    }
    encode_integral_expression!(steps, Vli, publish_property_length);

    encode_optional_enum_property!(steps, Uint8, PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR, u8, packet.payload_format);
    encode_optional_property!(steps, Uint32, PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL, packet.message_expiry_interval_seconds);
    encode_optional_property!(steps, Uint16, PROPERTY_KEY_TOPIC_ALIAS, resolution.alias);
    encode_optional_string_property!(steps, get_publish_packet_response_topic, PROPERTY_KEY_RESPONSE_TOPIC, packet.response_topic);
    encode_optional_bytes_property!(steps, get_publish_packet_correlation_data, PROPERTY_KEY_CORRELATION_DATA, packet.correlation_data);

    if let Some(subscription_identifiers) = &packet.subscription_identifiers {
        for val in subscription_identifiers {
            encode_integral_expression!(steps, Uint8, PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER);
            encode_integral_expression!(steps, Vli, *val);
        }
    }

    encode_optional_string_property!(steps, get_publish_packet_content_type, PROPERTY_KEY_CONTENT_TYPE, &packet.content_type);
    encode_user_properties!(steps, get_publish_packet_user_property, packet.user_properties);

    if packet.payload.is_some() {
        encode_raw_bytes!(steps, get_publish_packet_payload);
    }

    Ok(())
}


fn decode_publish_properties(property_bytes: &[u8], packet : &mut PublishPacket) -> Mqtt5Result<()> {
    let mut mutable_property_bytes = property_bytes;

    while !mutable_property_bytes.is_empty() {
        let property_key = mutable_property_bytes[0];
        mutable_property_bytes = &mutable_property_bytes[1..];

        match property_key {
            PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR => { mutable_property_bytes = decode_optional_u8_as_enum(mutable_property_bytes, &mut packet.payload_format, convert_u8_to_payload_format_indicator)?; }
            PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL => { mutable_property_bytes = decode_optional_u32(mutable_property_bytes, &mut packet.message_expiry_interval_seconds)?; }
            PROPERTY_KEY_TOPIC_ALIAS => { mutable_property_bytes = decode_optional_u16(mutable_property_bytes, &mut packet.topic_alias)?; }
            PROPERTY_KEY_RESPONSE_TOPIC => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.response_topic)?; }
            PROPERTY_KEY_CORRELATION_DATA => { mutable_property_bytes = decode_optional_length_prefixed_bytes(mutable_property_bytes, &mut packet.correlation_data)?; }
            PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER => {
                let mut subscription_id : usize = 0;
                mutable_property_bytes = decode_vli_into_mutable(mutable_property_bytes, &mut subscription_id)?;
                if packet.subscription_identifiers.is_none() {
                    packet.subscription_identifiers = Some(Vec::new());
                }

                let ids : &mut Vec<u32> = packet.subscription_identifiers.as_mut().unwrap();
                ids.push(subscription_id as u32);
            }
            PROPERTY_KEY_USER_PROPERTY => { mutable_property_bytes = decode_user_property(mutable_property_bytes, &mut packet.user_properties)?; }
            PROPERTY_KEY_CONTENT_TYPE => { mutable_property_bytes = decode_optional_length_prefixed_string(mutable_property_bytes, &mut packet.content_type)?; }
            _ => {
                error!("PublishPacket Decode - Invalid property type ({})", property_key);
                return Err(Mqtt5Error::MalformedPacket);
            }
        }
    }

    Ok(())
}

pub(crate) fn decode_publish_packet(first_byte: u8, packet_body: &[u8]) -> Mqtt5Result<Box<MqttPacket>> {

    let mut box_packet = Box::new(MqttPacket::Publish(PublishPacket { ..Default::default() }));

    if let MqttPacket::Publish(packet) = box_packet.as_mut() {
        if (first_byte & PUBLISH_PACKET_FIXED_HEADER_DUPLICATE_FLAG) != 0 {
            packet.duplicate = true;
        }

        if (first_byte & PUBLISH_PACKET_FIXED_HEADER_RETAIN_FLAG) != 0 {
            packet.retain = true;
        }

        packet.qos = convert_u8_to_quality_of_service((first_byte >> 1) & QOS_MASK)?;

        let mut mutable_body = packet_body;
        let mut properties_length : usize = 0;

        mutable_body = decode_length_prefixed_string(mutable_body, &mut packet.topic)?;

        if packet.qos != QualityOfService::AtMostOnce {
            mutable_body = decode_u16(mutable_body, &mut packet.packet_id)?;
        }

        mutable_body = decode_vli_into_mutable(mutable_body, &mut properties_length)?;
        if properties_length > mutable_body.len() {
            error!("PublishPacket Decode - property length exceeds overall packet length");
            return Err(Mqtt5Error::MalformedPacket);
        }

        let properties_bytes = &mutable_body[..properties_length];
        let payload_bytes = &mutable_body[properties_length..];

        decode_publish_properties(properties_bytes, packet)?;

        if !payload_bytes.is_empty() {
            packet.payload = Some(payload_bytes.to_vec());
        }

        return Ok(box_packet);
    }

    panic!("PublishPacket Decode - Internal error");
}

pub(crate) fn validate_publish_packet_outbound(packet: &PublishPacket) -> Mqtt5Result<()> {

    if packet.packet_id != 0 {
        error!("PublishPacket Outbound Validation - packet id may not be set");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    if packet.qos == QualityOfService::AtMostOnce && packet.duplicate {
        error!("PublishPacket Outbound Validation - packet id is zero");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    validate_string_length(&packet.topic, Mqtt5Error::PublishPacketValidation, "Publish", "topic")?;

    if !is_valid_topic(&packet.topic) {
        error!("PublishPacket Outbound Validation - invalid topic");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    if let Some(alias) = packet.topic_alias {
        if alias == 0 {
            error!("PublishPacket Outbound Validation - topic alias is zero");
            return Err(Mqtt5Error::PublishPacketValidation);
        }
    }

    if packet.subscription_identifiers.is_some() {
        error!("PublishPacket Outbound Validation - subscription identifiers not allowed on client packets");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    if let Some(response_topic) = &packet.response_topic {
        if !is_valid_topic(response_topic) {
            error!("PublishPacket Outbound Validation - invalid response topic");
            return Err(Mqtt5Error::PublishPacketValidation);
        }

        validate_string_length(response_topic, Mqtt5Error::PublishPacketValidation, "Publish", "response_topic")?;
    }

    validate_user_properties(&packet.user_properties, Mqtt5Error::PublishPacketValidation, "Publish")?;
    validate_optional_binary_length(&packet.correlation_data, Mqtt5Error::PublishPacketValidation, "Publish", "correlation_data")?;
    validate_optional_string_length(&packet.content_type, Mqtt5Error::PublishPacketValidation, "Publish", "content_type")?;

    Ok(())
}

pub(crate) fn validate_publish_packet_outbound_internal(packet: &PublishPacket, context: &OutboundValidationContext) -> Mqtt5Result<()> {

    let (total_remaining_length, _) = compute_publish_packet_length_properties(packet, &context.outbound_alias_resolution.unwrap_or(OutboundAliasResolution{..Default::default() }))?;
    let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
    if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
        error!("PublishPacket Outbound Validation - packet length exceeds maximum packet size allowed to server");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    if packet.packet_id == 0 && packet.qos != QualityOfService::AtMostOnce {
        error!("PublishPacket Outbound Validation - packet id must be non zero");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    let settings = context.negotiated_settings.unwrap();
    if packet.retain && !settings.retain_available {
        error!("PublishPacket Outbound Validation - retained messages not allowed on this connection");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    match settings.maximum_qos {
        QualityOfService::AtMostOnce => {
            if packet.qos != QualityOfService::AtMostOnce {
                error!("PublishPacket Outbound Validation - quality of service exceeds established maximum");
                return Err(Mqtt5Error::PublishPacketValidation);
            }
        }
        QualityOfService::AtLeastOnce => {
            if packet.qos == QualityOfService::ExactlyOnce {
                error!("PublishPacket Outbound Validation - quality of service exceeds established maximum");
                return Err(Mqtt5Error::PublishPacketValidation);
            }
        }
        _ => {}
    }

    Ok(())
}

pub(crate) fn validate_publish_packet_inbound_internal(packet: &PublishPacket, _: &InboundValidationContext) -> Mqtt5Result<()> {

    /* alias resolution happens after decode and before validation, so by now we should have a real topic */
    if packet.topic.is_empty() {
        error!("PublishPacket Inbound Validation - topic could not be resolved");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    if packet.packet_id == 0 && packet.qos != QualityOfService::AtMostOnce {
        error!("PublishPacket Inbound Validation - packet id must be non zero");
        return Err(Mqtt5Error::PublishPacketValidation);
    }

    Ok(())
}

impl fmt::Display for PublishPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PublishPacket {{")?;
        log_primitive_value!(self.packet_id, f, "packet_id");
        log_string!(self.topic, f, "topic");
        log_enum!(self.qos, f, "qos", quality_of_service_to_str);
        log_primitive_value!(self.duplicate, f, "duplicate");
        log_primitive_value!(self.retain, f, "retain");
        log_optional_binary_data!(self.payload, f, "payload", value);
        log_optional_enum!(self.payload_format, f, "payload_format", value, payload_format_indicator_to_str);
        log_optional_primitive_value!(self.message_expiry_interval_seconds, f, "message_expiry_interval_seconds", value);
        log_optional_primitive_value!(self.topic_alias, f, "topic_alias", value);
        log_optional_string!(self.response_topic, f, "response_topic", value);
        log_optional_binary_data!(self.correlation_data, f, "correlation_data", value);

        if let Some(ids) = &self.subscription_identifiers {
            write!(f, "  subscription_identifiers: [")?;
            for id in ids {
                write!(f, "{} ", id)?;
            }
            writeln!(f, "]")?;
        }

        log_optional_string!(self.content_type, f, "content_type", value);
        log_user_properties!(self.user_properties, f, "user_properties", value);
        write!(f, "}}")
    }
}

// Some convenience constructors
impl PublishPacket {
    pub fn new(topic: &str, qos: QualityOfService, payload: &[u8]) -> Self {
        PublishPacket {
            topic: topic.to_string(),
            qos,
            payload: Some(payload.to_vec()),
            ..Default::default()
        }
    }

    pub fn new_empty(topic: &str, qos: QualityOfService) -> Self {
        PublishPacket {
            topic: topic.to_string(),
            qos,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::decode::testing::*;
    use crate::validate::testing::create_invalid_user_properties;

    #[test]
    fn publish_round_trip_encode_decode_default() {
        let packet = PublishPacket {
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    #[test]
    fn publish_round_trip_encode_decode_basic() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            payload: Some("a payload".as_bytes().to_vec()),
            ..Default::default()
        };

        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    fn create_publish_with_all_fields() -> PublishPacket {
        return PublishPacket {
            packet_id: 47,
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            duplicate: true,
            retain: true,
            payload: Some("a payload".as_bytes().to_vec()),
            payload_format: Some(PayloadFormatIndicator::Utf8),
            message_expiry_interval_seconds : Some(3600),
            topic_alias: Some(10),
            response_topic: Some("Respond/to/me".to_string()),
            correlation_data: Some(vec!(1, 2, 3, 4, 5)),
            subscription_identifiers: Some(vec!(10, 20, 256, 32768)),
            content_type: Some("rest/json".to_string()),
            user_properties: Some(vec!(
                UserProperty{name: "name1".to_string(), value: "value1".to_string()},
                UserProperty{name: "name2".to_string(), value: "value2".to_string()},
                UserProperty{name: "name3".to_string(), value: "value3".to_string()},
            ))
        };
    }

    fn create_outbound_publish_with_all_fields() -> PublishPacket {
        let mut packet = create_publish_with_all_fields();
        packet.subscription_identifiers = None;

        packet
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields() {
        let packet = create_publish_with_all_fields();
        assert!(do_round_trip_encode_decode_test(&MqttPacket::Publish(packet)));
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_2byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 257]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_3byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 32768]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_round_trip_encode_decode_all_fields_4byte_payload() {
        let mut publish = create_publish_with_all_fields();
        publish.payload = Some(vec![0; 128 * 128 * 128]);

        let packet = &MqttPacket::Publish(publish);

        let decode_fragment_sizes : Vec<usize> = vec!(1, 2, 3, 5, 7);

        for decode_size in decode_fragment_sizes.iter() {
            assert!(do_single_encode_decode_test(&packet, 1024, *decode_size, 5));
        }
    }

    #[test]
    fn publish_decode_failure_message_expiry_interval_duplicate() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            message_expiry_interval_seconds: Some(1),
            ..Default::default()
        };

        let duplicate_message_expiry_interval = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 5;

            // Index = 2 + 2 + topic.len + 2(packet id) = 17
            clone[17] += 5;

            // finally, append the bytes for the duplicate property to the end.  This is valid
            // since we gave the publish no payload and so we're still in the property section at
            // the very end of the buffer.
            // We don't care about the actual value of the property.
            clone.push(PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL);
            clone.push(2);
            clone.push(0);
            clone.push(0);
            clone.push(0);
            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), duplicate_message_expiry_interval);
    }

    #[test]
    fn publish_decode_failure_invalid_qos() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            ..Default::default()
        };

        let invalidate_qos = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[0] |= 6; // Qos "3"

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), invalidate_qos);
    }

    const PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX : usize = 17;

    #[test]
    fn publish_decode_failure_invalid_payload_format_indicator() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            payload_format : Some(PayloadFormatIndicator::Utf8),
            ..Default::default()
        };

        let invalidate_pfi = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX + 2] = 2;

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), invalidate_pfi);
    }

    #[test]
    fn publish_decode_failure_duplicate_payload_format_indicator() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            payload_format : Some(PayloadFormatIndicator::Utf8),
            ..Default::default()
        };

        let duplicate_pfi = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 2;
            clone[PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX] += 2;

            clone.push(PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR);
            clone.push(0);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), duplicate_pfi);
    }

    #[test]
    fn publish_decode_failure_duplicate_message_expiry_interval() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            message_expiry_interval_seconds : Some(1),
            ..Default::default()
        };

        let duplicate_message_expiry = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 5;
            clone[PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX] += 5;

            clone.push(PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL);
            clone.push(1);
            clone.push(2);
            clone.push(3);
            clone.push(4);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), duplicate_message_expiry);
    }

    #[test]
    fn publish_decode_failure_duplicate_response_topic() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            response_topic : Some("a/b".to_string()),
            ..Default::default()
        };

        let duplicate_response_string = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 5;
            clone[PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX] += 5;

            clone.push(PROPERTY_KEY_RESPONSE_TOPIC);
            clone.push(0);
            clone.push(2);
            clone.push(65);
            clone.push(65);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), duplicate_response_string);
    }

    #[test]
    fn publish_decode_failure_duplicate_correlation_data() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            correlation_data : Some("a".as_bytes().to_vec()),
            ..Default::default()
        };

        let duplicate_correlation_data = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 5;
            clone[PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX] += 5;

            clone.push(PROPERTY_KEY_CORRELATION_DATA);
            clone.push(0);
            clone.push(2);
            clone.push(1);
            clone.push(5);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), duplicate_correlation_data);
    }

    #[test]
    fn publish_decode_failure_duplicate_content_type() {

        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            content_type : Some("JSON".to_string()),
            ..Default::default()
        };

        let duplicate_content_type = | bytes: &[u8] | -> Vec<u8> {
            let mut clone = bytes.to_vec();

            clone[1] += 5;
            clone[PUBLISH_PACKET_ALL_PROPERTIES_TEST_PROPERTY_LENGTH_INDEX] += 5;

            clone.push(PROPERTY_KEY_CONTENT_TYPE);
            clone.push(0);
            clone.push(2);
            clone.push(66);
            clone.push(65);

            clone
        };

        do_mutated_decode_failure_test(&MqttPacket::Publish(packet), duplicate_content_type);
    }

    #[test]
    fn publish_decode_failure_inbound_packet_size() {
        let packet = PublishPacket {
            topic: "hello/world".to_string(),
            qos: QualityOfService::AtLeastOnce,
            payload: Some("A very nice payload.  Much wow.".as_bytes().to_vec()),
            ..Default::default()
        };

        do_inbound_size_decode_failure_test(&MqttPacket::Publish(packet));
    }

    #[test]
    fn publish_validate_success() {
        let mut packet = create_publish_with_all_fields();
        packet.subscription_identifiers = None;
        packet.packet_id = 0;

        let outbound_packet = MqttPacket::Publish(packet);

        assert_eq!(validate_packet_outbound(&outbound_packet), Ok(()));

        let mut packet2 = create_publish_with_all_fields();
        packet2.subscription_identifiers = None;

        let outbound_internal_packet = MqttPacket::Publish(packet2);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_qos = QualityOfService::ExactlyOnce;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_outbound_internal(&outbound_internal_packet, &outbound_validation_context), Ok(()));

        let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_inbound_internal(&outbound_internal_packet, &inbound_validation_context), Ok(()));
    }

    #[test]
    fn publish_validate_failure_outbound_qos_zero_and_duplicate() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.qos = QualityOfService::AtMostOnce;
        packet.duplicate = true;

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_qos_zero_and_packet_id() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.qos = QualityOfService::AtMostOnce;
        packet.packet_id = 1;

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_outbound_failure_topic_length() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.topic = "A".repeat(65536).to_string();

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_outbound_failure_topic_invalid() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.topic = "A/+/B".to_string();

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_topic_alias_zero() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.topic_alias = Some(0);

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_response_topic_invalid() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.response_topic = Some("A/#/B".to_string());

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_response_topic_length() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.response_topic = Some("AB".repeat(33000).to_string());

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_subscription_identifiers_exist() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.subscription_identifiers = Some(vec![2, 3, 4]);

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_user_properties_invalid() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.user_properties = Some(create_invalid_user_properties());

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_correlation_data_length() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.correlation_data = Some(vec![0; 80 * 1024]);

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_content_type_length() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.content_type = Some("CD".repeat(33000).to_string());

        assert_eq!(validate_packet_outbound(&MqttPacket::Publish(packet)), Err(Mqtt5Error::PublishPacketValidation));
    }

    use crate::validate::testing::*;

    #[test]
    fn publish_validate_failure_outbound_size() {
        let mut packet = create_publish_with_all_fields();
        packet.topic_alias = None;
        packet.subscription_identifiers = None;

        do_outbound_size_validate_failure_test(&MqttPacket::Publish(packet), Mqtt5Error::PublishPacketValidation);
    }


    #[test]
    fn publish_validate_failure_outbound_internal_retain_unavailable() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.retain = true;

        let packet = MqttPacket::Publish(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.retain_available = false;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_outbound_internal(&packet, &outbound_validation_context), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_internal_maximum_qos_qos0_exceeded() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.qos = QualityOfService::AtLeastOnce;

        let packet = MqttPacket::Publish(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_qos = QualityOfService::AtMostOnce;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_outbound_internal(&packet, &outbound_validation_context), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_outbound_internal_maximum_qos_qos1_exceeded() {
        let mut packet = create_outbound_publish_with_all_fields();
        packet.qos = QualityOfService::ExactlyOnce;

        let packet = MqttPacket::Publish(packet);

        let mut test_validation_context = create_pinned_validation_context();
        test_validation_context.settings.maximum_qos = QualityOfService::AtLeastOnce;

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_outbound_internal(&packet, &outbound_validation_context), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_inbound_empty_topic() {
        let mut packet = create_publish_with_all_fields();
        packet.topic = "".to_string();

        let test_validation_context = create_pinned_validation_context();
        let validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);

        assert_eq!(validate_packet_inbound_internal(&MqttPacket::Publish(packet), &validation_context), Err(Mqtt5Error::PublishPacketValidation));
    }

    #[test]
    fn publish_validate_failure_qos1plus_packet_id_zero() {
        let mut packet = create_publish_with_all_fields();
        packet.subscription_identifiers = None;
        packet.packet_id = 0;

        let packet = MqttPacket::Publish(packet);

        let test_validation_context = create_pinned_validation_context();

        let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_outbound_internal(&packet, &outbound_validation_context), Err(Mqtt5Error::PublishPacketValidation));

        let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
        assert_eq!(validate_packet_inbound_internal(&packet, &inbound_validation_context), Err(Mqtt5Error::PublishPacketValidation));
    }
}
