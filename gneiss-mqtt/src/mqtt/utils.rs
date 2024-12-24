/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing containing miscellaneous constants and conversion functions related to the MQTT specification.
While currently public, this is unstable and probably going to change because the only exports are a couple of
integer -> mqtt spec enum conversion functions.
 */

use crate::mqtt::*;

pub(crate) const PACKET_TYPE_CONNECT: u8 = 1;
pub(crate) const PACKET_TYPE_CONNACK: u8 = 2;
pub(crate) const PACKET_TYPE_PUBLISH: u8 = 3;
pub(crate) const PACKET_TYPE_PUBACK: u8 = 4;
pub(crate) const PACKET_TYPE_PUBREC: u8 = 5;
pub(crate) const PACKET_TYPE_PUBREL: u8 = 6;
pub(crate) const PACKET_TYPE_PUBCOMP: u8 = 7;
pub(crate) const PACKET_TYPE_SUBSCRIBE: u8 = 8;
pub(crate) const PACKET_TYPE_SUBACK: u8 = 9;
pub(crate) const PACKET_TYPE_UNSUBSCRIBE: u8 = 10;
pub(crate) const PACKET_TYPE_UNSUBACK: u8 = 11;
pub(crate) const PACKET_TYPE_PINGREQ: u8 = 12;
pub(crate) const PACKET_TYPE_PINGRESP: u8 = 13;
pub(crate) const PACKET_TYPE_DISCONNECT: u8 = 14;
pub(crate) const PACKET_TYPE_AUTH: u8 = 15;

pub(crate) const PROPERTY_KEY_PAYLOAD_FORMAT_INDICATOR: u8 = 1;
pub(crate) const PROPERTY_KEY_MESSAGE_EXPIRY_INTERVAL: u8 = 2;
pub(crate) const PROPERTY_KEY_CONTENT_TYPE: u8 = 3;
pub(crate) const PROPERTY_KEY_RESPONSE_TOPIC: u8 = 8;
pub(crate) const PROPERTY_KEY_CORRELATION_DATA: u8 = 9;
pub(crate) const PROPERTY_KEY_SUBSCRIPTION_IDENTIFIER: u8 = 11;
pub(crate) const PROPERTY_KEY_SESSION_EXPIRY_INTERVAL: u8 = 17;
pub(crate) const PROPERTY_KEY_ASSIGNED_CLIENT_IDENTIFIER: u8 = 18;
pub(crate) const PROPERTY_KEY_SERVER_KEEP_ALIVE: u8 = 19;
pub(crate) const PROPERTY_KEY_AUTHENTICATION_METHOD: u8 = 21;
pub(crate) const PROPERTY_KEY_AUTHENTICATION_DATA: u8 = 22;
pub(crate) const PROPERTY_KEY_REQUEST_PROBLEM_INFORMATION: u8 = 23;
pub(crate) const PROPERTY_KEY_WILL_DELAY_INTERVAL: u8 = 24;
pub(crate) const PROPERTY_KEY_REQUEST_RESPONSE_INFORMATION: u8 = 25;
pub(crate) const PROPERTY_KEY_RESPONSE_INFORMATION: u8 = 26;
pub(crate) const PROPERTY_KEY_SERVER_REFERENCE: u8 = 28;
pub(crate) const PROPERTY_KEY_REASON_STRING: u8 = 31;
pub(crate) const PROPERTY_KEY_RECEIVE_MAXIMUM: u8 = 33;
pub(crate) const PROPERTY_KEY_TOPIC_ALIAS_MAXIMUM: u8 = 34;
pub(crate) const PROPERTY_KEY_TOPIC_ALIAS: u8 = 35;
pub(crate) const PROPERTY_KEY_MAXIMUM_QOS: u8 = 36;
pub(crate) const PROPERTY_KEY_RETAIN_AVAILABLE: u8 = 37;
pub(crate) const PROPERTY_KEY_USER_PROPERTY: u8 = 38;
pub(crate) const PROPERTY_KEY_MAXIMUM_PACKET_SIZE: u8 = 39;
pub(crate) const PROPERTY_KEY_WILDCARD_SUBSCRIPTIONS_AVAILABLE: u8 = 40;
pub(crate) const PROPERTY_KEY_SUBSCRIPTION_IDENTIFIERS_AVAILABLE: u8 = 41;
pub(crate) const PROPERTY_KEY_SHARED_SUBSCRIPTIONS_AVAILABLE: u8 = 42;

pub(crate) const PUBLISH_PACKET_FIXED_HEADER_DUPLICATE_FLAG : u8 = 8;
pub(crate) const PUBLISH_PACKET_FIXED_HEADER_RETAIN_FLAG : u8 = 1;
pub(crate) const QOS_MASK : u8 = 3;

#[cfg(test)]
pub(crate) const CONNECT_PACKET_CLEAN_START_FLAG_MASK : u8 = 1 << 1;
#[cfg(test)]
pub(crate) const CONNECT_PACKET_HAS_WILL_FLAG_MASK : u8 = 1 << 2;
#[cfg(test)]
pub(crate) const CONNECT_PACKET_WILL_RETAIN_FLAG_MASK : u8 = 1 << 5;
#[cfg(test)]
pub(crate) const CONNECT_PACKET_WILL_QOS_FLAG_SHIFT : u8 = 3;
#[cfg(test)]
pub(crate) const CONNECT_PACKET_HAS_USERNAME_FLAG_MASK : u8 = 1 << 7;
#[cfg(test)]
pub(crate) const CONNECT_PACKET_HAS_PASSWORD_FLAG_MASK : u8 = 1 << 6;

pub(crate) const UNSUBSCRIBE_FIRST_BYTE : u8 = (PACKET_TYPE_UNSUBSCRIBE << 4) | (0x02u8);
pub(crate) const UNSUBACK_FIRST_BYTE : u8 = PACKET_TYPE_UNSUBACK << 4;
pub(crate) const SUBSCRIBE_FIRST_BYTE : u8 = (PACKET_TYPE_SUBSCRIBE << 4) | (0x02u8);
pub(crate) const SUBACK_FIRST_BYTE : u8 = PACKET_TYPE_SUBACK << 4;
pub(crate) const PUBREL_FIRST_BYTE : u8 = (PACKET_TYPE_PUBREL << 4) | (0x02u8);
pub(crate) const PUBACK_FIRST_BYTE : u8 = PACKET_TYPE_PUBACK << 4;
pub(crate) const PUBREC_FIRST_BYTE : u8 = PACKET_TYPE_PUBREC << 4;
pub(crate) const PUBCOMP_FIRST_BYTE : u8 = PACKET_TYPE_PUBCOMP << 4;

pub(crate) const SUBSCRIPTION_OPTIONS_NO_LOCAL_MASK : u8 = 1u8 << 2;
pub(crate) const SUBSCRIPTION_OPTIONS_RETAIN_AS_PUBLISHED_MASK : u8 = 1u8 << 3;
pub(crate) const SUBSCRIPTION_OPTIONS_RETAIN_HANDLING_SHIFT : u8 = 4;

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