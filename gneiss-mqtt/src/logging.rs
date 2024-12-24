/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::mqtt::*;
use log::*;
use std::fmt;
use std::fmt::Write;

impl fmt::Display for UserProperty {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {}) ", self.name, self.value)
    }
}

pub(crate) fn create_user_properties_log_string(properties: &Vec<UserProperty>) -> String {
    let mut val : String = "[ ".to_string();
    for property in properties {
        write!(&mut val, " (\"{}\",\"{}\")", property.name, property.value).ok();
    }
    write!(&mut val, " ]").ok();
    val
}

macro_rules! log_primitive_value {
    ($integral_value: expr, $formatter: expr, $log_field: expr) => {
        write!($formatter, " {}:{}", $log_field, $integral_value)?;
    };
}

pub(crate) use log_primitive_value;

macro_rules! log_optional_primitive_value {
    ($optional_integral_value: expr, $formatter: expr, $log_field: expr, $value: ident) => {
        if let Some($value) = &$optional_integral_value {
            write!($formatter, " {}:{}", $log_field, $value)?;
        }
    };
}

pub(crate) use log_optional_primitive_value;

macro_rules! log_enum {
    ($enum_value: expr, $formatter: expr, $log_field: expr, $converter: ty) => {
        write!($formatter, " {}:{}", $log_field, <$converter>::to_string(&$enum_value))?;
    };
}

pub(crate) use log_enum;

macro_rules! log_optional_enum {
    ($optional_enum_value: expr, $formatter: expr, $log_field: expr, $value:ident, $converter: ty) => {
        if let Some($value) = &$optional_enum_value {
            write!($formatter, " {}:{}", $log_field, <$converter>::to_string(&*$value))?;
        }
    };
}

pub(crate) use log_optional_enum;

macro_rules! log_string {
    ($value: expr, $formatter: expr, $log_field: expr) => {
        write!($formatter, " {}:\"{}\"", $log_field, $value)?;
    };
}

pub(crate) use log_string;

macro_rules! log_optional_string {
    ($optional_string: expr, $formatter: expr, $log_field: expr, $value:ident) => {
        if let Some($value) = &$optional_string {
            write!($formatter, " {}:\"{}\"", $log_field, $value)?;
        }
    };
}

pub(crate) use log_optional_string;

macro_rules! log_optional_string_sensitive {
    ($optional_string: expr, $formatter: expr, $log_field: expr) => {
        if let Some(_) = &$optional_string {
            write!($formatter, " {}:<...redacted>", $log_field)?;
        }
    };
}

pub(crate) use log_optional_string_sensitive;

macro_rules! log_optional_binary_data {
    ($optional_data: expr, $formatter: expr, $log_field: expr, $value:ident) => {
        if let Some($value) = &$optional_data {
            write!($formatter, " {}:<{} Bytes>",  $log_field, $value.len())?;
        }
    };
}

pub(crate) use log_optional_binary_data;

macro_rules! log_optional_binary_data_sensitive {
    ($optional_data: expr, $formatter: expr, $log_field: expr) => {
        if let Some(_) = &$optional_data {
            write!($formatter, " {}:<...redacted>", $log_field)?;
        }
    };
}

pub(crate) use log_optional_binary_data_sensitive;

macro_rules! log_user_properties {
    ($user_properties: expr, $formatter: expr, $log_field: expr, $value:ident) => {
        if let Some($value) = &$user_properties {
            write!($formatter, " {}:{}", $log_field, create_user_properties_log_string($value))?;
        }
    };
}

pub(crate) use log_user_properties;

macro_rules! define_ack_packet_display_trait {
    ($packet_type: ident, $packet_name: expr, $reason_code_type: ident) => {
        impl fmt::Display for $packet_type {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{} {{", $packet_name)?;
                log_primitive_value!(self.packet_id, f, "packet_id");
                log_enum!(self.reason_code, f, "reason_code", $reason_code_type);
                log_optional_string!(self.reason_string, f, "reason_string", value);
                log_user_properties!(self.user_properties, f, "user_properties", value);
                write!(f, " }}")
            }
        }
    };
}

pub(crate) use define_ack_packet_display_trait;

fn get_packet_type_for_logging(packet: &MqttPacket) -> &'static str {
    match packet {
        MqttPacket::Connect(_) => { "ConnectPacket{...}" }
        MqttPacket::Connack(_) => { "ConnackPacket{...}" }
        MqttPacket::Publish(_) => { "PublishPacket{...}" }
        MqttPacket::Puback(_) => { "PubackPacket{...}" }
        MqttPacket::Pubrec(_) => { "PubrecPacket{...}" }
        MqttPacket::Pubrel(_) => { "PubrelPacket{...}" }
        MqttPacket::Pubcomp(_) => { "PubcompPacket{...}" }
        MqttPacket::Subscribe(_) => { "SubscribePacket{...}" }
        MqttPacket::Suback(_) => { "SubackPacket{...}" }
        MqttPacket::Unsubscribe(_) => { "UnsubscribePacket{...}" }
        MqttPacket::Unsuback(_) => { "UnsubackPacket{...}" }
        MqttPacket::Pingreq(_) => { "PingreqPacket{...}" }
        MqttPacket::Pingresp(_) => { "PingrespPacket{...}" }
        MqttPacket::Disconnect(_) => { "DisconnectPacket{...}" }
        MqttPacket::Auth(_) => { "AuthPacket{...}" }
    }
}

impl fmt::Display for MqttPacket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MqttPacket::Connect(packet) => { packet.fmt(f) }
            MqttPacket::Connack(packet) => { packet.fmt(f) }
            MqttPacket::Publish(packet) => { packet.fmt(f) }
            MqttPacket::Puback(packet) => { packet.fmt(f) }
            MqttPacket::Pubrec(packet) => { packet.fmt(f) }
            MqttPacket::Pubrel(packet) => { packet.fmt(f) }
            MqttPacket::Pubcomp(packet) => { packet.fmt(f) }
            MqttPacket::Subscribe(packet) => { packet.fmt(f) }
            MqttPacket::Suback(packet) => { packet.fmt(f) }
            MqttPacket::Unsubscribe(packet) => { packet.fmt(f) }
            MqttPacket::Unsuback(packet) => { packet.fmt(f) }
            MqttPacket::Pingreq(packet) => { packet.fmt(f) }
            MqttPacket::Pingresp(packet) => { packet.fmt(f) }
            MqttPacket::Disconnect(packet) => { packet.fmt(f) }
            MqttPacket::Auth(packet) => { packet.fmt(f) }
        }
    }
}

pub(crate) fn log_packet(prefix: &str, packet: &MqttPacket) {
    let level = log::max_level();
    match level {
        LevelFilter::Info => {
            info!("{}{}", prefix, get_packet_type_for_logging(packet));
        }
        LevelFilter::Debug | LevelFilter::Trace => {
            debug!("{}{}", prefix, packet);
        }
        _ => {}
    }
}