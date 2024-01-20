/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

extern crate log;

use crate::validate::*;

use log::*;

pub(crate) fn validate_string_length(value: &String, packet_type: PacketType, packet_name: &str, field_name: &str) -> MqttResult<()> {
    if value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        error!("{}Packet Validation - {} string field too long", packet_name, field_name);
        return Err(MqttError::PacketValidation(packet_type));
    }

    Ok(())
}

pub(crate) fn validate_optional_string_length(optional_string: &Option<String>, packet_type: PacketType, packet_name: &str, field_name: &str) -> MqttResult<()> {
    if let Some(value) = &optional_string {
        if value.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
            error!("{}Packet Validation - {} string field too long", packet_name, field_name);
            return Err(MqttError::PacketValidation(packet_type));
        }
    }

    Ok(())
}

pub(crate) fn validate_optional_binary_length(optional_data: &Option<Vec<u8>>, packet_type: PacketType, packet_name: &str, field_name: &str) -> MqttResult<()> {
    if let Some(value) = &optional_data {
        if value.len() > MAXIMUM_BINARY_PROPERTY_LENGTH {
            error!("{}Packet Validation - {} binary field too long", packet_name, field_name);
            return Err(MqttError::PacketValidation(packet_type));
        }
    }

    Ok(())
}

macro_rules! validate_optional_integer_non_zero {
    ($value_name: ident, $optional_integer_expr: expr, $packet_type: expr, $packet_name: expr, $field_name: expr) => {
        if let Some($value_name) = $optional_integer_expr {
            if $value_name == 0 {
                error!("{}Packet Validation - {} integer field is zero", $packet_name, $field_name);
                return Err(MqttError::PacketValidation($packet_type));
            }
        }
    };
}

pub(crate) use validate_optional_integer_non_zero;

macro_rules! validate_ack_outbound {
    ($function_name: ident, $packet_type_name: ident, $packet_type: expr, $packet_type_string: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type_name) -> MqttResult<()> {

            validate_optional_string_length(&packet.reason_string, $packet_type, $packet_type_string, "reason_string")?;
            validate_user_properties(&packet.user_properties, $packet_type, $packet_type_string)?;

            Ok(())
        }
    };
}

pub(crate) use validate_ack_outbound;

macro_rules! validate_ack_outbound_internal {
    ($function_name: ident, $packet_type_name: ident, $packet_type: expr, $packet_length_function_name: ident, $packet_type_string: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type_name, context: &OutboundValidationContext) -> MqttResult<()> {

            let (total_remaining_length, _) = $packet_length_function_name(packet)?;
            let total_packet_length = 1 + total_remaining_length + compute_variable_length_integer_encode_size(total_remaining_length as usize)? as u32;
            if total_packet_length > context.negotiated_settings.unwrap().maximum_packet_size_to_server {
                error!("{}Packet Validation - packet length exceeds allowed maximum to server", $packet_type_string);
                return Err(MqttError::PacketValidation($packet_type));
            }

            if packet.packet_id == 0 {
                error!("{}Packet Validation - packet id is zero", $packet_type_string);
                return Err(MqttError::PacketValidation($packet_type));
            }

            Ok(())
        }
    };
}

pub(crate) use validate_ack_outbound_internal;

macro_rules! validate_ack_inbound_internal {
    ($function_name: ident, $packet_type_name: ident, $packet_type: expr, $packet_type_string: expr) => {
        pub(crate) fn $function_name(packet: &$packet_type_name, _: &InboundValidationContext) -> MqttResult<()> {

            if packet.packet_id == 0 {
                error!("{}Packet Validation - packet id is zero", $packet_type_string);
                return Err(MqttError::PacketValidation($packet_type));
            }

            Ok(())
        }
    };
}

pub(crate) use validate_ack_inbound_internal;

pub(crate) fn is_valid_topic(topic: &str) -> bool {
    if topic.is_empty() || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        return false;
    }

    if topic.contains(['#', '+']) {
        return false;
    }

    true
}

// if the topic filter is not valid, then the other fields are not to be trusted
pub(crate) struct TopicFilterProperties {
    pub is_valid: bool,
    pub is_shared: bool,
    pub has_wildcard: bool
}

fn compute_topic_filter_properties(topic: &str) -> TopicFilterProperties {
    let mut properties = TopicFilterProperties {
        is_valid: true,
        is_shared: false,
        has_wildcard: false
    };

    if topic.is_empty() || topic.len() > MAXIMUM_STRING_PROPERTY_LENGTH {
        properties.is_valid = false;
        return properties;
    }

    let mut has_share_prefix = false;
    let mut has_share_name = false;
    let mut seen_mlw = false;
    for (index, segment) in  topic.split('/').enumerate() {
        if seen_mlw {
            properties.is_valid = false;
            break;
        }

        let has_wildcard = segment.contains(['#', '+']);
        properties.has_wildcard |= has_wildcard;

        if index == 0 && segment == "$share" {
            has_share_prefix = true;
        }

        if index == 1 && has_share_prefix && !segment.is_empty() && !has_wildcard {
            has_share_name = true;
        }

        if has_share_name && ((index == 2 && !segment.is_empty()) || index > 2) {
            properties.is_shared = true;
        }

        if segment.len() == 1 {
            if segment == "#" {
                seen_mlw = true;
            }
        } else if has_wildcard {
            properties.is_valid = false;
            break;
        }
    }

    properties
}

pub(crate) fn is_valid_topic_filter_internal(filter: &str, context: &OutboundValidationContext, no_local: Option<bool>) -> bool {
    let topic_filter_properties = compute_topic_filter_properties(filter);

    if !topic_filter_properties.is_valid {
        return false;
    }

    if topic_filter_properties.is_shared {
        if !context.negotiated_settings.unwrap().shared_subscriptions_available {
            return false;
        }

        if let Some(no_local_value) = no_local {
            if no_local_value {
                return false;
            }
        }
    }

    if topic_filter_properties.has_wildcard && !context.negotiated_settings.unwrap().wildcard_subscriptions_available {
        return false;
    }

    true
}

#[cfg(test)]
pub(crate) mod testing {

    use super::*;

    macro_rules! test_ack_validate_success {
        ($function_name: ident, $packet_type: ident, $packet_factory_function: ident) => {
            #[test]
            fn $function_name() {
                let packet = MqttPacket::$packet_type($packet_factory_function());

                assert!(validate_packet_outbound(&packet).is_ok());

                let test_validation_context = create_pinned_validation_context();

                let outbound_validation_context = create_outbound_validation_context_from_pinned(&test_validation_context);
                assert!(validate_packet_outbound_internal(&packet, &outbound_validation_context).is_ok());

                let inbound_validation_context = create_inbound_validation_context_from_pinned(&test_validation_context);
                assert!(validate_packet_inbound_internal(&packet, &inbound_validation_context).is_ok());
            }
        };
    }

    pub(crate) use test_ack_validate_success;

    macro_rules! test_ack_validate_failure_reason_string_length {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: pat) => {
            #[test]
            fn $function_name() {
                let mut packet = $packet_factory_function();
                packet.reason_string = Some("A".repeat(128 * 1024).to_string());

                assert_matches!(validate_packet_outbound(&MqttPacket::$packet_type_name(packet)), Err(MqttError::PacketValidation($packet_type)));
            }
        };
    }

    pub(crate) use test_ack_validate_failure_reason_string_length;

    macro_rules! test_ack_validate_failure_invalid_user_properties {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: pat) => {
            #[test]
            fn $function_name() {
                let mut packet = $packet_factory_function();
                packet.user_properties = Some(create_invalid_user_properties());

                assert_matches!(validate_packet_outbound(&MqttPacket::$packet_type_name(packet)), Err(MqttError::PacketValidation($packet_type)));
            }
        };
    }

    pub(crate) use test_ack_validate_failure_invalid_user_properties;

    macro_rules! test_ack_validate_failure_outbound_size {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: expr) => {
            #[test]
            fn $function_name() {
                let packet = $packet_factory_function();

                do_outbound_size_validate_failure_test(&MqttPacket::$packet_type_name(packet), $packet_type);
            }
        };
    }

    pub(crate) use test_ack_validate_failure_outbound_size;

    macro_rules! test_ack_validate_failure_packet_id_zero {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: pat) => {
            #[test]
            fn $function_name() {
                let mut ack = $packet_factory_function();
                ack.packet_id = 0;

                let packet = MqttPacket::$packet_type_name(ack);

                let test_validation_context = create_pinned_validation_context();

                let outbound_context = create_outbound_validation_context_from_pinned(&test_validation_context);
                assert_matches!(validate_packet_outbound_internal(&packet, &outbound_context), Err(MqttError::PacketValidation($packet_type)));

                let inbound_context = create_inbound_validation_context_from_pinned(&test_validation_context);
                assert_matches!(validate_packet_inbound_internal(&packet, &inbound_context), Err(MqttError::PacketValidation($packet_type)));
            }
        };
    }

    pub(crate) use test_ack_validate_failure_packet_id_zero;

    macro_rules! test_ack_validate_failure_inbound_packet_id_zero {
        ($function_name: ident, $packet_type_name: ident, $packet_factory_function: ident, $packet_type: pat) => {
            #[test]
            fn $function_name() {
                let mut ack = $packet_factory_function();
                ack.packet_id = 0;

                let packet = MqttPacket::$packet_type_name(ack);

                let test_validation_context = create_pinned_validation_context();
                let inbound_context = create_inbound_validation_context_from_pinned(&test_validation_context);
                assert_matches!(validate_packet_inbound_internal(&packet, &inbound_context), Err(MqttError::PacketValidation($packet_type)));
            }
        };
    }

    pub(crate) use test_ack_validate_failure_inbound_packet_id_zero;

    #[test]
    fn check_valid_topics() {
        assert_eq!(true, is_valid_topic("/"));
        assert_eq!(true, is_valid_topic("a/"));
        assert_eq!(true, is_valid_topic("/b"));
        assert_eq!(true, is_valid_topic("a/b/c"));
        assert_eq!(true, is_valid_topic("hippopotamus"));
    }

    #[test]
    fn check_invalid_topics() {
        assert_eq!(false, is_valid_topic("#"));
        assert_eq!(false, is_valid_topic("sport/tennis/#"));
        assert_eq!(false, is_valid_topic("sport/tennis#"));
        assert_eq!(false, is_valid_topic("sport/tennis/#/ranking"));
        assert_eq!(false, is_valid_topic(""));
        assert_eq!(false, is_valid_topic("+"));
        assert_eq!(false, is_valid_topic("+/tennis/#"));
        assert_eq!(false, is_valid_topic("sport/+/player1"));
        assert_eq!(false, is_valid_topic("sport+"));
        assert_eq!(false, is_valid_topic(&"s".repeat(70000).to_string()));
    }

    #[test]
    fn check_valid_topic_filters() {
        let default_settings = NegotiatedSettings {
            wildcard_subscriptions_available: true,
            shared_subscriptions_available: true,
            ..Default::default()
        };

        let context = OutboundValidationContext {
            negotiated_settings: Some(&default_settings),
            connect_options: None,
            outbound_alias_resolution: None,
        };

        assert_eq!(true, is_valid_topic_filter_internal("a/b/c", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("/#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("sports/tennis/#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("+", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("/+", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("+/a", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("+/tennis/#", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("port/+/player1", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("$share/derp/derp", &context, Some(false)));
        assert_eq!(true, is_valid_topic_filter_internal("$share/derp/derp", &context, None));
    }

    #[test]
    fn check_invalid_topic_filters() {
        let default_settings = NegotiatedSettings {
            wildcard_subscriptions_available: true,
            shared_subscriptions_available: true,
            ..Default::default()
        };

        let mut context = OutboundValidationContext {
            negotiated_settings: Some(&default_settings),
            connect_options: None,
            outbound_alias_resolution: None,
        };

        assert_eq!(false, is_valid_topic_filter_internal("", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("derp+", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("derp+/", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("derp#/", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("#/a", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("sport/tennis#", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("sport/tennis/#/ranking", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("sport+", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal(&"s".repeat(70000).to_string(), &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("$share/derp/derp", &context, Some(true)));

        let no_wildcard_settings = NegotiatedSettings {
            wildcard_subscriptions_available: false,
            shared_subscriptions_available: true,
            ..Default::default()
        };
        context.negotiated_settings = Some(&no_wildcard_settings);

        assert_eq!(false, is_valid_topic_filter_internal("#", &context, None));
        assert_eq!(false, is_valid_topic_filter_internal("/+", &context, None));
        assert_eq!(true, is_valid_topic_filter_internal("$share/derp/derp", &context, None));

        let no_shared_settings = NegotiatedSettings {
            wildcard_subscriptions_available: true,
            shared_subscriptions_available: false,
            ..Default::default()
        };
        context.negotiated_settings = Some(&no_shared_settings);

        assert_eq!(false, is_valid_topic_filter_internal(&"$share/derp/derp".to_string(), &context, None));
    }

    #[test]
    fn check_topic_properties() {
        assert_eq!(true, compute_topic_filter_properties("a/b/c").is_valid);
        assert_eq!(true, compute_topic_filter_properties("#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("sports/tennis/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+").is_valid);
        assert_eq!(true, compute_topic_filter_properties("/+").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+/a").is_valid);
        assert_eq!(true, compute_topic_filter_properties("+/tennis/#").is_valid);
        assert_eq!(true, compute_topic_filter_properties("port/+/player1").is_valid);
        assert_eq!(false, compute_topic_filter_properties("").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp+").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp+/").is_valid);
        assert_eq!(false, compute_topic_filter_properties("derp#/").is_valid);
        assert_eq!(false, compute_topic_filter_properties("#/a").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport/tennis#").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport/tennis/#/ranking").is_valid);
        assert_eq!(false, compute_topic_filter_properties("sport+").is_valid);
        assert_eq!(false, compute_topic_filter_properties(&"s".repeat(70000).to_string()).is_valid);

        assert_eq!(false, compute_topic_filter_properties("a/b/c").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share//c").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/+/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/#/a").is_shared);
        assert_eq!(false, compute_topic_filter_properties("$share/b/").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/b//").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/a/b").is_shared);
        assert_eq!(true, compute_topic_filter_properties("$share/a/b/c").is_shared);

        assert_eq!(false, compute_topic_filter_properties("a/b/c").has_wildcard);
        assert_eq!(false, compute_topic_filter_properties("/").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("#").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("+").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("a/+/+").has_wildcard);
        assert_eq!(true, compute_topic_filter_properties("a/b/#").has_wildcard);
    }
}
