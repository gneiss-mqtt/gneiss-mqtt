/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing a set of structured data types that model the MQTT5 specification.
 */

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

use crate::mqtt::auth::*;
use crate::mqtt::connack::*;
use crate::mqtt::connect::*;
use crate::mqtt::disconnect::*;
use crate::mqtt::pingreq::*;
use crate::mqtt::pingresp::*;
use crate::mqtt::puback::*;
use crate::mqtt::pubcomp::*;
use crate::mqtt::publish::*;
use crate::mqtt::pubrec::*;
use crate::mqtt::pubrel::*;
use crate::mqtt::suback::*;
use crate::mqtt::subscribe::*;
use crate::mqtt::unsuback::*;
use crate::mqtt::unsubscribe::*;

/// MQTT message delivery quality of service.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum QualityOfService {

    /// The message is delivered according to the capabilities of the underlying network. No response is sent by the
    /// receiver and no retry is performed by the sender. The message arrives at the receiver either once or not at all.
    #[default]
    AtMostOnce = 0,

    /// A level of service that ensures that the message arrives at the receiver at least once.
    AtLeastOnce = 1,

    /// A level of service that ensures that the message arrives at the receiver exactly once.
    ExactlyOnce = 2,
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

    /// Returned when the server does not support MQTT5 protocol version specified in the connection.
    UnsupportedProtocolVersion = 132,

    /// Returned when the client identifier in the CONNECT packet is a valid string but not one that
    /// is allowed on the server.
    ClientIdentifierNotValid = 133,

    /// Returned when the server does not accept the username and/or password specified by the client
    /// in the connection packet.
    BadUsernameOrPassword = 134,

    /// Returned when the client is not authorized to connect to the server.
    NotAuthorized = 135,

    /// Returned when the MQTT5 server is not available.
    ServerUnavailable = 136,

    /// Returned when the server is too busy to make a connection. It is recommended that the client try again later.
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

/// Reason code inside PUBCOMP packets that indicates the result of receiving a PUBREL packet as part of the QoS 2 publish delivery process.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901154) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PubcompReasonCode {

    /// Returned when the associated PUBREL was successfully accepted by the recipient.  Marks a successful
    /// conclusion to the QoS 2 delivery sequence.
    ///
    /// May be sent by the client or the server.
    #[default]
    Success = 0,

    /// Returned when the associated PUBREL's packet id was not being tracked by the recipient as an in-progress QoS 2 delivery.
    ///
    /// May be sent by the client or the server.
    PacketIdentifierNotFound = 146,
}

/// Reason code inside DISCONNECT packets.  Helps determine why a connection was terminated.
///
/// Enum values match [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208) encoding values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DisconnectReasonCode {

    /// Returned when the remote endpoint wishes to disconnect normally. Will not trigger the publish of a Will message if a
    /// Will message was configured on the connection.
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


/// Data model for MQTT5 user properties.
///
/// A user property is a name-value pair of utf-8 strings that can be added to mqtt5 packets. Names are
/// not unique; a given name value can appear more than once in a packet.
///
/// User properties are required to be a utf-8 string pair, as specified by the
/// [MQTT5 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901013).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UserProperty {

    /// User property name
    pub name: String,

    /// User property value
    pub value: String,
}

/// Specifies a single subscription within a Subscribe operation
///
/// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Subscription {

    /// Topic filter to subscribe to
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub topic_filter: String,

    /// Maximum QoS on which the subscriber will accept publish messages.  Negotiated QoS may be different.
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub qos: QualityOfService,

    /// Should the server not send publishes to a client when that client was the one who sent the publish?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub no_local: bool,

    /// Should messages sent due to this subscription keep the retain flag preserved on the message?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub retain_as_published: bool,

    /// Should retained messages on matching topics be sent in reaction to this subscription?
    ///
    /// See [MQTT5 Subscription Options](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169)
    pub retain_handling_type: RetainHandlingType,
}

impl Subscription {

    /// Common-case constructor for Subscriptions that don't need specialized configuration
    pub fn new(topic_filter: &str, qos: QualityOfService) -> Self {
        Subscription {
            topic_filter: topic_filter.to_string(),
            qos,
            ..Default::default()
        }
    }
}

/// Algebraic union of all MQTT5 packet types.
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
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
