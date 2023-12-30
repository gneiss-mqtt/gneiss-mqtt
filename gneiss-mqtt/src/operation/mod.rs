/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

// Internal module that implements most of the MQTT5 spec with respect to client protocol behavior

pub(crate) mod testing;

extern crate log;

use crate::*;
use crate::alias::*;
use crate::client::*;
use crate::client::internal::*;
use crate::decode::*;
use crate::encode::*;
use crate::encode::utils::*;
use crate::spec::*;
use crate::spec::connack::*;
use crate::spec::utils::*;
use crate::validate::*;

use log::*;

use std::cell::RefCell;
use std::cmp::{Ordering, Reverse};
use std::collections::*;
use std::fmt::*;
use std::mem;
use std::time::*;

enum MqttOperationOptions {
    Publish(PublishOptionsInternal),
    Subscribe(SubscribeOptionsInternal),
    Unsubscribe(UnsubscribeOptionsInternal),
}

// Data structure that tracks the state of an MQTT operation.  This includes both user-submitted
// operations and internally-generated ones.  Every outbound packet corresponds to an operation.
// This packet correspondence is 1-1 with the single exception of a pubrel being associated with a
// qos2 publish.
pub(crate) struct MqttOperation {

    // Every operation has a unique id, starting at 1.  Id allocation is serialized based on
    // time-of-submission.  In this way, complying with MQTT spec ordering requirements ends up
    // being sorts of id sequences.
    id: u64,

    // The base packet associated with this operation.
    packet: Box<MqttPacket>,

    // unpleasant hack to let the same operation track both the original qos 2 publish and the
    // followup pubrel
    qos2_pubrel: Option<Box<MqttPacket>>,

    // MQTT packet id that has been assigned to this operation.  Assignment is also reflected in
    // the packet itself.
    packet_id: Option<u16>,

    // Additional options (primarily completion channel) for an operation
    options: Option<MqttOperationOptions>,

    // Always starts as None
    //
    // Set when the operation is "essentially" flushed to the socket (but before write completion)
    // When the operation completes (by either write completion for pingreqs and qos 0 publishes,
    // final ack for subscribe, unsubscribe and qos1+ publishes), we bump the next ping
    // timepoint to at least (this value + the keep alive interval).
    //
    // The details are complicated, but it boils down to this:
    //
    // The next ping timepoint is based on the transmission time of the last broker-acknowledged
    // packet sent by the client.
    ping_extension_base_timepoint: Option<Instant>,
}

impl MqttOperation {
    pub fn bind_packet_id(&mut self, packet_id: u16) {
        self.packet_id = Some(packet_id);
        match &mut *self.packet {
            MqttPacket::Subscribe(subscribe) => {
                debug!("Subscribe operation {} binding to packet id {}", self.id, packet_id);
                subscribe.packet_id = packet_id;
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                debug!("Unsubscribe operation {} binding to packet id {}", self.id, packet_id);
                unsubscribe.packet_id = packet_id;
            }
            MqttPacket::Publish(publish) => {
                debug!("Publish operation {} binding to packet id {}", self.id, packet_id);
                publish.packet_id = packet_id;
            }
            _ => {
                panic!("Invalid packet type for packet id binding");
            }
        }
    }

    pub fn unbind_packet_id(&mut self) {
        self.packet_id = None;
        match &mut *self.packet {
            MqttPacket::Subscribe(subscribe) => {
                debug!("Subscribe operation {} unbinding packet id", self.id);
                subscribe.packet_id = 0;
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                debug!("Unsubscribe operation {} unbinding packet id", self.id);
                unsubscribe.packet_id = 0;
            }
            MqttPacket::Publish(publish) => {
                debug!("Publish operation {} unbinding packet id", self.id);
                publish.packet_id = 0;
            }
            _ => {
                panic!("Invalid packet type for packet id unbinding");
            }
        }
    }
}

// Most received packets stay internal or are routed to an operation's result channel.  But
// Connack, Publish, and Disconnect are all surfaced to the user through the client.
#[cfg_attr(test, derive(Eq, PartialEq, Debug))]
pub(crate) enum PacketEvent {
    Connack(ConnackPacket),
    Publish(PublishPacket),
    Disconnect(DisconnectPacket)
}

// The client's operational state is completely uncoupled from networking data types.  We offer
// a simple interface that models and handles all relevant events.
pub(crate) enum NetworkEvent<'a> {
    ConnectionOpened,
    ConnectionClosed,
    IncomingData(&'a [u8]),
    WriteCompletion
}

pub(crate) struct NetworkEventContext<'a> {
    pub(crate) event: NetworkEvent<'a>,
    pub(crate) current_time: Instant,

    // output field for packets that the client is interested in
    pub(crate) packet_events: &'a mut VecDeque<PacketEvent>,
}

// The four actions users can take with respect to operational state.  Start/stop is handled
// by the containing client.
pub(crate) enum UserEvent {
    Publish(Box<MqttPacket>, PublishOptionsInternal),
    Subscribe(Box<MqttPacket>, SubscribeOptionsInternal),
    Unsubscribe(Box<MqttPacket>, UnsubscribeOptionsInternal),
    Disconnect(Box<MqttPacket>)
}

pub(crate) struct UserEventContext {
    pub(crate) event: UserEvent,
    pub(crate) current_time: Instant,
}

pub(crate) struct ServiceContext<'a> {
    // output field for all data that should be written to the socket.  This vector is fixed-sized.
    // Because we wait for write completion before encoding more, the capacity of this vector
    // represents a bound on the amount of data between the client and the socket.
    pub(crate) to_socket: &'a mut Vec<u8>,
    pub(crate) current_time: Instant,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum OperationalStateType {
    Disconnected,
    PendingConnack,
    Connected,
    PendingDisconnect,
    Halted
}

impl Display for OperationalStateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            OperationalStateType::Disconnected => { write!(f, "Disconnected") }
            OperationalStateType::PendingConnack => { write!(f, "PendingConnack") }
            OperationalStateType::Connected => { write!(f, "Connected") }
            OperationalStateType::PendingDisconnect => { write!(f, "PendingDisconnect") }
            OperationalStateType::Halted => { write!(f, "Halted") }
        }
    }
}

pub(crate) struct OperationalStateConfig {
    pub connect_options: ConnectOptions,

    pub base_timestamp: Instant,

    pub offline_queue_policy: OfflineQueuePolicy,

    pub connack_timeout: Duration,
    pub ping_timeout: Duration,

    pub outbound_alias_resolver: Option<Box<dyn OutboundAliasResolver + Send>>,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum OperationalQueueType {
    User,
    HighPriority,
}

impl Display for OperationalQueueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self {
            OperationalQueueType::User => { write!(f, "User") }
            OperationalQueueType::HighPriority => { write!(f, "HighPriority") }
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum OperationalQueueServiceMode {
    All,
    HighPriorityOnly,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum OperationalEnqueuePosition {
    Front,
    Back
}

impl Display for OperationalEnqueuePosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationalEnqueuePosition::Front => { write!(f, "Front") }
            OperationalEnqueuePosition::Back => { write!(f, "Back") }
        }
    }
}

enum OperationResponse {
    Publish(PublishResponse),
    Subscribe(SubackPacket),
    Unsubscribe(UnsubackPacket),
}

#[derive(Copy, Clone, PartialEq, Eq)]
struct OperationTimeoutRecord {
    id: u64,
    timeout: Instant
}

impl PartialOrd for OperationTimeoutRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.timeout.cmp(&other.timeout))
    }    
}

impl Ord for OperationTimeoutRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timeout.cmp(&other.timeout)
    }
}

// Primary data structure that tracks MQTT-related state for the containing client.
pub(crate) struct OperationalState {
    config: OperationalStateConfig,

    state: OperationalStateType,

    // the need to model time in a simple, test-controllable fashion leads to a solution where
    // the state thinks in time based on elapsed milliseconds since the state was created.  This
    // allows for simple time mocking which lets us simulate the passage of time "instantly."
    current_time: Instant,
    elapsed_time_ms: u128,

    // Flag set by the service function after encoding bytes to be written to the socket.
    // Unset when we receive notice that the socket has fully accepted all encoded bytes.
    // No additional bytes are encoded while this flag is set.
    pending_write_completion: bool,

    // All incomplete operations tracked by the client
    operations: HashMap<u64, MqttOperation>,

    // (Optional) Timeouts for all ack-based operations (qos1+ publish, subscribe, unsubscribe)
    // The timeout only covers the period between operation-written-to-socket and
    // response-received-from-socket.  The time an operation spends in an intake queue is not
    // bounded by anything.
    operation_ack_timeouts: BinaryHeap<Reverse<OperationTimeoutRecord>>,

    // Intake queues

    // lowest priority queue; all user operations are added to the end on submission
    user_operation_queue: VecDeque<u64>,

    // contains qos1+ publishes that were interrupted by a disconnect; spec compliance requires
    // these be re-sent first on session resumption using the original order and packet ids
    resubmit_operation_queue: VecDeque<u64>,

    // highest priority queue; for acks, pings, disconnect
    high_priority_operation_queue: VecDeque<u64>,

    // Service pulls operations from the intake queues based on priority order.  When an operation
    // becomes current, we bind a packet id if necessary, and set up the encoder to encode it.  It
    // stays there until the encoder has fully written all of the bytes to a buffer.  For larger
    // packets this may take a number of [encode -> write to socket -> write completion] cycles.
    current_operation: Option<u64>,

    // Tracks the packet ids of incoming qos2 publishes that haven't been released yet.  When
    // we receive a qos2 publish whose packet id is in here, we can ignore it because it's a
    // duplicate delivery.  Packet ids are removed when we receive a pubrel for it.
    qos2_incomplete_incoming_publishes: HashSet<u16>,

    // Tracks the packet ids in use by the client for outbound ack-based operations.  Does not
    // reset between connections.  Used to find unused packet ids for unbound operations.
    // { packet id -> operation id }
    allocated_packet_ids: HashMap<u16, u64>,

    // Tracks all qos1+ publishes that have been written to the socket but not yet completed.
    // A Qos2 publish will be in this map from the time the publish is written until the pubcomp is
    // received or there is a disconnection.
    // { packet id -> operation id }
    pending_publish_operations: HashMap<u16, u64>,

    // Tracks all subscribes and unsubscribes that have been written to the socket but not yet
    // completed.
    // { packet id -> operation id }
    pending_non_publish_operations: HashMap<u16, u64>,

    // Tracks all incomplete operations that don't use acks that have been written to the socket.
    // These operations will be completed on the next write completion event.
    pending_write_completion_operations: VecDeque<u64>,

    // Connection-scoped set of negotiated protocol values
    current_settings: Option<NegotiatedSettings>,

    // monotonically-increasing operation id value
    next_operation_id: u64,

    // counter that helps us heuristically find an unused packet id with as little id-space
    // search as possible
    next_packet_id: u16,

    // Tracks if the containing client has previously successfully connected.  Used to conditionally
    // rejoin sessions.
    has_connected_successfully: bool,

    // MQTT packet encode and decode
    encoder: Encoder,
    decoder: Decoder,

    // Point in time we should send another ping.  If None, we are in the middle of a ping.
    next_ping_timepoint: Option<Instant>,

    // Point in time that our current ping will time out.  If none, we are not in the middle of a
    // ping.
    ping_timeout_timepoint: Option<Instant>,

    // Point in time that we will consider the initial CONNECT packet/request to have timed out.
    connack_timeout_timepoint: Option<Instant>,

    // Topic aliasing support
    outbound_alias_resolver: RefCell<Box<dyn OutboundAliasResolver>>,
    inbound_alias_resolver: InboundAliasResolver
}

impl Display for OperationalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = log::max_level();
        match level {
            LevelFilter::Debug => {
                self.log_debug(f)
            }
            LevelFilter::Trace => {
                self.log_trace(f)
            }
            _ => { Ok(()) }
        }
    }
}

impl OperationalState {

    // Crate-public API

    pub(crate) fn new(mut config: OperationalStateConfig) -> OperationalState {
        let outbound_resolver = config.outbound_alias_resolver.take().unwrap_or(Box::new(NullOutboundAliasResolver::new()));
        let inbound_resolver = InboundAliasResolver::new(config.connect_options.topic_alias_maximum.unwrap_or(0));
        let base_time = config.base_timestamp;

        OperationalState {
            config,
            state: OperationalStateType::Disconnected,
            current_time: base_time,
            elapsed_time_ms: 0,
            pending_write_completion : false,
            operations: HashMap::new(),
            operation_ack_timeouts: BinaryHeap::new(),
            user_operation_queue: VecDeque::new(),
            resubmit_operation_queue: VecDeque::new(),
            high_priority_operation_queue: VecDeque::new(),
            current_operation: None,
            qos2_incomplete_incoming_publishes: HashSet::new(),
            allocated_packet_ids: HashMap::new(),
            pending_publish_operations: HashMap::new(),
            pending_non_publish_operations: HashMap::new(),
            pending_write_completion_operations: VecDeque::new(),
            current_settings: None,
            next_operation_id : 1,
            next_packet_id : 1,
            has_connected_successfully: false,
            encoder: Encoder::new(),
            decoder: Decoder::new(),
            next_ping_timepoint: None,
            ping_timeout_timepoint: None,
            connack_timeout_timepoint: None,
            outbound_alias_resolver: RefCell::new(outbound_resolver),
            inbound_alias_resolver: inbound_resolver
        }
    }

    pub(crate) fn handle_network_event(&mut self, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        self.update_internal_clock(&context.current_time);

        let event = &context.event;
        let result =
            match &event {
                NetworkEvent::ConnectionOpened => { self.handle_network_event_connection_opened(context) }
                NetworkEvent::ConnectionClosed => { self.handle_network_event_connection_closed(context) }
                NetworkEvent::WriteCompletion => { self.handle_network_event_write_completion(context) }
                NetworkEvent::IncomingData(data) => { self.handle_network_event_incoming_data(context, data) }
            };

        self.log_operational_state();

        // Any error state returned from an event handler halts the client.  This is not always
        // an ERROR-error.  For example, write completion that includes a disconnect packet will
        // return an error, allowing us to reset the client nicely.
        if result.is_err() {
            error!("[{} ms] handle_network_event - final result: {:?}", self.elapsed_time_ms, result);
            self.change_state(OperationalStateType::Halted);
        } else {
            debug!("[{} ms] handle_network_event - final result: {:?}", self.elapsed_time_ms, result);
        }

        result
    }

    pub(crate) fn service(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        self.update_internal_clock(&context.current_time);

        let result =
            match self.state {
                OperationalStateType::Disconnected => { self.service_disconnected(context) }
                OperationalStateType::PendingConnack => { self.service_pending_connack(context) }
                OperationalStateType::Connected => { self.service_connected(context) }
                OperationalStateType::PendingDisconnect => { self.service_pending_disconnect(context) }
                OperationalStateType::Halted => { Err(Mqtt5Error::InternalStateError) }
            };

        self.log_operational_state();

        // Any error state returned from an event handler halts the client.
        if result.is_err() {
            error!("[{} ms] service - final result: {:?}", self.elapsed_time_ms, result);
            self.change_state(OperationalStateType::Halted);
        } else {
            debug!("[{} ms] service - final result: {:?}", self.elapsed_time_ms, result);
        }

        result
    }

    pub(crate) fn handle_user_event(&mut self, context: UserEventContext) {
        self.update_internal_clock(&context.current_time);

        let event = context.event;
        let (op_id, queue, position) =
            match event {
                UserEvent::Subscribe(packet, subscribe_options) => {
                    (self.create_operation(packet, Some(MqttOperationOptions::Subscribe(subscribe_options))), OperationalQueueType::User, OperationalEnqueuePosition::Back)
                }
                UserEvent::Unsubscribe(packet, unsubscribe_options) => {
                    (self.create_operation(packet, Some(MqttOperationOptions::Unsubscribe(unsubscribe_options))), OperationalQueueType::User, OperationalEnqueuePosition::Back)
                }
                UserEvent::Publish(packet, publish_options) => {
                    (self.create_operation(packet, Some(MqttOperationOptions::Publish(publish_options))), OperationalQueueType::User, OperationalEnqueuePosition::Back)
                }
                UserEvent::Disconnect(disconnect) => {
                    (self.create_operation(disconnect, None), OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front)
                }
            };

        assert_ne!(op_id, 0);

        if let Some(check_operation) = self.operations.get(&op_id) {
            if !self.operation_packet_passes_offline_queue_policy(&check_operation.packet) {
                debug!("[{} ms] handle_user_event - operation {} failed by offline queue policy", self.elapsed_time_ms, op_id);
                let _ = self.complete_operation_as_failure(op_id, Mqtt5Error::OfflineQueuePolicyFailed);
                return;
            }
        }

        debug!("[{} ms] handle_user_event - queuing operation with id {} into {} of {} queue", self.elapsed_time_ms, op_id, position, queue);
        self.enqueue_operation(op_id, queue, position);

        self.log_operational_state();
    }

    pub(crate) fn get_next_service_timepoint(&mut self, current_time: &Instant) -> Option<Instant> {
        self.update_internal_clock(current_time);

        let next_service_time =
            match self.state {
                OperationalStateType::Disconnected => { self.get_next_service_timepoint_disconnected() }
                OperationalStateType::PendingConnack => { self.get_next_service_timepoint_pending_connack() }
                OperationalStateType::Connected => { self.get_next_service_timepoint_connected() }
                OperationalStateType::PendingDisconnect => { self.get_next_service_timepoint_pending_disconnect() }
                OperationalStateType::Halted => { None }
            };

        if let Some(next_timepoint) = &next_service_time {
            debug!("[{} ms] get_next_service_timepoint - state {}, target_elapsed_time: {} ms", self.elapsed_time_ms, self.state, self.get_elapsed_millis(next_timepoint));
        } else {
            debug!("[{} ms] get_next_service_timepoint - state {}, target_elapsed_time: NEVER", self.elapsed_time_ms, self.state);
        }

        next_service_time
    }

    pub(crate) fn reset(&mut self, current_time: &Instant) {
        self.update_internal_clock(current_time);

        if self.state != OperationalStateType::Disconnected {
            self.state = OperationalStateType::Halted;
        }

        let operations : Vec<u64> = self.operations.keys().copied().collect();
        for id in operations {
            let _ = self.complete_operation_as_failure(id, Mqtt5Error::OperationalStateReset);
        }

        self.pending_write_completion = false;
        self.operations.clear();
        self.operation_ack_timeouts.clear();
        self.user_operation_queue.clear();
        self.resubmit_operation_queue.clear();
        self.high_priority_operation_queue.clear();
        self.current_operation = None;
        self.qos2_incomplete_incoming_publishes.clear();
        self.allocated_packet_ids.clear();
        self.pending_publish_operations.clear();
        self.pending_non_publish_operations.clear();
        self.pending_write_completion_operations.clear();
        self.current_settings = None;
        self.next_packet_id = 1;
        self.has_connected_successfully = false;
        self.next_ping_timepoint = None;
        self.ping_timeout_timepoint = None;
        self.connack_timeout_timepoint = None;
    }

    // Private Implementation

    fn operation_packet_passes_offline_queue_policy(&self, packet: &MqttPacket) -> bool {
        if self.state == OperationalStateType::Connected {
            return true;
        }

        does_packet_pass_offline_queue_policy(packet, &self.config.offline_queue_policy)
    }

    fn log_debug(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "OperationalState: {{")?;
        writeln!(f, "  state: {}", self.state)?;
        writeln!(f, "  elapsed_time_ms: {}", self.elapsed_time_ms)?;
        writeln!(f, "  pending_write_completion: {}", self.pending_write_completion)?;
        writeln!(f, "  operations: {} items", self.operations.len())?;
        writeln!(f, "  operation_ack_timeouts: {} timeouts pending", self.operation_ack_timeouts.len())?;
        writeln!(f, "  user_operation_queue: {} items", self.user_operation_queue.len())?;
        writeln!(f, "  resubmit_operation_queue: {} items", self.resubmit_operation_queue.len())?;
        writeln!(f, "  high_priority_operation_queue: {} items", self.high_priority_operation_queue.len())?;
        writeln!(f, "  current_operation: {:?}", self.current_operation)?;
        writeln!(f, "  qos2_incomplete_incoming_publishes: {} operations", self.qos2_incomplete_incoming_publishes.len())?;
        writeln!(f, "  allocated_packet_ids: {} ids", self.allocated_packet_ids.len())?;
        writeln!(f, "  pending_publish_operations: {} operations", self.pending_publish_operations.len())?;
        writeln!(f, "  pending_non_publish_operations: {} operations", self.pending_non_publish_operations.len())?;
        writeln!(f, "  pending_write_completion_operations: {} operations", self.pending_write_completion_operations.len())?;
        writeln!(f, "  next_operation_id: {}", self.next_operation_id)?;
        writeln!(f, "  next_packet_id: {}", self.next_packet_id)?;
        write!(f, "}}")?;

        Ok(())
    }

    fn log_trace(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "OperationalState: {{")?;
        writeln!(f, "  state: {}", self.state)?;
        writeln!(f, "  elapsed_time_ms: {}", self.elapsed_time_ms)?;
        writeln!(f, "  pending_write_completion: {}", self.pending_write_completion)?;
        writeln!(f, "  operations: {{")?;
        self.operations.iter().for_each(|(id, operation)| {
            let _ = writeln!(f, "    ({}, {})", *id, mqtt_packet_to_str(&operation.packet));
        });
        writeln!(f, "  }}")?;
        writeln!(f, "  operation_ack_timeouts: {} timeouts pending", self.operation_ack_timeouts.len())?;
        writeln!(f, "  user_operation_queue: {:?}", self.user_operation_queue)?;
        writeln!(f, "  resubmit_operation_queue: {:?}", self.resubmit_operation_queue)?;
        writeln!(f, "  high_priority_operation_queue: {:?}", self.high_priority_operation_queue)?;
        writeln!(f, "  current_operation: {:?}", self.current_operation)?;
        writeln!(f, "  qos2_incomplete_incoming_publishes: {:?}", self.qos2_incomplete_incoming_publishes)?;
        writeln!(f, "  allocated_packet_ids: {{")?;
        self.allocated_packet_ids.iter().for_each(|(packet_id, operation_id)| {
            let _ = writeln!(f, "    ({}, {})", *packet_id, *operation_id);
        });
        writeln!(f, "  }}")?;
        writeln!(f, "  pending_publish_operations: {{")?;
        self.pending_publish_operations.iter().for_each(|(packet_id, operation_id)| {
            let _ = writeln!(f, "    ({}, {})", *packet_id, *operation_id);
        });
        writeln!(f, "  }}")?;
        writeln!(f, "  pending_non_publish_operations: {{")?;
        self.pending_non_publish_operations.iter().for_each(|(packet_id, operation_id)| {
            let _ = writeln!(f, "    ({}, {})", *packet_id, *operation_id);
        });
        writeln!(f, "  }}")?;
        writeln!(f, "  pending_write_completion_operations: {:?}", self.pending_write_completion_operations)?;
        writeln!(f, "  next_operation_id: {}", self.next_operation_id)?;
        writeln!(f, "  next_packet_id: {}", self.next_packet_id)?;
        write!(f, "}}")?;

        Ok(())
    }

    fn log_operational_state(&self) {
        let level = log::max_level();
        match level {
            LevelFilter::Debug => {
                debug!("{}", self);
            }
            LevelFilter::Trace => {
                trace!("{}", self);
            }
            _ => {}
        }
    }

    fn update_internal_clock(&mut self, current_time: &Instant) {
        self.current_time = *current_time;
        self.elapsed_time_ms = (*current_time - self.config.base_timestamp).as_millis();
    }

    fn get_elapsed_millis(&self, timepoint: &Instant) -> u128 {
        (*timepoint - self.config.base_timestamp).as_millis()
    }

    fn partition_operation_queue_by_queue_policy(&self, queue: &VecDeque<u64>, policy: &OfflineQueuePolicy) -> (VecDeque<u64>, VecDeque<u64>) {
        partition_operations_by_queue_policy(queue.iter().filter(|id| {
            self.operations.get(*id).is_some()
        }).map(|id| {
            (*id, &*self.operations.get(id).unwrap().packet)
        }), policy)
    }

    fn should_retain_high_priority_operation(&self, id: u64) -> bool {
        if let Some(operation) = self.operations.get(&id) {
            if operation.qos2_pubrel.is_some() {
                return true;
            }
        }

        false
    }

    fn partition_high_priority_queue_for_disconnect<T>(&self, iterator: T) -> (VecDeque<u64>, VecDeque<u64>) where T : Iterator<Item = u64> {
        let mut retained = VecDeque::new();
        let mut rejected = VecDeque::new();

        iterator.for_each(|id| {
            if self.should_retain_high_priority_operation(id) {
                retained.push_back(id);
            } else {
                rejected.push_back(id);
            }
        });

        (retained, rejected)
    }

    fn apply_disconnect_completion(&mut self, operation: &MqttOperation) -> Mqtt5Result<()> {
        if let MqttPacket::Disconnect(_) = &*operation.packet {
            if self.state == OperationalStateType::PendingDisconnect {
                self.state = OperationalStateType::Halted;
            }
            info!("[{} ms] apply_disconnect_completion - user-requested disconnect operation {} completed", self.elapsed_time_ms, operation.id);
            return Err(Mqtt5Error::UserInitiatedDisconnect);
        }

        Ok(())
    }

    fn complete_operation_as_success(&mut self, id : u64, completion_result: Option<OperationResponse>) -> Mqtt5Result<()> {
        let operation_option = self.operations.remove(&id);
        if operation_option.is_none() {
            error!("[{} ms] complete_operation_as_success - operation id {} does not exist", self.elapsed_time_ms, id);
            return Err(Mqtt5Error::InternalStateError);
        }

        let operation = operation_option.unwrap();
        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
            self.pending_publish_operations.remove(&packet_id);
            self.pending_non_publish_operations.remove(&packet_id);
        }

        self.apply_ping_extension_on_operation_success(&operation);
        self.apply_disconnect_completion(&operation)?;

        if operation.options.is_none() {
            info!("[{} ms] complete_operation_as_success - internal {} operation {} completed", self.elapsed_time_ms, mqtt_packet_to_str(&operation.packet), id);
            return Ok(())
        }

        info!("[{} ms] complete_operation_as_success - user {} operation {} completed", self.elapsed_time_ms, mqtt_packet_to_str(&operation.packet), id);
        complete_operation_with_result(&mut operation.options.unwrap(), completion_result)
    }

    fn complete_operation_as_failure(&mut self, id : u64, error: Mqtt5Error) -> Mqtt5Result<()> {
        let operation_option = self.operations.remove(&id);
        if operation_option.is_none() {
            // not fatal; the limits of the priority queue implementation used for timeouts
            // can result in situations where we try to fail an operation that has already
            // completed
            warn!("[{} ms] complete_operation_as_failure ({}) - operation id {} does not exist", self.elapsed_time_ms, error, id);
            return Ok(())
        }

        let operation = operation_option.unwrap();
        if let Some(packet_id) = operation.packet_id {
            self.allocated_packet_ids.remove(&packet_id);
            self.pending_publish_operations.remove(&packet_id);
            self.pending_non_publish_operations.remove(&packet_id);
        }

        self.apply_disconnect_completion(&operation)?;

        if operation.options.is_none() {
            info!("[{} ms] complete_operation_as_failure ({}) - internal {} operation {} completed", self.elapsed_time_ms, error, mqtt_packet_to_str(&operation.packet), id);
            return Ok(())
        }

        info!("[{} ms] complete_operation_as_failure ({}) - user {} operation {} completed", self.elapsed_time_ms, error, mqtt_packet_to_str(&operation.packet), id);
        complete_operation_with_error(&mut operation.options.unwrap(), error)
    }

    fn complete_operation_sequence_as_failure<T>(&mut self, iterator: T, error: Mqtt5Error) -> Mqtt5Result<()> where T : Iterator<Item = u64> {
        #[allow(clippy::manual_try_fold)]
        iterator.fold(
            Ok(()),
            |res, item| {
                fold_mqtt5_result(res, self.complete_operation_as_failure(item, error))
            }
        )
    }

    fn complete_operation_sequence_as_empty_success<T>(&mut self, iterator: T) -> Mqtt5Result<()> where T : Iterator<Item = u64> {
        #[allow(clippy::manual_try_fold)]
        iterator.fold(
            Ok(()),
            |res, item| {
                fold_mqtt5_result(res, self.complete_operation_as_success(item, None))
            }
        )
    }

    fn handle_network_event_connection_opened(&mut self, context: &NetworkEventContext) -> Mqtt5Result<()> {
        if self.state != OperationalStateType::Disconnected {
            error!("[{} ms] handle_network_event_connection_opened - called in invalid state", self.elapsed_time_ms);
            self.change_state(OperationalStateType::Halted);
            return Err(Mqtt5Error::InternalStateError);
        }

        info!("[{} ms] handle_network_event_connection_opened", self.elapsed_time_ms);
        self.change_state(OperationalStateType::PendingConnack);
        self.current_operation = None;
        self.pending_write_completion = false;
        self.decoder.reset_for_new_connection();

        // Queue up a Connect packet
        let connect = self.create_connect();
        let connect_op_id = self.create_operation(connect, None);

        self.enqueue_operation(connect_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front);

        let connack_timeout = context.current_time + self.config.connack_timeout;

        debug!("[{} ms] handle_network_event_connection_opened - setting connack timeout to {} ms", self.elapsed_time_ms, self.get_elapsed_millis(&connack_timeout));
        self.connack_timeout_timepoint = Some(connack_timeout);

        Ok(())
    }

    fn apply_connection_closed_to_current_operation(&mut self) -> Mqtt5Result<()> {
        if let Some(id) = self.current_operation {
            if let Some(operation) = self.operations.get(&id) {
                match &*operation.packet {
                    MqttPacket::Subscribe(_) | MqttPacket::Unsubscribe(_) => {
                        if does_packet_pass_offline_queue_policy(&operation.packet, &self.config.offline_queue_policy) {
                            self.user_operation_queue.push_front(id);
                        } else {
                            self.complete_operation_as_failure(id, Mqtt5Error::OfflineQueuePolicyFailed)?;
                        }
                    }
                    MqttPacket::Publish(publish) => {
                        if publish.duplicate {
                            self.resubmit_operation_queue.push_front(id);
                        } else if publish.qos == QualityOfService::ExactlyOnce && operation.qos2_pubrel.is_some() {
                            self.high_priority_operation_queue.push_front(id);
                        } else if does_packet_pass_offline_queue_policy(&operation.packet, &self.config.offline_queue_policy) {
                            self.user_operation_queue.push_front(id);
                        } else {
                            self.complete_operation_as_failure(id, Mqtt5Error::OfflineQueuePolicyFailed)?;
                        }
                    }
                    _ => {
                        self.complete_operation_as_failure(id, Mqtt5Error::ConnectionClosed)?;
                    }
                }
            }
        }

        self.current_operation = None;

        Ok(())
    }

    fn handle_network_event_connection_closed(&mut self, _: &mut NetworkEventContext) -> Mqtt5Result<()> {
        if self.state == OperationalStateType::Disconnected {
            error!("[{} ms] handle_network_event_connection_closed - called in invalid state", self.elapsed_time_ms);
            return Err(Mqtt5Error::InternalStateError);
        }

        info!("[{} ms] handle_network_event_connection_closed", self.elapsed_time_ms);
        self.change_state(OperationalStateType::Disconnected);
        self.connack_timeout_timepoint = None;
        self.next_ping_timepoint = None;
        self.ping_timeout_timepoint = None;
        self.operation_ack_timeouts.clear();

        self.apply_connection_closed_to_current_operation()?;

        let mut result : Mqtt5Result<()> = Ok(());
        let mut completions : VecDeque<u64> = VecDeque::new();

        /*
         * high priority operations are processed as follows:
         *
         *   puback, pingreq, pubrec, pubcomp, disconnect can all be failed without consequence
         *
         *   pubrels are left alone but not requeued.  When the pending_publish table is
         *   processed a few lines further down, the associated operation will be added to the
         *   resubmit queue.
         */
        mem::swap(&mut completions, &mut self.high_priority_operation_queue);
        let (_, failures) = self.partition_high_priority_queue_for_disconnect(completions.into_iter());

        result = fold_mqtt5_result(result, self.complete_operation_sequence_as_failure(failures.into_iter(), Mqtt5Error::ConnectionClosed));

        /*
         * write completion pending operations can be processed immediately and either failed
         * if they fail the offline queue policy or re-queued
         */
        let mut completions : VecDeque<u64> = VecDeque::new();
        mem::swap(&mut completions, &mut self.pending_write_completion_operations);

        let (mut retained, rejected) = self.partition_operation_queue_by_queue_policy(&completions, &self.config.offline_queue_policy);

        /* keep the ones that pass policy (qos 0 publish under once case) */
        self.user_operation_queue.append(&mut retained);

        /* fail everything else */
        result = fold_mqtt5_result(result, self.complete_operation_sequence_as_failure(rejected.into_iter(), Mqtt5Error::OfflineQueuePolicyFailed));

        /*
         * unacked operations are processed as follows:
         *
         *   subscribes and unsubscribes have the offline queue policy applied.  If they fail, the
         *   operation is failed, otherwise it gets put back in the user queue
         *
         *   publish and pubrel get moved to the resubmit queue.  They'll be re-checked on the
         *   next successful connection and either have the offline queue policy applied (if no
         *   session is found) or stay in the resubmit queue.
         */

        /*
         * qos1+ publishes: mark as duplicate and add to end of resubmit queue
         */
        let mut unacked_publish_table = HashMap::new();
        mem::swap(&mut unacked_publish_table, &mut self.pending_publish_operations);

        unacked_publish_table.into_iter().for_each(|(_, id) |{
            self.set_publish_duplicate_flag(id, true);
            self.resubmit_operation_queue.push_back(id);
        });

        /*
         * subscribe/unsubscribe to the user queue
         */
        let mut unacked_sub_unsub_table = HashMap::new();
        mem::swap(&mut unacked_sub_unsub_table, &mut self.pending_non_publish_operations);

        unacked_sub_unsub_table.into_iter().for_each(|(_, id) |{
            self.user_operation_queue.push_front(id);
        });

        /*
         * apply the offline policy to user queue operations
         */
        let mut user_move : VecDeque<u64> = VecDeque::new();
        mem::swap(&mut user_move, &mut self.user_operation_queue);

        let (mut retained_user, rejected_user) = self.partition_operation_queue_by_queue_policy(&user_move, &self.config.offline_queue_policy);
        result = fold_mqtt5_result(result, self.complete_operation_sequence_as_failure(rejected_user.into_iter(), Mqtt5Error::OfflineQueuePolicyFailed));

        self.user_operation_queue.append(&mut retained_user);

        result
    }

    fn handle_network_event_write_completion(&mut self, _: &NetworkEventContext) -> Mqtt5Result<()> {
        if self.state == OperationalStateType::Halted || self.state == OperationalStateType::Disconnected {
            error!("[{} ms] handle_network_event_write_completion - called in invalid state", self.elapsed_time_ms);
            return Err(Mqtt5Error::InternalStateError);
        }

        if !self.pending_write_completion {
            error!("[{} ms] handle_network_event_write_completion - called with no pending completion", self.elapsed_time_ms);
            self.change_state(OperationalStateType::Halted);

            return Err(Mqtt5Error::InternalStateError);
        }

        debug!("[{} ms] handle_network_event - write completion", self.elapsed_time_ms);

        self.pending_write_completion = false;

        let mut completions : VecDeque<u64> = VecDeque::new();
        mem::swap(&mut completions, &mut self.pending_write_completion_operations);
        let result : Mqtt5Result<()> = self.complete_operation_sequence_as_empty_success(completions.iter().copied());

        result
    }

    fn change_state(&mut self, next_state: OperationalStateType) {
        debug!("[{} ms] change_state - transitioning from {} to {}", self.elapsed_time_ms, self.state, next_state);
        self.state = next_state;
    }

    fn is_connect_packet(&self, id: u64) -> bool {
        if let Some(operation) = self.operations.get(&id) {
            return mqtt_packet_to_packet_type(&operation.packet) == PacketType::Connect;
        }

        false
    }

    fn is_connect_in_queue(&self) -> bool {
        self.high_priority_operation_queue.iter().any(|id| self.is_connect_packet(*id))
    }

    fn handle_network_event_incoming_data(&mut self, context: &mut NetworkEventContext, data: &[u8]) -> Mqtt5Result<()> {
        if self.state == OperationalStateType::Disconnected || self.state == OperationalStateType::Halted {
            error!("[{} ms] handle_network_event_incoming_data - called in invalid state", self.elapsed_time_ms);
            return Err(Mqtt5Error::InternalStateError);
        }

        if self.state == OperationalStateType::PendingConnack && self.is_connect_in_queue() {
            error!("[{} ms] handle_network_event_incoming_data - data received before CONNECT sent", self.elapsed_time_ms);
            self.change_state(OperationalStateType::Halted);
            return Err(Mqtt5Error::ProtocolError);
        }

        debug!("[{} ms] handle_network_event_incoming_data received {} bytes", self.elapsed_time_ms, data.len());
        let mut decoded_packets = VecDeque::new();
        let mut decode_context = DecodingContext {
            maximum_packet_size: self.get_maximum_incoming_packet_size(),
            decoded_packets: &mut decoded_packets
        };

        let decode_result = self.decoder.decode_bytes(data, &mut decode_context);
        if decode_result.is_err() {
            error!("[{} ms] handle_network_event_incoming_data - decode failure", self.elapsed_time_ms);
            self.change_state(OperationalStateType::Halted);
            return decode_result;
        }

        for mut packet in decoded_packets {
            if let MqttPacket::Publish(publish) = &mut(*packet) {
                if self.inbound_alias_resolver.resolve_topic_alias(&publish.topic_alias, &mut publish.topic).is_err() {
                    error!("[{} ms] handle_network_event_incoming_data - topic alias resolution failure", self.elapsed_time_ms);
                    return Err(Mqtt5Error::ProtocolError);
                }
            }

            let mut validation_context = InboundValidationContext {
                negotiated_settings : None
            };

            if let Some(settings) = &self.current_settings {
                validation_context.negotiated_settings = Some(settings);
            }

            let validation_result = validate_packet_inbound_internal(&packet, &validation_context);
            if validation_result.is_err() {
                error!("[{} ms] handle_network_event_incoming_data - incoming packet validation failure", self.elapsed_time_ms);
                self.change_state(OperationalStateType::Halted);
                return validation_result;
            }

            let handler_result = self.handle_packet(packet, context);
            if handler_result.is_err() {
                error!("[{} ms] handle_network_event_incoming_data - packet handling failure", self.elapsed_time_ms);
                self.change_state(OperationalStateType::Halted);
                return handler_result;
            }
        }

        Ok(())
    }

    // blocks packet processing if the next packet is a qos1+ publish and
    // we are at the negotiated limit for unacknowledged qos1+ publishes.  This is technically
    // not spec-compliant because the spec requires receive maximum to not block other non-publish
    // packets from going out.  It is my opinion that the intent of that requirement was to
    // keep acks, pings, disconnects, auth all flowing while at the maximum.  The fact that
    // subscribes and unsubscribes are also blocked does not affect the user contract in a
    // negative way.  I personally think the creators used a slightly-over-aggressive simple
    // rule here because expressing this otherwise leads to a wall of text like this.
    fn does_operation_pass_receive_maximum_flow_control(&self, id: u64) -> bool {
        if let Some(settings) = &self.current_settings {
            if self.pending_publish_operations.len() >= settings.receive_maximum_from_server as usize {
                if let Some(operation) = self.operations.get(&id) {
                    if let MqttPacket::Publish(publish) = &*operation.packet {
                        if publish.qos != QualityOfService::AtMostOnce {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }

    fn dequeue_operation(&mut self, mode: OperationalQueueServiceMode) -> Option<u64> {
        if !self.high_priority_operation_queue.is_empty() {
            return Some(self.high_priority_operation_queue.pop_front().unwrap());
        }

        if mode != OperationalQueueServiceMode::HighPriorityOnly {
            if !self.resubmit_operation_queue.is_empty() {
                if self.does_operation_pass_receive_maximum_flow_control(*self.resubmit_operation_queue.front().unwrap()) {
                    return Some(self.resubmit_operation_queue.pop_front().unwrap());
                } else {
                    return None;
                }
            }

            if !self.user_operation_queue.is_empty() {
                if self.does_operation_pass_receive_maximum_flow_control(*self.user_operation_queue.front().unwrap()) {
                    return Some(self.user_operation_queue.pop_front().unwrap());
                } else {
                    return None;
                }
            }
        }

        None
    }

    fn compute_outbound_alias_resolution(&self, packet: &MqttPacket) -> OutboundAliasResolution {
        if let MqttPacket::Publish(publish) = packet {
            return self.outbound_alias_resolver.borrow_mut().resolve_and_apply_topic_alias(&publish.topic_alias, &publish.topic);
        }

        OutboundAliasResolution{ ..Default::default() }
    }

    fn get_next_ack_timeout(&mut self) -> Option<u64> {
        if let Some(reverse_record) = self.operation_ack_timeouts.peek() {
            let record = &reverse_record.0;
            if record.timeout <= self.current_time {
                return Some(record.id);
            }
        }

        None
    }

    fn process_ack_timeouts(&mut self) -> Mqtt5Result<()> {
        let mut result = Ok(());

        while let Some(id) = self.get_next_ack_timeout() {
            self.operation_ack_timeouts.pop();
            result = fold_mqtt5_result(result, self.complete_operation_as_failure(id, Mqtt5Error::AckTimeout));
        }

        result
    }

    fn get_operation_timeout_duration(&self, operation: &MqttOperation) -> Option<Duration> {
        match &operation.options {
            Some(MqttOperationOptions::Unsubscribe(unsubscribe_options)) => {
                if let Some(timeout) = &unsubscribe_options.options.timeout {
                    return Some(*timeout);
                }
            }
            Some(MqttOperationOptions::Subscribe(subscribe_options)) => {
                if let Some(timeout) = &subscribe_options.options.timeout {
                    return Some(*timeout);
                }
            }
            Some(MqttOperationOptions::Publish(publish_options)) => {
                if let Some(timeout) = &publish_options.options.timeout {
                    return Some(*timeout);
                }
            }
            _ => {}
        }

        None
    }

    fn start_operation_ack_timeout(&mut self, id: u64, now: Instant) {
        let mut timeout_duration_option : Option<Duration> = None;
        if let Some(operation) = self.operations.get(&id) {
            timeout_duration_option = self.get_operation_timeout_duration(operation);
        }

        if let Some(timeout_duration) = timeout_duration_option {
            let timeout = now + timeout_duration;

            let timeout_record = OperationTimeoutRecord {
                id,
                timeout
            };

            self.operation_ack_timeouts.push(Reverse(timeout_record));
        }
    }

    fn apply_ping_extension_on_operation_success(&mut self, operation: &MqttOperation) {
        let mut extension_base_option : Option<Instant> = None;

        match &*operation.packet {
            MqttPacket::Subscribe(_) | MqttPacket::Unsubscribe(_) => {
                extension_base_option = operation.ping_extension_base_timepoint;
            }
            MqttPacket::Publish(publish) => {
                if publish.qos != QualityOfService::AtMostOnce {
                    extension_base_option = operation.ping_extension_base_timepoint;
                }
            }
            _ => {}
        }

        if let (Some(extension_base), Some(settings)) = (extension_base_option, &self.current_settings) {
            let potential_extension = extension_base + Duration::from_secs(settings.server_keep_alive as u64);
            if self.next_ping_timepoint.is_some() && potential_extension > self.next_ping_timepoint.unwrap() {
                self.next_ping_timepoint = Some(potential_extension);
            }
        }
    }

    fn on_current_operation_fully_written(&mut self, now: Instant) {
        let operation = self.operations.get_mut(&self.current_operation.unwrap()).unwrap();
        let packet = &*operation.packet;
        match packet {
            MqttPacket::Subscribe(subscribe) => {
                self.pending_non_publish_operations.insert(subscribe.packet_id, operation.id);
            }
            MqttPacket::Unsubscribe(unsubscribe) => {
                self.pending_non_publish_operations.insert(unsubscribe.packet_id, operation.id);
            }
            MqttPacket::Publish(publish) => {
                if publish.qos == QualityOfService::AtMostOnce {
                    self.pending_write_completion_operations.push_back(operation.id);
                } else {
                    self.pending_publish_operations.insert(publish.packet_id, operation.id);
                }
            }
            MqttPacket::Disconnect(_) => {
                self.state = OperationalStateType::PendingDisconnect;
                self.pending_write_completion_operations.push_back(operation.id);
            }
            _ => {
                self.pending_write_completion_operations.push_back(operation.id);
            }
        }

        operation.ping_extension_base_timepoint = Some(now);

        let id = operation.id;
        self.start_operation_ack_timeout(id, now);

        self.current_operation = None;
    }

    fn service_disconnected(&mut self, _: &mut ServiceContext) -> Mqtt5Result<()> {
        debug!("[{} ms] service_disconnected", self.elapsed_time_ms);
        Ok(())
    }

    fn service_queue(&mut self, context: &mut ServiceContext, mode: OperationalQueueServiceMode) -> Mqtt5Result<()> {
        let to_socket_length = context.to_socket.len();

        while self.state == OperationalStateType::PendingConnack || self.state == OperationalStateType::Connected {
            if self.current_operation.is_none() {
                self.current_operation = self.dequeue_operation(mode);
                if self.current_operation.is_none() {
                    debug!("[{} ms] service_queue - no operations ready for processing", self.elapsed_time_ms);
                    return Ok(())
                }

                let current_operation_id = self.current_operation.unwrap();
                debug!("[{} ms] service_queue - operation {} dequeued for processing", self.elapsed_time_ms, current_operation_id);
                if !self.operations.contains_key(&current_operation_id) {
                    warn!("[{} ms] service_queue - operation {} does not exist", self.elapsed_time_ms, current_operation_id);
                    self.current_operation = None;
                    continue;
                }

                self.acquire_packet_id_for_operation(current_operation_id)?;

                let operation = self.operations.get(&current_operation_id).unwrap();
                let mut packet = &*operation.packet;
                if let Some(pubrel) = &operation.qos2_pubrel {
                    packet = &**pubrel;
                }

                let outbound_alias_resolution = self.compute_outbound_alias_resolution(packet);

                let mut validation_context = OutboundValidationContext {
                    negotiated_settings : None,
                    connect_options: Some(&self.config.connect_options),
                    outbound_alias_resolution: Some(outbound_alias_resolution)
                };

                if let Some(settings) = &self.current_settings {
                    validation_context.negotiated_settings = Some(settings);
                }

                if let Err(error) = validate_packet_outbound_internal(packet, &validation_context) {
                    warn!("[{} ms] service_queue - {} operation {} failed last-chance validation", self.elapsed_time_ms, mqtt_packet_to_str(packet), current_operation_id);
                    self.current_operation = None;
                    self.complete_operation_as_failure(current_operation_id, error)?;
                    continue;
                }

                let encode_context = EncodingContext {
                    outbound_alias_resolution
                };

                debug!("[{} ms] service_queue - operation {} submitted to encoder for setup", self.elapsed_time_ms, current_operation_id);
                self.encoder.reset(packet, &encode_context)?;
            }

            let packet = &self.operations.get(&self.current_operation.unwrap()).unwrap().packet;


            let encode_result = self.encoder.encode(packet, context.to_socket)?;
            if context.to_socket.len() != to_socket_length {
                self.pending_write_completion = true;
            }

            if encode_result == EncodeResult::Complete {
                debug!("[{} ms] service_queue - operation {} encoding complete", self.elapsed_time_ms, self.current_operation.unwrap());
                self.on_current_operation_fully_written(context.current_time);
            } else {
                debug!("[{} ms] service_queue - operation {} encoding still in progress", self.elapsed_time_ms, self.current_operation.unwrap());
                return Ok(())
            }
        }

        Ok(())
    }

    fn service_pending_connack(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        debug!("[{} ms] service_pending_connack", self.elapsed_time_ms);

        if context.current_time >= self.connack_timeout_timepoint.unwrap() {
            error!("[{} ms] service_pending_connack - connack timeout exceeded", self.elapsed_time_ms);
            return Err(Mqtt5Error::ConnackTimeout);
        }

        self.service_queue(context, OperationalQueueServiceMode::HighPriorityOnly)?;

        Ok(())
    }

    fn service_keep_alive(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        if let Some(ping_timeout) = &self.ping_timeout_timepoint {
            if &context.current_time >= ping_timeout {
                error!("[{} ms] service_keep_alive - keep alive timeout exceeded", self.elapsed_time_ms);
                return Err(Mqtt5Error::PingTimeout);
            }
        } else if let Some(next_ping) = &self.next_ping_timepoint {
            if &context.current_time >= next_ping {
                debug!("[{} ms] service_keep_alive - next ping time reached, sending ping", self.elapsed_time_ms);
                let ping = Box::new(MqttPacket::Pingreq(PingreqPacket{}));
                let ping_op_id = self.create_operation(ping, None);

                self.enqueue_operation(ping_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Front);

                self.ping_timeout_timepoint = Some(context.current_time + self.config.ping_timeout);

                let server_keep_alive = self.current_settings.as_ref().unwrap().server_keep_alive as u64;
                if server_keep_alive > 0 {
                    self.next_ping_timepoint = Some(context.current_time + Duration::from_secs(server_keep_alive));
                }
            }
        }

        Ok(())
    }

    fn service_connected(&mut self, context: &mut ServiceContext) -> Mqtt5Result<()> {
        debug!("[{} ms] service_connected", self.elapsed_time_ms);

        self.service_keep_alive(context)?;
        self.service_queue(context, OperationalQueueServiceMode::All)?;
        self.process_ack_timeouts()?;

        Ok(())
    }

    fn service_pending_disconnect(&mut self, _: &mut ServiceContext) -> Mqtt5Result<()> {
        debug!("[{} ms] service_pending_disconnect", self.elapsed_time_ms);

        self.process_ack_timeouts()?;

        Ok(())
    }

    fn get_next_service_timepoint_operational_queue(&self, mode: OperationalQueueServiceMode) -> Option<Instant> {
        if self.pending_write_completion {
            return None;
        }

        if !self.high_priority_operation_queue.is_empty() {
            return Some(self.current_time);
        }

        if mode == OperationalQueueServiceMode::All {
            /* receive_maximum flow control check */
            if let Some(settings) = &self.current_settings {
                if self.pending_publish_operations.len() >= settings.receive_maximum_from_server as usize {
                    let mut head = self.resubmit_operation_queue.front();
                    if head.is_none() {
                        head = self.user_operation_queue.front();
                    }

                    if let Some(head_id) = head {
                        if let Some(operation) = self.operations.get(head_id) {
                            if let MqttPacket::Publish(publish) = &*operation.packet {
                                if publish.qos != QualityOfService::AtMostOnce {
                                    return None;
                                }
                            }
                        }
                    }
                }
            }

            if !self.resubmit_operation_queue.is_empty() || !self.user_operation_queue.is_empty() {
                return Some(self.current_time);
            }
        }

        None
    }

    fn get_next_service_timepoint_disconnected(&self) -> Option<Instant> {
        None
    }

    fn get_next_service_timepoint_pending_connack(&self) -> Option<Instant> {
        fold_timepoint(&self.get_next_service_timepoint_operational_queue(OperationalQueueServiceMode::HighPriorityOnly), &self.connack_timeout_timepoint.unwrap())
    }

    fn get_next_service_timepoint_connected(&self) -> Option<Instant> {
        let mut next_service_time: Option<Instant> = fold_optional_timepoint_min(&None, &self.ping_timeout_timepoint);

        if let Some(ack_timeout) = self.operation_ack_timeouts.peek() {
            next_service_time = fold_timepoint(&next_service_time, &ack_timeout.0.timeout);
        }

        if self.pending_write_completion {
            return next_service_time;
        }

        next_service_time = fold_optional_timepoint_min(&next_service_time, &self.next_ping_timepoint);

        fold_optional_timepoint_min(&self.get_next_service_timepoint_operational_queue( OperationalQueueServiceMode::All), &next_service_time)
    }

    fn get_next_service_timepoint_pending_disconnect(&self) -> Option<Instant> {
        let mut next_service_time = self.get_next_service_timepoint_operational_queue(OperationalQueueServiceMode::HighPriorityOnly);

        if let Some(ack_timeout) = self.operation_ack_timeouts.peek() {
            next_service_time = fold_timepoint(&next_service_time, &ack_timeout.0.timeout);
        }

        next_service_time
    }

    fn unbind_operation_packet_id(&mut self, id: u64) {
        if let Some(operation) = self.operations.get_mut(&id) {
            if let Some(packet_id) = operation.packet_id {
                self.allocated_packet_ids.remove(&packet_id);
                operation.unbind_packet_id();
            }
        }
    }

    fn clear_qos2_state(&mut self, id: u64) {
        if let Some(operation) = self.operations.get_mut(&id) {
            operation.qos2_pubrel = None;
        }
    }

    fn set_publish_duplicate_flag(&mut self, id: u64, value: bool) {
        if let Some(operation) = self.operations.get_mut(&id) {
            if let MqttPacket::Publish(publish) = &mut *operation.packet {
                debug!("[{} ms] set_publish_duplicate_flag - setting publish operation {} duplicate field to {}", self.elapsed_time_ms, id, value);
                publish.duplicate = value;
            }
        }
    }

    fn apply_session_present_to_connection(&mut self, session_present: bool) -> Mqtt5Result<()> {
        let mut result = Ok(());

        if !session_present {
            info!("[{} ms] apply_session_present_to_connection - no session present", self.elapsed_time_ms);
            /*
             * No session.  Everything in the resubmit queue should be checked against the offline
             * policy and either failed or moved to the user queue.
             */
            let mut resubmit = VecDeque::new();
            std::mem::swap(&mut resubmit, &mut self.resubmit_operation_queue);

            let (mut retained, rejected) = self.partition_operation_queue_by_queue_policy(&resubmit, &self.config.offline_queue_policy);

            /* keep the ones that pass policy */
            retained.iter().for_each(|id| { self.set_publish_duplicate_flag(*id, false) });
            self.user_operation_queue.append(&mut retained);

            /* fail everything else */
            result = self.complete_operation_sequence_as_failure(rejected.into_iter(), Mqtt5Error::OfflineQueuePolicyFailed);

            self.qos2_incomplete_incoming_publishes.clear();
            self.allocated_packet_ids.clear();

            assert!(self.resubmit_operation_queue.is_empty());
        } else {
            info!("[{} ms] apply_session_present_to_connection - successfully rejoined a session", self.elapsed_time_ms);
        }

        // at this point, anything in the user queue is starting over, so drop any packet id
        // associations and reset qos2 publishes
        let mut user_queue = VecDeque::new();
        std::mem::swap(&mut user_queue, &mut self.user_operation_queue);
        user_queue.iter().for_each(|id| {
            self.unbind_operation_packet_id(*id);
            self.clear_qos2_state(*id);
        });
        self.user_operation_queue = user_queue;

        // re-establish submission order after all the shuffling
        sort_operation_deque(&mut self.resubmit_operation_queue);
        sort_operation_deque(&mut self.user_operation_queue);

        assert!(self.high_priority_operation_queue.is_empty());
        assert!(self.pending_publish_operations.is_empty());
        assert!(self.pending_non_publish_operations.is_empty());
        assert!(self.operation_ack_timeouts.is_empty());
        assert!(self.pending_write_completion_operations.is_empty());

        result
    }

    fn handle_connack(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        if let MqttPacket::Connack(connack) = *packet {
            info!("[{} ms] handle_connack - processing CONNACK packet", self.elapsed_time_ms);

            if self.state != OperationalStateType::PendingConnack {
                error!("[{} ms] handle_connack - invalid state to receive a connack", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }

            if connack.reason_code != ConnectReasonCode::Success {
                error!("[{} ms] handle_connack - connection rejected with reason code {}", self.elapsed_time_ms, connect_reason_code_to_str(connack.reason_code));
                context.packet_events.push_back(PacketEvent::Connack(connack));
                return Err(Mqtt5Error::ConnectionRejected);
            }

            validate_connack_packet_inbound_internal(&connack)?;

            self.change_state(OperationalStateType::Connected);
            self.has_connected_successfully = true;

            let settings = build_negotiated_settings(&self.config, &connack, &self.current_settings);
            debug!("[{} ms] handle_connack - negotiated settings: {}", self.elapsed_time_ms, &settings);

            let server_keep_alive = settings.server_keep_alive as u64;
            self.current_settings = Some(settings);
            self.connack_timeout_timepoint = None;
            self.outbound_alias_resolver.borrow_mut().reset_for_new_connection(connack.topic_alias_maximum.unwrap_or(0));
            self.inbound_alias_resolver.reset_for_new_connection();

            self.ping_timeout_timepoint = None;
            if server_keep_alive > 0 {
                self.next_ping_timepoint = Some(context.current_time + Duration::from_secs(server_keep_alive));
            } else {
                self.next_ping_timepoint = None;
            }

            self.apply_session_present_to_connection(connack.session_present)?;

            context.packet_events.push_back(PacketEvent::Connack(connack));

            return Ok(());
        }

        error!("[{} ms] handle_connack - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_pingresp(&mut self) -> Mqtt5Result<()> {
        info!("[{} ms] handle_pingresp - processing PINGRESP packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Connected |  OperationalStateType::PendingDisconnect => {
                if self.ping_timeout_timepoint.is_some() {
                    self.ping_timeout_timepoint = None;
                    Ok(())
                } else {
                    error!("[{} ms] handle_pingresp - no matching PINGREQ", self.elapsed_time_ms);
                    Err(Mqtt5Error::ProtocolError)
                }
            }
            _ => {
                error!("[{} ms] handle_pingresp - invalid state to receive a PINGRESP", self.elapsed_time_ms);
                Err(Mqtt5Error::ProtocolError)
            }
        }
    }

    fn handle_suback(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        info!("[{} ms] handle_suback - processing SUBACK packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_suback - invalid state to receive a SUBACK", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Suback(suback) = *packet {
            let packet_id = suback.packet_id;
            let operation_id_option = self.pending_non_publish_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Subscribe(suback)));
            }

            error!("[{} ms] handle_suback - no matching operation corresponding to SUBACK packet id {}", self.elapsed_time_ms, packet_id);
            return Err(Mqtt5Error::ProtocolError);
        }

        error!("[{} ms] handle_suback - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_unsuback(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        info!("[{} ms] handle_unsuback - processing UNSUBACK packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_unsuback - invalid state to receive an UNSUBACK", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Unsuback(unsuback) = *packet {
            let packet_id = unsuback.packet_id;
            let operation_id_option = self.pending_non_publish_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Unsubscribe(unsuback)));
            }

            error!("[{} ms] handle_unsuback - no matching operation corresponding to UNSUBACK packet id {}", self.elapsed_time_ms, packet_id);
            return Err(Mqtt5Error::ProtocolError);
        }

        error!("[{} ms] handle_unsuback - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_puback(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        info!("[{} ms] handle_puback - processing PUBACK packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_puback - invalid state to receive a PUBACK", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Puback(puback) = *packet {
            let packet_id = puback.packet_id;
            let operation_id_option = self.pending_publish_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Publish(PublishResponse::Qos1(puback))));
            }

            error!("[{} ms] handle_puback - no matching operation corresponding to PUBACK packet id {}", self.elapsed_time_ms, packet_id);
            return Err(Mqtt5Error::ProtocolError);
        }

        error!("[{} ms] handle_puback - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_pubrec(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        info!("[{} ms] handle_pubrec - processing PUBREC packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_pubrec - invalid state to receive a PUBREC", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Pubrec(pubrec) = *packet {
            let packet_id = pubrec.packet_id;
            let operation_id_option = self.pending_publish_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                if pubrec.reason_code as u8 >= 128 {
                    return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Publish(PublishResponse::Qos2(Qos2Response::Pubrec(pubrec)))));
                } else {
                    let operation_option = self.operations.get_mut(operation_id);
                    if let Some(operation) = operation_option {
                        if let MqttPacket::Publish(publish) = &*operation.packet {
                            if publish.qos == QualityOfService::ExactlyOnce {
                                operation.qos2_pubrel = Some(Box::new(MqttPacket::Pubrel(PubrelPacket {
                                    packet_id: pubrec.packet_id,
                                    ..Default::default()
                                })));

                                self.enqueue_operation(*operation_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back);
                                return Ok(());
                            }
                        }

                        error!("[{} ms] handle_pubrec - operation {} corresponding to packet id {} is not a QoS 2 publish", self.elapsed_time_ms, operation_id, packet_id);
                        return Err(Mqtt5Error::ProtocolError);
                    }

                    warn!("[{} ms] handle_pubrec - operation {} corresponding to packet id {} does not exist", self.elapsed_time_ms, operation_id, packet_id);
                    return Ok(());
                }
            }

            error!("[{} ms] handle_pubrec - no matching operation corresponding to PUBREC packet id {}", self.elapsed_time_ms, packet_id);
            return Err(Mqtt5Error::ProtocolError);
        }

        error!("[{} ms] handle_pubrec - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_pubrel(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        info!("[{} ms] handle_pubrel - processing PUBREL packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_pubrel - invalid state to receive a PUBREL", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Pubrel(pubrel) = &*packet {
            self.qos2_incomplete_incoming_publishes.remove(&pubrel.packet_id);

            let pubcomp = Box::new(MqttPacket::Pubcomp(PubcompPacket{
                packet_id: pubrel.packet_id,
                ..Default::default()
            }));
            let pubcomp_op_id = self.create_operation(pubcomp, None);

            self.enqueue_operation(pubcomp_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back);

            return Ok(());
        }

        error!("[{} ms] handle_pubrel - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_pubcomp(&mut self, packet: Box<MqttPacket>) -> Mqtt5Result<()> {
        info!("[{} ms] handle_pubcomp - processing PUBCOMP packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_pubcomp - invalid state to receive a PUBCOMP", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Pubcomp(pubcomp) = *packet {
            let packet_id = pubcomp.packet_id;
            let operation_id_option = self.pending_publish_operations.get(&packet_id);
            if let Some(operation_id) = operation_id_option {
                return self.complete_operation_as_success(*operation_id, Some(OperationResponse::Publish(PublishResponse::Qos2(Qos2Response::Pubcomp(pubcomp)))));
            }

            error!("[{} ms] handle_pubcomp - no matching operation corresponding to PUBCOMP packet id {}", self.elapsed_time_ms, packet_id);
            return Err(Mqtt5Error::ProtocolError);
        }

        error!("[{} ms] handle_pubcomp - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::ProtocolError)
    }

    fn handle_publish(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        info!("[{} ms] handle_publish - processing PUBLISH packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                error!("[{} ms] handle_publish - invalid state to receive a PUBLISH", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Publish(publish) = *packet {
            let packet_id = publish.packet_id;
            let qos = publish.qos;
            match qos {
                QualityOfService::AtMostOnce => {
                    context.packet_events.push_back(PacketEvent::Publish(publish));
                    return Ok(());
                }

                QualityOfService::AtLeastOnce => {
                    context.packet_events.push_back(PacketEvent::Publish(publish));

                    let puback = Box::new(MqttPacket::Puback(PubackPacket{
                        packet_id,
                        ..Default::default()
                    }));
                    let puback_op_id = self.create_operation(puback, None);

                    self.enqueue_operation(puback_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back);

                    return Ok(());
                }

                QualityOfService::ExactlyOnce => {
                    if !self.qos2_incomplete_incoming_publishes.contains(&packet_id) {
                        context.packet_events.push_back(PacketEvent::Publish(publish));
                        self.qos2_incomplete_incoming_publishes.insert(packet_id);
                    }

                    let pubrec = Box::new(MqttPacket::Pubrec(PubrecPacket{
                        packet_id,
                        ..Default::default()
                    }));
                    let pubrec_op_id = self.create_operation(pubrec, None);

                    self.enqueue_operation(pubrec_op_id, OperationalQueueType::HighPriority, OperationalEnqueuePosition::Back);

                    return Ok(());
                }
            }
        }

        error!("[{} ms] handle_publish - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_disconnect(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        info!("[{} ms] handle_disconnect - processing DISCONNECT packet", self.elapsed_time_ms);
        match self.state {
            OperationalStateType::Disconnected | OperationalStateType::PendingConnack => {
                // per spec, the server must always send a CONNACK before a DISCONNECT is valid
                error!("[{} ms] handle_disconnect - invalid state to receive a DISCONNECT", self.elapsed_time_ms);
                return Err(Mqtt5Error::ProtocolError);
            }
            _ => {}
        }

        if let MqttPacket::Disconnect(disconnect) = *packet {
            context.packet_events.push_back(PacketEvent::Disconnect(disconnect));

            return Err(Mqtt5Error::ServerSideDisconnect);
        }

        error!("[{} ms] handle_disconnect - invalid input", self.elapsed_time_ms);
        Err(Mqtt5Error::InternalStateError)
    }

    fn handle_auth(&mut self, _: Box<MqttPacket>, _: &mut NetworkEventContext) -> Mqtt5Result<()> {
        info!("[{} ms] handle_auth - processing AUTH packet", self.elapsed_time_ms);
        Err(Mqtt5Error::Unimplemented)
    }

    fn handle_packet(&mut self, packet: Box<MqttPacket>, context: &mut NetworkEventContext) -> Mqtt5Result<()> {
        match &*packet {
            MqttPacket::Connack(_) => { self.handle_connack(packet, context) }
            MqttPacket::Publish(_) => { self.handle_publish(packet, context) }
            MqttPacket::Pingresp(_) => { self.handle_pingresp() }
            MqttPacket::Disconnect(_) => { self.handle_disconnect(packet, context) }
            MqttPacket::Suback(_) => { self.handle_suback(packet) }
            MqttPacket::Unsuback(_) => { self.handle_unsuback(packet) }
            MqttPacket::Puback(_) => { self.handle_puback(packet) }
            MqttPacket::Pubcomp(_) => { self.handle_pubcomp(packet) }
            MqttPacket::Pubrel(_) => { self.handle_pubrel(packet) }
            MqttPacket::Pubrec(_) => { self.handle_pubrec(packet) }
            MqttPacket::Auth(_) => { self.handle_auth(packet, context) }
            _ => {
                error!("[{} ms] handle_packet - invalid packet type for client received", self.elapsed_time_ms);
                Err(Mqtt5Error::ProtocolError)
            }
        }
    }

    fn get_maximum_incoming_packet_size(&self) -> u32 {
        if let Some(maximum_packet_size) = &self.config.connect_options.maximum_packet_size_bytes {
            return *maximum_packet_size;
        }

        MAXIMUM_VARIABLE_LENGTH_INTEGER as u32
    }

    fn get_queue(&mut self, queue_type: OperationalQueueType) -> &mut VecDeque<u64> {
        match queue_type {
            OperationalQueueType::User => { &mut self.user_operation_queue }
            OperationalQueueType::HighPriority => { &mut self.high_priority_operation_queue }
        }
    }

    fn enqueue_operation(&mut self, id: u64, queue_type: OperationalQueueType, position: OperationalEnqueuePosition) {
        if !self.operations.contains_key(&id) {
            panic!("Attempt to enqueue a non-existent operation");
        }

        debug!("[{} ms] enqueue_operation - operation {} added to {} of queue {} ", self.elapsed_time_ms, id, position, queue_type);
        let queue = self.get_queue(queue_type);
        match position {
            OperationalEnqueuePosition::Front => { queue.push_front(id); }
            OperationalEnqueuePosition::Back => { queue.push_back(id); }
        }
    }

    fn create_operation(&mut self, packet: Box<MqttPacket>, options: Option<MqttOperationOptions>) -> u64 {
        let id = self.next_operation_id;
        self.next_operation_id += 1;

        info!("[{} ms] create_operation - building {} operation with id {}", self.elapsed_time_ms, mqtt_packet_to_str(&packet), id);
        debug!("[{} ms] create_operation - operation {}:\n{}", self.elapsed_time_ms, id, &packet);

        let operation = MqttOperation {
            id,
            packet,
            qos2_pubrel: None,
            packet_id: None,
            options,
            ping_extension_base_timepoint : None,
        };

        self.operations.insert(id, operation);

        id
    }

    fn create_connect(&self) -> Box<MqttPacket> {
        let mut connect = self.config.connect_options.to_connect_packet(self.has_connected_successfully);

        if connect.client_id.is_none() {
            if let Some(settings) = &self.current_settings {
                connect.client_id = Some(settings.client_id.clone());
            }
        }

        Box::new(MqttPacket::Connect(connect))
    }

    fn acquire_free_packet_id(&mut self, operation_id: u64) -> Mqtt5Result<u16> {
        let start_id = self.next_packet_id;
        let mut check_id = start_id;

        loop {
            if self.next_packet_id == u16::MAX {
                self.next_packet_id = 1;
            } else {
                self.next_packet_id += 1;
            }

            if let hash_map::Entry::Vacant(e) = self.allocated_packet_ids.entry(check_id) {
                e.insert(operation_id);
                return Ok(check_id);
            }

            if self.next_packet_id == start_id {
                error!("[{} ms] acquire_packet_id_for_operation - operation {} could not find an unbound packet id", self.elapsed_time_ms, operation_id);
                return Err(Mqtt5Error::PacketIdSpaceExhausted);
            }

            check_id = self.next_packet_id;
        }
    }

    fn acquire_packet_id_for_operation(&mut self, operation_id: u64) -> Mqtt5Result<()> {
        let operation = self.operations.get(&operation_id).unwrap();

        if let Some(packet_id) = operation.packet_id {
            debug!("[{} ms] acquire_packet_id_for_operation - operation {} reusing existing packet id binding: {}", self.elapsed_time_ms, operation_id, packet_id);
            return Ok(());
        }

        match &*operation.packet {
            MqttPacket::Subscribe(_) | MqttPacket::Unsubscribe(_) => { }
            MqttPacket::Publish(publish) => {
                if publish.qos == QualityOfService::AtMostOnce {
                    return Ok(());
                }
            }
            _ => { return Ok(()); }
        }

        let packet_id = self.acquire_free_packet_id(operation_id)?;

        let operation = self.operations.get_mut(&operation_id).unwrap();
        operation.bind_packet_id(packet_id);

        Ok(())
    }

    // Test accessors
    pub(crate) fn get_negotiated_settings(&self) -> &Option<NegotiatedSettings> {
        &self.current_settings
    }
}

fn build_negotiated_settings(config: &OperationalStateConfig, packet: &ConnackPacket, existing_settings: &Option<NegotiatedSettings>) -> NegotiatedSettings {
    let connect = &config.connect_options;

    let final_client_id =
        if packet.assigned_client_identifier.is_some() {
            packet.assigned_client_identifier.as_ref().unwrap().clone()
        } else if connect.client_id.is_some() {
            connect.client_id.as_ref().unwrap().clone()
        } else if let Some(settings) = &existing_settings {
            settings.client_id.clone()
        } else {
            panic!("");
        };

    NegotiatedSettings {
        maximum_qos : packet.maximum_qos.unwrap_or(QualityOfService::ExactlyOnce),
        session_expiry_interval : packet.session_expiry_interval.unwrap_or(connect.session_expiry_interval_seconds.unwrap_or(0)),
        receive_maximum_from_server : packet.receive_maximum.unwrap_or(65535),
        maximum_packet_size_to_server : packet.maximum_packet_size.unwrap_or(MAXIMUM_VARIABLE_LENGTH_INTEGER as u32),
        topic_alias_maximum_to_server : packet.topic_alias_maximum.unwrap_or(0),
        server_keep_alive : packet.server_keep_alive.unwrap_or(connect.keep_alive_interval_seconds.unwrap_or(0)),
        retain_available : packet.retain_available.unwrap_or(true),
        wildcard_subscriptions_available : packet.wildcard_subscriptions_available.unwrap_or(true),
        subscription_identifiers_available : packet.subscription_identifiers_available.unwrap_or(true),
        shared_subscriptions_available : packet.shared_subscriptions_available.unwrap_or(true),
        rejoined_session : packet.session_present,
        client_id : final_client_id
    }
}

fn complete_operation_with_result(operation_options: &mut MqttOperationOptions, completion_result: Option<OperationResponse>) -> Mqtt5Result<()> {
    match operation_options {
        MqttOperationOptions::Publish(publish_options) => {
            let mut publish_response = PublishResponse::Qos0;
            if completion_result.is_some() {
                if let Some(OperationResponse::Publish(publish_result)) = completion_result {
                    publish_response = publish_result;
                } else {
                    return Err(Mqtt5Error::InternalStateError);
                }
            }

            let sender = publish_options.response_sender.take().unwrap();
            let _ = sender.send(Ok(publish_response));

            return Ok(());
        }
        MqttOperationOptions::Subscribe(subscribe_options) => {
            if let OperationResponse::Subscribe(suback) = completion_result.unwrap() {
                let sender = subscribe_options.response_sender.take().unwrap();
                let _ = sender.send(Ok(suback));

                return Ok(());
            }
        }
        MqttOperationOptions::Unsubscribe(unsubscribe_options) => {
            if let OperationResponse::Unsubscribe(unsuback) = completion_result.unwrap() {
                let sender = unsubscribe_options.response_sender.take().unwrap();
                let _ = sender.send(Ok(unsuback));

                return Ok(());
            }
        }
    }

    Err(Mqtt5Error::InternalStateError)
}

fn complete_operation_with_error(operation_options: &mut MqttOperationOptions, error: Mqtt5Error) ->Mqtt5Result<()> {
    match operation_options {
        MqttOperationOptions::Publish(publish_options) => {
            let sender = publish_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
        MqttOperationOptions::Subscribe(subscribe_options) => {
            let sender = subscribe_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
        MqttOperationOptions::Unsubscribe(unsubscribe_options) => {
            let sender = unsubscribe_options.response_sender.take().unwrap();
            let _ = sender.send(Err(error));
        }
    }

    Ok(())
}

fn does_packet_pass_offline_queue_policy(packet: &MqttPacket, policy: &OfflineQueuePolicy) -> bool {
    match packet {
        MqttPacket::Subscribe(_) | MqttPacket::Unsubscribe(_) => {
            !matches!(policy, OfflineQueuePolicy::PreserveQos1PlusPublishes | OfflineQueuePolicy::PreserveNothing)
        }
        MqttPacket::Publish(publish) => {
            match policy {
                OfflineQueuePolicy::PreserveNothing => { false }
                OfflineQueuePolicy::PreserveQos1PlusPublishes | OfflineQueuePolicy::PreserveAcknowledged => {
                    publish.qos != QualityOfService::AtMostOnce
                }
                _ => { true }
            }
        }
        _ => { false }
    }
}

fn partition_operations_by_queue_policy<'a, T>(iterator: T, policy: &OfflineQueuePolicy) -> (VecDeque<u64>, VecDeque<u64>) where T : Iterator<Item = (u64, &'a MqttPacket)> {
    let mut retained : VecDeque<u64> = VecDeque::new();
    let mut filtered : VecDeque<u64> = VecDeque::new();

    iterator.for_each(|(id, packet)| {
        if does_packet_pass_offline_queue_policy(packet, policy) {
            retained.push_back(id);
        } else {
            filtered.push_back(id);
        }
    });

    (retained, filtered)
}

fn sort_operation_deque(operations: &mut VecDeque<u64>) {
    operations.rotate_right(operations.as_slices().1.len());
    operations.as_mut_slices().0.sort();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_operational_state_config_for_settings_test(connect_options: ConnectOptions) -> OperationalStateConfig {
        OperationalStateConfig {
            connect_options,
            base_timestamp: Instant::now(),
            offline_queue_policy: OfflineQueuePolicy::PreserveAll,
            connack_timeout: Duration::from_secs(30),
            ping_timeout: Duration::from_millis(30000),
            outbound_alias_resolver: None,
        }
    }

    #[test]
    fn build_negotiated_settings_min_connect_min_connack() {
        let config = build_operational_state_config_for_settings_test(ConnectOptionsBuilder::new().build());

        let connack = ConnackPacket {
            assigned_client_identifier: Some("client".to_string()),
            ..Default::default()
        };

        let actual_settings = build_negotiated_settings(&config, &connack, &None);
        let expected_settings = NegotiatedSettings {
            maximum_qos : QualityOfService::ExactlyOnce,
            session_expiry_interval : 0,
            receive_maximum_from_server : 65535,
            maximum_packet_size_to_server : MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            topic_alias_maximum_to_server : 0,
            server_keep_alive : 0,
            retain_available : true,
            wildcard_subscriptions_available : true,
            subscription_identifiers_available : true,
            shared_subscriptions_available : true,
            rejoined_session : false,
            client_id : "client".to_string()
        };

        assert_eq!(expected_settings, actual_settings);
    }

    #[test]
    fn build_negotiated_settings_max_connect_min_connack() {
        let config = build_operational_state_config_for_settings_test(
            ConnectOptionsBuilder::new()
                .with_keep_alive_interval_seconds(1200)
                .with_rejoin_session_policy(RejoinSessionPolicy::Always)
                .with_client_id("connect_client_id")
                .with_session_expiry_interval_seconds(3600)
                .with_receive_maximum(20)
                .with_topic_alias_maximum(5)
                .with_maximum_packet_size_bytes(128 * 1024)
                .build());

        let connack = ConnackPacket {
            ..Default::default()
        };

        let actual_settings = build_negotiated_settings(&config, &connack, &None);
        let expected_settings = NegotiatedSettings {
            maximum_qos : QualityOfService::ExactlyOnce,
            session_expiry_interval : 3600,
            receive_maximum_from_server : 65535,
            maximum_packet_size_to_server : MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            topic_alias_maximum_to_server : 0,
            server_keep_alive : 1200,
            retain_available : true,
            wildcard_subscriptions_available : true,
            subscription_identifiers_available : true,
            shared_subscriptions_available : true,
            rejoined_session : false,
            client_id : "connect_client_id".to_string()
        };

        assert_eq!(expected_settings, actual_settings);
    }

    #[test]
    fn build_negotiated_settings_min_connect_max_connack() {
        let config = build_operational_state_config_for_settings_test(ConnectOptionsBuilder::new().build());

        let connack = ConnackPacket {
            session_present: true,
            session_expiry_interval: Some(7200),
            receive_maximum: Some(50),
            maximum_qos: Some(QualityOfService::AtLeastOnce),
            retain_available: Some(false),
            maximum_packet_size: Some(65536),
            assigned_client_identifier: Some("assigned_client_id".to_string()),
            topic_alias_maximum: Some(20),
            wildcard_subscriptions_available: Some(false),
            subscription_identifiers_available: Some(false),
            shared_subscriptions_available: Some(false),
            server_keep_alive: Some(600),
            ..Default::default()
        };

        let actual_settings = build_negotiated_settings(&config, &connack, &None);
        let expected_settings = NegotiatedSettings {
            maximum_qos : QualityOfService::AtLeastOnce,
            session_expiry_interval : 7200,
            receive_maximum_from_server : 50,
            maximum_packet_size_to_server : 65536,
            topic_alias_maximum_to_server : 20,
            server_keep_alive : 600,
            retain_available : false,
            wildcard_subscriptions_available : false,
            subscription_identifiers_available : false,
            shared_subscriptions_available : false,
            rejoined_session : true,
            client_id : "assigned_client_id".to_string()
        };

        assert_eq!(expected_settings, actual_settings);
    }

    #[test]
    fn build_negotiated_settings_max_connect_max_connack() {
        let config = build_operational_state_config_for_settings_test(
            ConnectOptionsBuilder::new()
                .with_keep_alive_interval_seconds(1200)
                .with_rejoin_session_policy(RejoinSessionPolicy::Never)
                .with_client_id("connect_client_id")
                .with_session_expiry_interval_seconds(3600)
                .with_receive_maximum(20)
                .with_topic_alias_maximum(5)
                .with_maximum_packet_size_bytes(128 * 1024)
                .build());

        let connack = ConnackPacket {
            session_present: true,
            session_expiry_interval: Some(1800),
            receive_maximum: Some(40),
            maximum_qos: Some(QualityOfService::AtMostOnce),
            retain_available: Some(true),
            maximum_packet_size: Some(48 * 1024),
            assigned_client_identifier: Some("assigned_client_id".to_string()),
            topic_alias_maximum: Some(30),
            wildcard_subscriptions_available: Some(false),
            subscription_identifiers_available: Some(true),
            shared_subscriptions_available: Some(false),
            server_keep_alive: Some(900),
            ..Default::default()
        };

        let actual_settings = build_negotiated_settings(&config, &connack, &None);
        let expected_settings = NegotiatedSettings {
            maximum_qos : QualityOfService::AtMostOnce,
            session_expiry_interval : 1800,
            receive_maximum_from_server : 40,
            maximum_packet_size_to_server : 48 * 1024,
            topic_alias_maximum_to_server : 30,
            server_keep_alive : 900,
            retain_available : true,
            wildcard_subscriptions_available : false,
            subscription_identifiers_available : true,
            shared_subscriptions_available : false,
            rejoined_session : true,
            client_id : "assigned_client_id".to_string()
        };

        assert_eq!(expected_settings, actual_settings);
    }

    #[test]
    fn build_negotiated_settings_existing_client_id() {
        let config = build_operational_state_config_for_settings_test(ConnectOptionsBuilder::new().build());

        let connack = ConnackPacket {
            ..Default::default()
        };

        let existing_settings = NegotiatedSettings {
            client_id: "existing_client_id".to_string(),
            ..Default::default()
        };

        let actual_settings = build_negotiated_settings(&config, &connack, &Some(existing_settings));
        let expected_settings = NegotiatedSettings {
            maximum_qos : QualityOfService::ExactlyOnce,
            session_expiry_interval : 0,
            receive_maximum_from_server : 65535,
            maximum_packet_size_to_server : MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
            topic_alias_maximum_to_server : 0,
            server_keep_alive : 0,
            retain_available : true,
            wildcard_subscriptions_available : true,
            subscription_identifiers_available : true,
            shared_subscriptions_available : true,
            rejoined_session : false,
            client_id : "existing_client_id".to_string()
        };

        assert_eq!(expected_settings, actual_settings);
    }

    fn build_partition_operation_sequence() -> Vec<(u64, MqttPacket)> {
        return vec!(
            (666, MqttPacket::Pubrel(PubrelPacket {
                ..Default::default()
            })),
            (5, MqttPacket::Pingreq(PingreqPacket{})),
            (1023, MqttPacket::Publish(PublishPacket{
                qos : QualityOfService::AtLeastOnce,
                ..Default::default()
            })),
            (43, MqttPacket::Publish(PublishPacket {
                qos : QualityOfService::AtMostOnce,
                ..Default::default()
            })),
            (17, MqttPacket::Subscribe(SubscribePacket{ ..Default::default() })),
            (23, MqttPacket::Unsubscribe(UnsubscribePacket{ ..Default::default() })),
            (1024, MqttPacket::Publish(PublishPacket{
                qos : QualityOfService::ExactlyOnce,
                ..Default::default()
            })),
            (3, MqttPacket::Disconnect(DisconnectPacket{ ..Default::default() }))
        );
    }

    fn do_partition_operation_by_queue_policy_test(policy: OfflineQueuePolicy, expected_retain: Vec<u64>, expected_reject: Vec<u64>) {
        let operation_sequence = build_partition_operation_sequence();
        let sequence_iter = operation_sequence.iter().map(|(id, packet)| {
            (*id, packet)
        });

        let (mut retain, mut reject) = partition_operations_by_queue_policy(sequence_iter, &policy);

        sort_operation_deque(&mut retain);
        sort_operation_deque(&mut reject);

        let retain_vector : Vec<u64> = retain.into_iter().collect();
        let reject_vector : Vec<u64> = reject.into_iter().collect();

        assert_eq!(expected_retain, retain_vector);
        assert_eq!(expected_reject, reject_vector);
    }

    #[test]
    fn partition_operations_by_queue_policy_preserve_all() {
        do_partition_operation_by_queue_policy_test(
            OfflineQueuePolicy::PreserveAll,
            vec!(17, 23, 43, 1023, 1024),
            vec!(3, 5, 666)
        );
    }

    #[test]
    fn partition_operations_by_queue_policy_preserve_acknowledged() {
        do_partition_operation_by_queue_policy_test(
            OfflineQueuePolicy::PreserveAcknowledged,
            vec!(17, 23, 1023, 1024),
            vec!(3, 5, 43, 666)
        );
    }

    #[test]
    fn partition_operations_by_queue_policy_preserve_qos1plus() {
        do_partition_operation_by_queue_policy_test(
            OfflineQueuePolicy::PreserveQos1PlusPublishes,
            vec!(1023, 1024),
            vec!(3, 5, 17, 23, 43, 666)
        );
    }

    #[test]
    fn partition_operations_by_queue_policy_preserve_nothing() {
        do_partition_operation_by_queue_policy_test(
            OfflineQueuePolicy::PreserveNothing,
            vec!(),
            vec!(3, 5, 17, 23, 43, 666, 1023, 1024)
        );
    }

    fn build_operational_state_for_acquire_packet_id_test() -> OperationalState {
        let config = OperationalStateConfig {
            connect_options: ConnectOptionsBuilder::new().build(),
            base_timestamp: Instant::now(),
            offline_queue_policy: OfflineQueuePolicy::PreserveNothing,
            connack_timeout: Duration::from_millis(0),
            ping_timeout: Duration::from_millis(0),
            outbound_alias_resolver: None,
        };

        OperationalState::new(config)
    }

    #[test]
    fn acquire_free_packet_id_start() {
        let mut operational_state = build_operational_state_for_acquire_packet_id_test();

        assert_eq!(Ok(1), operational_state.acquire_free_packet_id(1));
        assert_eq!(Ok(2), operational_state.acquire_free_packet_id(2));
        assert_eq!(Ok(3), operational_state.acquire_free_packet_id(3));
    }

    #[test]
    fn acquire_free_packet_id_with_skips() {
        let mut operational_state = build_operational_state_for_acquire_packet_id_test();

        operational_state.next_packet_id = 5;
        operational_state.allocated_packet_ids.insert(5, 10);
        operational_state.allocated_packet_ids.insert(6, 12);
        operational_state.allocated_packet_ids.insert(7, 14);
        operational_state.allocated_packet_ids.insert(9, 18);
        operational_state.allocated_packet_ids.insert(11, 22);

        assert_eq!(Ok(8), operational_state.acquire_free_packet_id(1));
        assert_eq!(Ok(10), operational_state.acquire_free_packet_id(2));
        assert_eq!(Ok(12), operational_state.acquire_free_packet_id(3));
    }

    #[test]
    fn acquire_free_packet_id_with_wrap_around() {
        let mut operational_state = build_operational_state_for_acquire_packet_id_test();

        operational_state.next_packet_id = 65534;

        assert_eq!(Ok(65534), operational_state.acquire_free_packet_id(1));
        assert_eq!(Ok(65535), operational_state.acquire_free_packet_id(2));
        assert_eq!(Ok(1), operational_state.acquire_free_packet_id(3));
        assert_eq!(Ok(2), operational_state.acquire_free_packet_id(4));
    }

    #[test]
    fn acquire_free_packet_id_with_wrap_around_with_skips() {
        let mut operational_state = build_operational_state_for_acquire_packet_id_test();

        operational_state.next_packet_id = 65534;
        operational_state.allocated_packet_ids.insert(65534, 10);
        operational_state.allocated_packet_ids.insert(65535, 12);
        operational_state.allocated_packet_ids.insert(1, 14);
        operational_state.allocated_packet_ids.insert(2, 18);
        operational_state.allocated_packet_ids.insert(4, 22);

        assert_eq!(Ok(3), operational_state.acquire_free_packet_id(1));
        assert_eq!(Ok(5), operational_state.acquire_free_packet_id(2));
    }

    #[test]
    fn acquire_free_packet_id_no_space() {
        let mut operational_state = build_operational_state_for_acquire_packet_id_test();
        for i in 0..u16::MAX {
            operational_state.allocated_packet_ids.insert(i + 1, i as u64);
        }

        assert_eq!(Err(Mqtt5Error::PacketIdSpaceExhausted), operational_state.acquire_free_packet_id(1));
    }
}