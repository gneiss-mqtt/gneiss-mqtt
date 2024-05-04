/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Implementation of an MQTT client that uses one or more background threads for processing.
 */


use std::io::{Read, Write};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;
use log::{debug, trace};
use crate::client::{ClientImplState, MqttClientImpl, OperationOptions};
use crate::error::MqttResult;

/// Tokio-specific client configuration
pub struct ThreadedClientOptions<T> where T : Read + Write + Send {

    pub idle_service_sleep: Duration,

    /// Factory function for creating the final connection object based on all the various
    /// configuration options and features.  It might be a TcpStream, it might be a TlsStream,
    /// it might be a WebsocketStream, it might be some nested combination.
    ///
    /// Ultimately, the type must implement Read and Write.
    pub connection_factory: Box<dyn Fn() -> MqttResult<T>>,
}

pub(crate) struct ClientRuntimeState<T> where T : Read + Write {
    threaded_config: ThreadedClientOptions<T>,
    operation_receiver: std::sync::mpsc::Receiver<OperationOptions>,
    stream: Option<T>
}

impl<T> ClientRuntimeState<T> where T : Read + Write {
    pub(crate) fn process_stopped(&mut self, client: &mut MqttClientImpl) -> MqttResult<ClientImplState> {
        loop {
            trace!("threaded - process_stopped loop");

            let operation_result = self.operation_receiver.try_recv();
            let mut sleep_duration = Some(self.threaded_config.idle_service_sleep);
            if let Some(operation_options) = operation_result {
                debug!("threaded - process_stopped - user operation received");
                client.handle_incoming_operation(operation_options);
                sleep_duration = None;
            }

            if let Some(transition_state) = client.compute_optional_state_transition() {
                return Ok(transition_state);
            } else if let Some(sleep_duration) = sleep_duration {
                sleep(sleep_duration);
            }
        }
    }
}


struct ThreadedClient {
    pub(crate) operation_sender: std::sync::mpsc::Sender<OperationOptions>,

    pub(crate) listener_id_allocator: Mutex<u64>
}