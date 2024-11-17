/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::error::*;
use crate::decode::*;
use crate::encode::*;
use crate::mqtt::*;

use std::collections::{VecDeque};
use std::io::{ErrorKind, Read, Write};
use std::io::ErrorKind::{TimedOut, WouldBlock, Interrupted};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::time::Duration;
use crate::alias::OutboundAliasResolution;
use crate::client::config::*;
use crate::mqtt::utils::mqtt_packet_to_packet_type;
use crate::testing::protocol::*;
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;


struct MockBrokerConnection {
    decoder: Decoder,
    encoder: Encoder,
    packet_handlers: PacketHandlerSet,
    stream: TcpStream,
    context: Arc<Mutex<BrokerTestContext>>
}

impl MockBrokerConnection {
    pub fn new(stream: TcpStream, packet_handlers: PacketHandlerSet, context: Arc<Mutex<BrokerTestContext>>) -> Self {
        MockBrokerConnection {
            decoder: Decoder::new(),
            encoder: Encoder::new(),
            packet_handlers,
            stream,
            context
        }
    }

    pub fn run(&mut self) {
        self.stream.set_read_timeout(Some(Duration::from_millis(20))).expect("Failed to set read timeout");

        let mut broker_packets = VecDeque::new();
        let mut response_bytes = Vec::new();
        let mut read_buffer : [u8; 4096] = [0; 4096];
        let mut done = false;

        while !done {
            let read_result = self.stream.read(&mut read_buffer);
            match read_result {
                Ok(bytes_read) => {
                    if bytes_read > 0 {
                        let mut decode_context = DecodingContext {
                            maximum_packet_size : MAXIMUM_VARIABLE_LENGTH_INTEGER as u32,
                            decoded_packets: &mut broker_packets,
                        };

                        if self.decoder.decode_bytes(&read_buffer[..bytes_read], &mut decode_context).is_err() {
                            panic!("Test triggered broker decode failure");
                        }

                        for packet in &broker_packets {
                            if self.handle_packet(packet, &mut response_bytes).is_err() {
                                panic!("Test triggered broker packet handling failure");
                            }
                        }
                    } else {
                        done = true;
                    }
                }
                Err(error) => {
                    let error_kind = error.kind();
                    match error_kind {
                        TimedOut | WouldBlock | Interrupted => {}
                        _ => { done = true; }
                    }
                }
            }

            let mut wrote_bytes = false;
            let mut remaining_bytes = response_bytes.as_slice();
            while remaining_bytes.len() > 0 {
                let write_result = self.stream.write(remaining_bytes);
                match write_result {
                    Ok(bytes_written) => {
                        wrote_bytes = true;
                        remaining_bytes = &remaining_bytes[bytes_written..];
                    }
                    Err(_) => {
                        self.stream.shutdown(Shutdown::Both).expect("Failed to close stream");
                        break;
                    }
                }

                if remaining_bytes.len() > 0 {
                    std::thread::sleep(Duration::from_millis(0));
                }
            }

            if wrote_bytes {
                self.stream.flush().expect("Failed to flush broker write");
            }

            response_bytes.clear();
            broker_packets.clear();

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn handle_packet(&mut self, packet: &Box<MqttPacket>, response_bytes: &mut Vec<u8>) -> MqttResult<()> {
        let mut response_packets = VecDeque::new();
        let packet_type = mqtt_packet_to_packet_type(&*packet);

        if let Some(handler) = self.packet_handlers.get(&packet_type) {
            let mut context = self.context.lock().unwrap();
            (*handler)(packet, &mut response_packets, context.deref_mut())?;

            let mut encode_buffer = Vec::with_capacity(4096);

            for response_packet in &response_packets {
                let encoding_context = EncodingContext {
                    outbound_alias_resolution: OutboundAliasResolution {
                        skip_topic: false,
                        alias: None,
                    }
                };

                self.encoder.reset(&*response_packet, &encoding_context)?;

                let mut encode_result = EncodeResult::Full;
                while encode_result == EncodeResult::Full {
                    encode_result = self.encoder.encode(&*response_packet, &mut encode_buffer)?;
                    response_bytes.append(&mut encode_buffer);
                    encode_buffer.clear(); // redundant probably
                }
            }
        }

        Ok(())
    }
}

pub(crate) struct MockBroker {
    port: u16,
    shutdown_flag: Arc<Mutex<bool>>
}

impl MockBroker {
    pub fn close(&self) {
        let mut shutdown_status = self.shutdown_flag.lock().unwrap();
        *shutdown_status = true;
    }

    pub fn new(handler_set_factory: PacketHandlerSetFactory) -> Self {
        let bind_result = TcpListener::bind("127.0.0.1:0");
        if let Ok(listener) = bind_result {
            listener.set_nonblocking(true).expect("Failed to set listener socket non-blocking");

            let port = listener.local_addr().unwrap().port();
            let shutdown_flag= Arc::new(Mutex::new(false));
            let interior_shutdown_flag = shutdown_flag.clone();

            let test_context = Arc::new(Mutex::new(BrokerTestContext::default()));

            std::thread::spawn(move || {
                for next_stream_result in listener.incoming() {
                    match next_stream_result {
                        Ok(stream) => {
                            let handler_set = handler_set_factory();
                            let mut connection = MockBrokerConnection::new(stream, handler_set, test_context.clone());
                            std::thread::spawn(move || {
                                connection.run();
                            });
                        }
                        Err(error) => {
                            let error_kind = error.kind();
                            if error_kind != ErrorKind::WouldBlock {
                                break;
                            }

                            let shutdown_status = interior_shutdown_flag.lock().unwrap();
                            if *shutdown_status {
                                break;
                            }
                        }
                    }
                }

                println!("Stop listening!");
            });

            return MockBroker {
                port,
                shutdown_flag
            }
        }

        panic!("Could not find an open port");
    }
}

#[derive(Default)]
pub(crate) struct ClientTestOptions {
    pub(crate) packet_handler_set_factory_fn: Option<PacketHandlerSetFactory>,

    pub(crate) client_options_mutator_fn: Option<Box<dyn Fn(&mut MqttClientOptionsBuilder)>>,

    pub(crate) connect_options_mutator_fn: Option<Box<dyn Fn(&mut ConnectOptionsBuilder)>>
}

pub(crate) fn build_mock_client_server(mut config: ClientTestOptions) -> (GenericClientBuilder, MockBroker) {
    let handler_set_factory : PacketHandlerSetFactory =
        if config.packet_handler_set_factory_fn.is_some() {
            config.packet_handler_set_factory_fn.take().unwrap()
        } else {
            Box::new(|| { create_default_packet_handlers() })
        };

    let broker = MockBroker::new(handler_set_factory);

    let mut client_options_builder = MqttClientOptionsBuilder::new();
    client_options_builder.with_connect_timeout(Duration::from_secs(3));

    if let Some(client_options_mutator) = config.client_options_mutator_fn {
        (*client_options_mutator)(&mut client_options_builder);
    }

    let mut connect_options_builder = ConnectOptionsBuilder::new();
    if let Some(connect_options_mutator) = config.connect_options_mutator_fn {
        (*connect_options_mutator)(&mut connect_options_builder);
    }

    let mut client_builder = GenericClientBuilder::new("127.0.0.1", broker.port);
    client_builder.with_client_options(client_options_builder.build());
    client_builder.with_connect_options(connect_options_builder.build());

    (client_builder, broker)
}
