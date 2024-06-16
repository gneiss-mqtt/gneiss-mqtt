/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::io::{ErrorKind, Read, Write};

use tungstenite::error::Error;
use tungstenite::protocol::{Message, WebSocket};

struct MessageCursor {
    data: Vec<u8>,
    index: usize,
}

impl MessageCursor {
    fn new(message: Message) -> Option<Self> {
        match message {
            Message::Text(value) => {
                Some(MessageCursor {
                    data: value.into_bytes(),
                    index: 0
                })
            }
            Message::Binary(value) => {
                Some(MessageCursor {
                    data: value,
                    index: 0
                })
            }
            _ => {
                None
            }
        }
    }

    fn read(&mut self, dest: &mut[u8]) -> usize {
        if self.index < self.data.len() {
            let amount = usize::min(self.data.len() - self.index, dest.len());
            if amount > 0 {
                let dest_slice = &mut dest[..amount];
                let source_slice = &self.data[..amount];

                dest_slice.copy_from_slice(source_slice);

                self.index += amount;

                return amount;
            }
        }

        0
    }
}

pub(crate) struct WebsocketStreamWrapper<T> where T : Read + Write + Send + Sync + 'static {
    stream: WebSocket<T>,
    current_read_message: Option<MessageCursor>,
    final_error: Option<tungstenite::error::Error>
}

impl<T> WebsocketStreamWrapper<T> where T : Read + Write + Send + Sync + 'static {
    pub(crate) fn new(stream : WebSocket<T>) -> WebsocketStreamWrapper<T> {
        WebsocketStreamWrapper {
            stream,
            current_read_message: None,
            final_error: None
        }
    }
}

impl<T> Read for WebsocketStreamWrapper<T> where T : Read + Write + Send + Sync + 'static {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;

        while bytes_read < buf.len() {
            if self.current_read_message.is_none() {

                if self.final_error.is_some() {
                    if bytes_read > 0 {
                        return Ok(bytes_read);
                    } else {
                        return Err(map_tungstenite_error_to_io_error(self.final_error.take().unwrap()));
                    }
                }

                let read_result = self.stream.read();
                match read_result {
                    Ok(message) => {
                        self.current_read_message = MessageCursor::new(message);
                    }
                    Err(err) => {
                        if is_tungstenite_error_would_block(&err) {
                            return Ok(bytes_read);
                        }

                        self.final_error = Some(err);
                    }
                }
            }

            if let Some(current_message) = &mut self.current_read_message {
                bytes_read += current_message.read(buf);
            }
        }


        Ok(bytes_read)
    }
}

impl<T> Write for WebsocketStreamWrapper<T> where T : Read + Write + Send + Sync + 'static {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let message = Message::Binary(buf.to_vec());
        let write_result = self.stream.send(message);

        match write_result {
            Ok(()) => {
                Ok(buf.len())
            }
            Err(err) => {
                Err(map_tungstenite_error_to_io_error(err))
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.flush().map_err(|err| { map_tungstenite_error_to_io_error(err) })
    }
}

fn map_tungstenite_error_to_io_error(error: tungstenite::error::Error) -> std::io::Error {
    match error {
        Error::Io(io_error) => {
            io_error
        }
        Error::WriteBufferFull(_) => {
            std::io::Error::new(ErrorKind::WouldBlock, "Websocket write buffer full")
        }
        Error::ConnectionClosed |
        Error::AlreadyClosed => {
            std::io::Error::new(std::io::ErrorKind::ConnectionAborted, error)
        }
        _ => {
            std::io::Error::new(std::io::ErrorKind::Other, error)
        }
    }
}

fn is_tungstenite_error_would_block(error: &tungstenite::error::Error) -> bool {
    match error {
        Error::Io(io_error) => {
            let error_kind = io_error.kind();
            error_kind != ErrorKind::WouldBlock
        }
        _ => {
            false
        }
    }
}