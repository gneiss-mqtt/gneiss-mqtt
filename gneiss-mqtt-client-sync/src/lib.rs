/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
TBI
 */

use gneiss_mqtt::error::GneissResult;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub trait Stream {
    fn get_read(&mut self) -> &mut dyn Read;

    fn get_write(&mut self) -> &mut dyn Write;
}

impl <T> Stream for T where T : Read + Write {
    fn get_read(&mut self) -> &mut dyn Read {
        self
    }

    fn get_write(&mut self) -> &mut dyn Write {
        self
    }
}

pub struct StreamHandle {
    stream: Box<dyn Stream + Send + Sync>
}

impl StreamHandle {
    pub fn new<T>(stream : T) -> StreamHandle where T : Stream + Send + Sync + 'static {
        StreamHandle {
            stream: Box::new(stream)
        }
    }
}

impl Read for StreamHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.get_read().read(buf)
    }
}

impl Write for StreamHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.stream.get_write().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.stream.get_write().flush()
    }
}

pub trait SyncStreamSource {
    fn create_source(&self) -> GneissResult<StreamHandle>;

    fn host(&self) -> &str;

    fn port(&self) -> u16;
}

pub struct TcpStreamSource {
    host: String,
    port: u16
}

impl TcpStreamSource {
    pub fn new(host: &str, port: u16) -> TcpStreamSource {
        TcpStreamSource {
            host: host.to_string(),
            port
        }
    }
}

impl SyncStreamSource for TcpStreamSource {
    fn create_source(&self) -> GneissResult<StreamHandle> {
        let stream = TcpStream::connect((self.host.as_str(), self.port))?;
        Ok(StreamHandle::new(stream))
    }

    fn host(&self) -> &str { &self.host }

    fn port(&self) -> u16 { self.port }
}


#[derive(Clone)]
pub struct SyncClientConnectionFactory {
    source: Arc<dyn SyncStreamSource + Send + Sync>,

    stream_wrapper: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>,
}

impl SyncClientConnectionFactory {
    pub fn new(host: &str, port: u16) -> SyncClientConnectionFactory {
        SyncClientConnectionFactory {
            source: Arc::new(TcpStreamSource::new(host, port)),
            stream_wrapper: Arc::new(|stream| {
                Ok(stream)
            })
        }
    }

    pub fn apply_transform(&mut self, stream_transform: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>) -> &mut Self {
        let base_stream_wrapper = self.stream_wrapper.clone();
        let new_stream_wrapper = stream_transform.clone();

        self.stream_wrapper = Arc::new(move |stream| {
            let base_stream = base_stream_wrapper(stream)?;
            let wrapped_stream = new_stream_wrapper(base_stream)?;

            Ok(wrapped_stream)
        });

        self
    }

    pub fn replace_source(&mut self, source_transform: Arc<dyn Fn(Arc<dyn SyncStreamSource>) -> GneissResult<Arc<dyn SyncStreamSource + Send + Sync>> + Send + Sync>) -> GneissResult<()> {
        let old_source = self.source.clone();
        self.source = source_transform(old_source)?;

        Ok(())
    }

    pub fn connect(&self) -> GneissResult<StreamHandle> {
        let base_stream = self.source.create_source()?;
        let wrapper_stream = (self.stream_wrapper)(base_stream)?;

        Ok(wrapper_stream)
    }
}

pub struct SyncStreamTransform {
    transform: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>
}

impl SyncStreamTransform {
    pub fn transform(&self) -> Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync> {
        self.transform.clone()
    }
}
