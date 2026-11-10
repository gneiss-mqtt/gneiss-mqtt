/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

/*!
Module containing types and functionality for building sync MQTT client network connections
 */

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use crate::error::GneissResult;

/// Bidirectional stream trait that allows (non-simultaneous) access to both Read and Write
/// traits
pub trait Stream {

    /// std::io::Read trait accessor
    fn get_read(&mut self) -> &mut dyn Read;

    /// std::io::Write trait accessor
    fn get_write(&mut self) -> &mut dyn Write;

    /// perform any final connection-related operations before the stream is considered ready to use
    fn complete(&mut self) -> GneissResult<()>;
}

#[doc(hidden)]
pub trait CompletableStream {

    /// perform any final connection-related operations before the stream is considered ready to use
    fn complete(&mut self) -> GneissResult<()>;
}

impl CompletableStream for TcpStream {
    fn complete(&mut self) -> GneissResult<()> {
        self.set_nonblocking(true)?;
        Ok(())
    }
}

impl <T> Stream for T where T : Read + Write + CompletableStream
{
    fn get_read(&mut self) -> &mut dyn Read {
        self
    }

    fn get_write(&mut self) -> &mut dyn Write {
        self
    }

    fn complete(&mut self) -> GneissResult<()> {
        CompletableStream::complete(self)
    }
}

/// Thread-safe wrapper for a Stream object
pub struct StreamHandle {
    stream: Box<dyn Stream + Send + Sync>
}

impl StreamHandle {

    /// StreamHandle constructor
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

impl CompletableStream for StreamHandle {
    fn complete(&mut self) -> GneissResult<()> {
        self.stream.complete()
    }
}

/// Trait for a network (TCP-based) stream factory
pub trait SyncStreamSource {

    /// Creates a new stream
    fn create_source(&self) -> GneissResult<StreamHandle>;

    /// The host that this stream factory creates streams to
    fn host(&self) -> &str;

    /// The port that this stream factory creates streams to
    fn port(&self) -> u16;
}

/// SyncStreamSource implementation that creates a direct TcpStream connection
pub struct TcpStreamSource {
    host: String,
    port: u16
}

impl TcpStreamSource {

    /// TcpStreamSource constructor
    pub fn new(host: &str, port: u16) -> TcpStreamSource {
        TcpStreamSource {
            host: host.to_string(),
            port
        }
    }
}

impl SyncStreamSource for TcpStreamSource {
    fn create_source(&self) -> GneissResult<StreamHandle> {
        let mut to_socket_addrs = (self.host.as_str(), self.port).to_socket_addrs()?;
        let addr = to_socket_addrs.next().unwrap();
        let stream = TcpStream::connect(addr)?;
        Ok(StreamHandle::new(stream))
    }

    fn host(&self) -> &str { &self.host }

    fn port(&self) -> u16 { self.port }
}

/// Thread-safe stream source type
pub type CrossThreadStreamSource = Arc<dyn SyncStreamSource + Send + Sync>;

/// Thread-safe stream source transformer type
pub type CrossThreadStreamSourceTransform = Arc<dyn Fn(Arc<dyn SyncStreamSource>) -> GneissResult<CrossThreadStreamSource> + Send + Sync>;

/// Thread-safe stream -> stream transformation function
pub type SyncStreamTransform = Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>;

/// Stream connection factory type.  We distinguish between transformers (after the initial stream is created) and the stream source to
/// allow for proxy configuration to "rewrite" the stream source (by replacing the original stream source with a pair
/// (proxy-source, proxy-connect) where proxy-source is a stream source that connects to the proxy and proxy-connect is a transform
/// that triggers the proxy to establish a connection to the remote host).
#[derive(Clone)]
pub struct SyncClientConnectionFactory {
    source: CrossThreadStreamSource,

    stream_transform: SyncStreamTransform,
}

impl SyncClientConnectionFactory {

    /// SyncClientConnectionFactory constructor
    pub fn new(host: &str, port: u16) -> SyncClientConnectionFactory {
        SyncClientConnectionFactory {
            source: Arc::new(TcpStreamSource::new(host, port)),
            stream_transform: Arc::new(|stream| {
                Ok(stream)
            })
        }
    }

    /// Appends a stream transform to the sequence of already-applied transforms.  This allows for building up a complex sequence of
    /// steps in the connection establishment process.  For example, starting with a base stream source, you can generically apply
    /// TLS and websockets stages by applying an appropriate transform from the crate that supplies that functionality.
    pub fn apply_transform(&mut self, stream_transform: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>) -> &mut Self {
        let base_stream_wrapper = self.stream_transform.clone();
        let new_stream_wrapper = stream_transform.clone();

        self.stream_transform = Arc::new(move |stream| {
            let base_stream = base_stream_wrapper(stream)?;
            let wrapped_stream = new_stream_wrapper(base_stream)?;

            Ok(wrapped_stream)
        });

        self
    }

    /// Apply a transformation function to a stream source.  The most common usage is to apply an HTTP proxy transform,
    /// changing the source from a direct connect to the host to an indirect connect through the proxy.
    pub fn replace_source(&mut self, source_transform: CrossThreadStreamSourceTransform) -> GneissResult<()> {
        let old_source = self.source.clone();
        self.source = source_transform(old_source)?;

        Ok(())
    }

    /// Create a new connection using the stream source and composed transforms
    pub fn connect(&self) -> GneissResult<StreamHandle> {
        let base_stream = self.source.create_source()?;
        let mut wrapper_stream = (self.stream_transform)(base_stream)?;
        Stream::complete(&mut wrapper_stream)?;

        Ok(wrapper_stream)
    }
}



/*
/// Thread-safe container for stream -> stream transformation functions
pub struct SyncStreamTransform {
    transform: Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync>
}

impl SyncStreamTransform {

    /// Transformation function accessor
    pub fn transform(&self) -> Arc<dyn Fn(StreamHandle) -> GneissResult<StreamHandle> + Send + Sync> {
        self.transform.clone()
    }
}
*/