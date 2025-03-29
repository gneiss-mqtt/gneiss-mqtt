/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use std::error::Error;
use std::fmt;
use gneiss_mqtt::error::GneissError;

/// Additional details about an InvalidConfiguration error variant
#[derive(Debug)]
pub struct InvalidConfigurationContext {
}

/// Additional details about an ProtocolClient error variant
#[derive(Debug)]
pub struct ProtocolClientContext {
    pub source: Box<GneissError>
}

/// Additional details about an OperationChannelFailure error variant
#[derive(Debug)]
pub struct OperationChannelFailureContext {
    pub source: Box<dyn Error + Send + Sync + 'static>
}


#[derive(Debug)]
#[non_exhaustive]
pub enum RequestResponseError {
    InvalidConfiguration(InvalidConfigurationContext),
    ProtocolClient(ProtocolClientContext),
    OperationChannelFailure(OperationChannelFailureContext),
}

impl RequestResponseError {
    pub(crate) fn new_invalid_configuration() -> Self {
        RequestResponseError::InvalidConfiguration(
            InvalidConfigurationContext {
            }
        )
    }

    pub(crate) fn new_protocol_client(error: GneissError) -> Self {
        RequestResponseError::ProtocolClient(
            ProtocolClientContext {
                source: Box::new(error)
            }
        )
    }

    pub fn new_operation_channel_failure(error: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        RequestResponseError::OperationChannelFailure(
            OperationChannelFailureContext {
                source: error.into()
            }
        )
    }
}

impl Error for RequestResponseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RequestResponseError::ProtocolClient(context) => Some(&context.source),
            RequestResponseError::OperationChannelFailure(context) => Some(context.source.as_ref()),
            _ => { None }
        }
    }
}

impl fmt::Display for RequestResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestResponseError::InvalidConfiguration(_) => {
                write!(f, "Invalid configuration was passed to a constructor")
            }
            RequestResponseError::ProtocolClient( context ) => {
                write!(f, "Protocol client error: {}", context.source)
            }
            RequestResponseError::OperationChannelFailure( context ) => {
                write!(f, "Failure encountered while sending/receiving on a request-response operation-related channel: {}", context.source)
            }
        }
    }
}

impl From<GneissError> for RequestResponseError {
    fn from(error: GneissError) -> Self {
        RequestResponseError::new_protocol_client(error)
    }
}

pub type RequestResponseResult<T> = Result<T, RequestResponseError>;
