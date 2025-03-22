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

#[derive(Debug)]
#[non_exhaustive]
pub enum RequestResponseError {
    InvalidConfiguration(InvalidConfigurationContext),
    ProtocolClient(ProtocolClientContext),
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
}

impl Error for RequestResponseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RequestResponseError::ProtocolClient(context) => Some(&context.source),
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
        }
    }
}

impl From<GneissError> for RequestResponseError {
    fn from(error: GneissError) -> Self {
        RequestResponseError::new_protocol_client(error)
    }
}

pub type RequestResponseResult<T> = Result<T, RequestResponseError>;
