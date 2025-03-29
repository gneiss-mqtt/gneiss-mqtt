/*
 * Copyright Bret Ambrose. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

use crate::model::ServiceErrorResponse;
use std::error::Error;
use std::fmt;
use gneiss_mqtt_request_response::error::*;

#[derive(Debug)]
#[non_exhaustive]
pub enum ModeledServiceException {
    ServiceErrorResponse(ServiceErrorResponse)
}

impl From<ServiceErrorResponse> for ModeledServiceException {
    fn from(error_response: ServiceErrorResponse) -> Self {
        ModeledServiceException::ServiceErrorResponse(error_response)
    }
}

impl fmt::Display for ModeledServiceException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ModeledServiceException::ServiceErrorResponse(value) => {
                write!(f, "ServiceErrorResponse: {}", value)
            }
        }
    }
}

#[derive(Debug)]
pub struct RequestResponseContext {
    pub source: Box<RequestResponseError>
}

#[derive(Debug)]
pub struct ServiceExceptionContext {
    pub modeled_exception: ModeledServiceException,
}

/// Additional details about an OperationChannelFailure error variant
#[derive(Debug)]
pub struct OperationChannelFailureContext {
    pub source: Box<dyn Error + Send + Sync + 'static>
}

#[derive(Debug)]
pub struct DeserializationFailureContext {
    pub source: Box<dyn Error + Send + Sync + 'static>
}


#[derive(Debug)]
#[non_exhaustive]
pub enum ShadowError {
    RequestResponse(RequestResponseContext),
    ServiceException(ServiceExceptionContext),
    OperationChannelFailure(OperationChannelFailureContext),
    DeserializationFailure(DeserializationFailureContext)
}

impl ShadowError {
    pub(crate) fn new_request_response(error: RequestResponseError) -> Self {
        ShadowError::RequestResponse(
            RequestResponseContext {
                source: Box::new(error)
            }
        )
    }

    pub(crate) fn new_service_exception(exception: ModeledServiceException) -> Self {
        ShadowError::ServiceException(
            ServiceExceptionContext {
                modeled_exception: exception
            }
        )
    }

    pub(crate) fn new_operation_channel_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        ShadowError::OperationChannelFailure(
            OperationChannelFailureContext {
                source: source.into()
            }
        )
    }

    pub(crate) fn new_deserialization_failure(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        ShadowError::DeserializationFailure(
            DeserializationFailureContext {
                source: source.into()
            }
        )
    }
}

impl Error for ShadowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ShadowError::RequestResponse(context) => { Some(context.source.as_ref()) }
            ShadowError::OperationChannelFailure(context) => Some(context.source.as_ref()),
            ShadowError::DeserializationFailure(context) => Some(context.source.as_ref()),
            _ => { None }
        }
    }
}

impl fmt::Display for ShadowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShadowError::RequestResponse(context) => {
                write!(f, "Request response client error: {}", context.source)
            }
            ShadowError::ServiceException(context) => {
                write!(f, "Modeled service exception: {}", context.modeled_exception)
            }
            ShadowError::OperationChannelFailure( context ) => {
                write!(f, "Failure encountered while sending/receiving on a shadow operation-related channel: {}", context.source)
            }
            ShadowError::DeserializationFailure( context ) => {
                write!(f, "Failure deserializing response: {}", context.source)
            }
        }
    }
}

impl From<RequestResponseError> for ShadowError {
    fn from(error: RequestResponseError) -> Self {
        ShadowError::new_request_response(error)
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for ShadowError {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        ShadowError::new_operation_channel_failure(err)
    }
}

// Can't implement From since neither Result nor RequestResponseResult are defined here
pub(crate) fn from_request_response_result<T>(res: RequestResponseResult<T>) -> ShadowResult<T> {
    match res {
        Err(err) => Err(ShadowError::new_request_response(err)),
        Ok(val) => Ok(val)
    }
}

pub type ShadowResult<T> = Result<T, ShadowError>;
