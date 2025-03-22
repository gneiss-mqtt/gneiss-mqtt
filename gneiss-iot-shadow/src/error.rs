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
enum ModeledServiceException {
    ServiceErrorResponse(ServiceErrorResponse)
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

#[derive(Debug)]
#[non_exhaustive]
pub enum ShadowError {
    RequestResponse(RequestResponseContext),
    ServiceException(ServiceExceptionContext),
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
}

impl Error for ShadowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ShadowError::RequestResponse(err) => { Some(&err.source) }
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
        }
    }
}

impl From<RequestResponseError> for ShadowError {
    fn from(error: RequestResponseError) -> Self {
        ShadowError::new_request_response(error)
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
