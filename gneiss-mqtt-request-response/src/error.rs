use std::error::Error;
use std::fmt;

#[derive(Debug)]
#[non_exhaustive]
pub enum RequestResponseError {
    InvalidConfiguration
}

impl RequestResponseError {
    pub(crate) fn new_invalid_configuration() -> Self {
        RequestResponseError::InvalidConfiguration
    }
}

impl Error for RequestResponseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            _ => { None }
        }
    }
}

impl fmt::Display for RequestResponseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestResponseError::InvalidConfiguration => {
                write!(f, "Invalid configuration was passed to a constructor")
            }
        }
    }
}

pub type RequestResponseResult<T> = Result<T, RequestResponseError>;
