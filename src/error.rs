use crate::bindings::*;
use std::error;

#[derive(Debug, PartialEq)]
#[repr(u32)]
pub enum Error {
    Ok = pulsar_result_pulsar_result_Ok,
    UnknownError = pulsar_result_pulsar_result_UnknownError,
    InvalidConfiguration = pulsar_result_pulsar_result_InvalidConfiguration,
    Timeout = pulsar_result_pulsar_result_Timeout,
    ConnectError = pulsar_result_pulsar_result_ConnectError,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FUCK")
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "what the hell is this garbage"
    }
}
