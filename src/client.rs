use crate::bindings::*;
use crate::error::Error as PError;
use std::ffi::CString;
use std::io::Error;
use std::ptr;

#[derive(Debug)]
pub struct ClientConfig {
    inner: ptr::NonNull<pulsar_client_configuration_t>,
}

impl ClientConfig {
    pub fn new() -> Result<ClientConfig, Error> {
        let conf: *mut pulsar_client_configuration_t =
            unsafe { pulsar_client_configuration_create() };
        if conf.is_null() {
            return Err(Error::last_os_error());
        }
        Ok(ClientConfig {
            inner: ptr::NonNull::new(conf).unwrap(),
        })
    }
}

impl Drop for ClientConfig {
    fn drop(&mut self) {
        unsafe { pulsar_client_configuration_free(self.inner.as_mut()) }
    }
}

#[derive(Debug)]
pub struct Client {
    inner: ptr::NonNull<pulsar_client_t>,
}

impl Client {
    pub fn new(service_url: &str, conf: ClientConfig) -> Result<Client, Error> {
        unsafe {
            let surl = CString::new(service_url).unwrap();
            let client = pulsar_client_create(surl.as_ptr(), conf.inner.as_ptr());
            if client.is_null() {
                return Err(Error::last_os_error());
            }

            Ok(Client {
                inner: ptr::NonNull::new(client).unwrap(),
            })
        }
    }

    pub fn close(self) -> Result<(), (Client, PError)> {
        unsafe {
            let code = pulsar_client_close(self.inner.as_ptr());
            if code == 0 {
                Ok(())
            } else {
                Err((self, PError::UnknownError))
            }
        }
    }

    pub fn as_ptr(&mut self) -> *mut pulsar_client_t {
        self.inner.as_ptr()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        unsafe {
            pulsar_client_free(self.inner.as_mut());
        }
    }
}
