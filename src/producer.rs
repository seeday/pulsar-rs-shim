use crate::bindings::*;
use crate::client::*;
use crate::error::Error as PError;
use crate::message::*;
use std::ffi::CString;
use std::io::Error;
use std::ptr;
use std::ptr::NonNull;

pub struct ProducerConfig {
    inner: NonNull<pulsar_producer_configuration_t>,
}

impl ProducerConfig {
    pub fn new() -> Result<ProducerConfig, Error> {
        let conf: *mut pulsar_producer_configuration_t =
            unsafe { pulsar_producer_configuration_create() };
        if conf.is_null() {
            return Err(Error::last_os_error());
        }
        Ok(ProducerConfig {
            inner: NonNull::new(conf).unwrap(),
        })
    }
}

pub struct Producer {
    inner: NonNull<pulsar_producer_t>,
}

impl Producer {
    pub fn new(
        client: &mut Client,
        topic: &str,
        config: ProducerConfig,
    ) -> Result<Producer, PError> {
        let p = unsafe {
            let mut p: *mut pulsar_producer_t = ptr::null_mut();
            let tpc = CString::new(topic).unwrap();
            let err = pulsar_client_create_producer(
                client.as_ptr(),
                tpc.as_ptr(),
                config.inner.as_ptr(),
                &mut p,
            );
            if err != 0 {
                return Err(PError::UnknownError);
            } else {
                NonNull::new(p).unwrap()
            }
        };

        Ok(Producer { inner: p })
    }

    pub fn send(&self, mut msg: Message) -> Result<(), PError> {
        unsafe {
            let code = pulsar_producer_send(self.inner.as_ptr(), msg.as_ptr());
            if code == 0 {
                Ok(())
            } else {
                Err(PError::UnknownError)
            }
        }
    }

    pub fn send_async(&self, mut msg: Message) {
        unsafe {
            pulsar_producer_send_async(
                self.inner.as_ptr(),
                msg.as_ptr(),
                Option::None,
                ptr::null_mut()
            );
        }
    }

    pub fn flush(&self) -> Result<(), PError>{
        unsafe {
            let code = pulsar_producer_flush(self.inner.as_ptr());
            if code == 0 {
                Ok(())
            } else {
                Err(PError::UnknownError)
            }
        }
    }

    pub fn close(self) -> Result<(), PError> {
        unsafe {
            let code = pulsar_producer_close(self.inner.as_ptr());
            if code != 0 {
                Err(PError::UnknownError)
            } else {
                Ok(())
            }
        }
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        unsafe {
            pulsar_producer_free(self.inner.as_ptr());
        }
    }
}
