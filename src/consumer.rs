use crate::bindings::*;
use crate::client::Client;
use crate::error::Error as PError;
use crate::message::Message;
use std::ffi::CString;
use std::io::Error;
use std::ptr;
use std::ptr::NonNull;

pub struct ConsumerConfig {
    inner: NonNull<pulsar_consumer_configuration_t>,
}

impl ConsumerConfig {
    pub fn new() -> Result<ConsumerConfig, Error> {
        let conf: *mut pulsar_consumer_configuration_t =
            unsafe { pulsar_consumer_configuration_create() };
        if conf.is_null() {
            return Err(Error::last_os_error());
        }
        Ok(ConsumerConfig {
            inner: NonNull::new(conf).unwrap(),
        })
    }
}

pub struct Consumer {
    inner: NonNull<pulsar_consumer_t>,
}

impl Consumer {
    pub fn new(
        client: &mut Client,
        topic: &str,
        sub_name: &str,
        config: ConsumerConfig,
    ) -> Result<Consumer, PError> {
        let c = unsafe {
            let mut c: *mut pulsar_consumer_t = ptr::null_mut();
            let topic = CString::new(topic).unwrap();
            let sub_name = CString::new(sub_name).unwrap();
            let err = pulsar_client_subscribe(
                client.as_ptr(),
                topic.as_ptr(),
                sub_name.as_ptr(),
                config.inner.as_ptr(),
                &mut c,
            );
            if err != 0 {
                return Err(PError::UnknownError);
            } else {
                NonNull::new(c).unwrap()
            }
        };

        Ok(Consumer { inner: c })
    }

    pub fn recv(&self) -> Result<Message, PError> {
        unsafe {
            let mut m = ptr::null_mut();
            let code = pulsar_consumer_receive(self.inner.as_ptr(), &mut m);
            if code == 0 {
                Ok(Message::from(m))
            } else {
                Err(PError::UnknownError)
            }
        }
    }

    pub fn ack(&self, mut m: Message) -> Result<(), PError> {
        unsafe {
            if pulsar_consumer_acknowledge(self.inner.as_ptr(), m.as_ptr()) != 0 {
                Err(PError::UnknownError)
            } else {
                Ok(())
            }
        }
    }

    pub fn ack_async(&self, mut m: Message) {
        unsafe {
            pulsar_consumer_acknowledge_async(
                self.inner.as_ptr(),
                m.as_ptr(),
                Option::None,
                ptr::null_mut(),
            );
        }
    }

    pub fn close(self) {
        unsafe {
            pulsar_consumer_close(self.inner.as_ptr());
        }
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        unsafe {
            pulsar_consumer_free(self.inner.as_ptr());
        }
    }
}
