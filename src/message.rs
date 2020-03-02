use crate::bindings::*;
use std::ptr::NonNull;
use std::slice;
use std::ffi::CString;

#[derive(Debug)]
pub struct Message {
    inner: NonNull<pulsar_message_t>,
}

impl Message {
    pub fn new() -> Message {
        unsafe {
            Message {
                inner: NonNull::new(pulsar_message_create()).unwrap(),
            }
        }
    }

    pub fn set_bytes(&mut self, value: &[u8]) {
        unsafe {
            pulsar_message_set_content(
                self.inner.as_ptr(),
                value.as_ptr() as *mut std::ffi::c_void,
                value.len(),
            );
        }
    }

    pub fn set_string(&mut self, value: &str) {
        self.set_bytes(value.as_bytes());
    }

    pub fn set_partition_key(&mut self, value: &str) {
        let string = CString::new(value).unwrap();
        unsafe {
            pulsar_message_set_partition_key(self.inner.as_ptr(), string.into_raw());
        }
    }

    pub fn set_message_timestamp(&mut self, value: u64) {
        unsafe {
            pulsar_message_set_event_timestamp(self.inner.as_ptr(), value);
        }
    }

    pub fn get_bytes(&self) -> Vec<u8> {
        let msg = self.inner.as_ptr();
        unsafe {
            let ptr = pulsar_message_get_data(msg);
            let len = pulsar_message_get_length(msg) as usize;
            slice::from_raw_parts(ptr as *mut u8, len).to_vec()
        }
    }

    pub fn get_string(&self) -> String {
        String::from_utf8(self.get_bytes()).unwrap()
    }

    pub fn as_ptr(&mut self) -> *mut pulsar_message_t {
        self.inner.as_ptr()
    }
}

impl Default for Message {
    fn default() -> Self {
        Self::new()
    }
}

impl From<*mut pulsar_message_t> for Message {
    fn from(other: *mut pulsar_message_t) -> Message {
        Message {
            inner: NonNull::new(other).unwrap(),
        }
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        unsafe {
            pulsar_message_free(self.inner.as_ptr());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_roundtrip() {
        //let contents = "hello, world!";
        //let mut m = Message::new();
        //println!("{:?}", m.get_bytes());
        //m.set_bytes(contents.as_bytes());
        //let v = m.get_bytes();
        //assert_eq!(&String::from_utf8(v).unwrap(), contents);
    }
}
