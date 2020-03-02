extern crate libc;

use crate::bindings::*;
use crate::error::Error;
use crate::message::Message;
use std::ffi::CString;
use std::mem;
use std::ptr;
use std::slice;

#[test]
pub fn test_client_create() {
    unsafe {
        let conf = pulsar_client_configuration_create();
        assert!(!conf.is_null());
        let client = pulsar_client_create(
            CString::new("pulsar://localhost:6650").unwrap().as_ptr(),
            conf,
        );
        assert!(!client.is_null());
        assert_eq!(pulsar_client_close(client), pulsar_result_pulsar_result_Ok);
        pulsar_client_configuration_free(conf);
        pulsar_client_free(client);
    }
}

#[test]
pub fn test_raw_producer() {
    unsafe {
        let conf = pulsar_client_configuration_create();
        assert!(!conf.is_null());
        let client = pulsar_client_create(
            CString::new("pulsar://localhost:6650").unwrap().as_ptr(),
            conf,
        );
        assert!(!client.is_null());

        // actual producer code
        let pconf = pulsar_producer_configuration_create();
        let mut producer: *mut pulsar_producer_t = ptr::null_mut();
        assert_eq!(
            pulsar_client_create_producer(
                client,
                CString::new("public/default/test").unwrap().as_ptr(),
                pconf,
                &mut producer
            ),
            pulsar_result_pulsar_result_Ok
        );

        let msg = pulsar_message_create();
        let contents = CString::new("hello, world!").unwrap();
        let bytes = contents.to_bytes_with_nul();
        pulsar_message_set_content(msg, bytes.as_ptr() as *const std::ffi::c_void, bytes.len());
        assert_eq!(
            pulsar_producer_send(producer, msg),
            pulsar_result_pulsar_result_Ok
        );

        pulsar_message_free(msg);

        assert_eq!(
            pulsar_producer_close(producer),
            pulsar_result_pulsar_result_Ok
        );
        pulsar_producer_free(producer);
        pulsar_producer_configuration_free(pconf);
        assert_eq!(pulsar_client_close(client), pulsar_result_pulsar_result_Ok);
        pulsar_client_free(client);
        pulsar_client_configuration_free(conf);
    }
}

#[test]
pub fn test_raw_consumer() {
    unsafe {
        let conf = pulsar_client_configuration_create();
        assert!(!conf.is_null());
        let client = pulsar_client_create(
            CString::new("pulsar://localhost:6650").unwrap().as_ptr(),
            conf,
        );
        assert!(!client.is_null());

        // actual consumer code
        let cconf = pulsar_consumer_configuration_create();
        let mut consumer = ptr::null_mut();
        pulsar_consumer_set_consumer_name(cconf, CString::new("testy").unwrap().as_ptr());
        pulsar_consumer_configuration_set_consumer_type(
            cconf,
            pulsar_consumer_type_pulsar_ConsumerExclusive,
        );

        assert_eq!(
            pulsar_client_subscribe(
                client,
                CString::new("public/default/test").unwrap().as_ptr(),
                CString::new("test").unwrap().as_ptr(),
                cconf,
                &mut consumer,
            ),
            pulsar_result_pulsar_result_Ok
        );

        loop {
            let mut msg = ptr::null_mut();
            let code = pulsar_consumer_receive_with_timeout(consumer, &mut msg, 1000);
            if (code == pulsar_result_pulsar_result_Timeout) {
                break;
            } else {
                assert_eq!(code, 0);
            }
            let msgObj = Message::from(msg);
            let id = pulsar_message_get_message_id(msg);
            let id_str = pulsar_message_id_str(id);
            let id_cstr = CString::from_raw(id_str);
            let data = pulsar_message_get_data(msg);
            let len = pulsar_message_get_length(msg) as usize;
            let str = slice::from_raw_parts(data as *const u8, len);
            let S = ::std::str::from_utf8_unchecked(str).to_string();
            //let str = CString::from_raw(data as *mut ::std::os::raw::c_char);
            println!("-------------------------------");
            println!("{:?}, {:?}, {:?}", id, id_str, id_cstr);
            println!("{:?}, {:?}, {:?}", data, str, S);
            println!("{:?}", msgObj.get_string());
            println!("-------------------------------");

            //assert_eq!(
            //    pulsar_consumer_acknowledge(consumer, msg),
            //    pulsar_result_pulsar_result_Ok
            //);
            pulsar_message_id_free(id);
            //pulsar_message_free(msg); // freed by msgObj
        }

        assert_eq!(
            pulsar_consumer_close(consumer),
            pulsar_result_pulsar_result_Ok
        );
        assert_eq!(pulsar_client_close(client), pulsar_result_pulsar_result_Ok);
        pulsar_consumer_configuration_free(cconf);
        pulsar_consumer_free(consumer);
        pulsar_client_configuration_free(conf);
        pulsar_client_free(client);
    }
}
