extern crate pulsar_rs;

use pulsar_rs::client::{Client, ClientConfig};
use pulsar_rs::consumer::{Consumer, ConsumerConfig};
use pulsar_rs::error::Error as PError;
use pulsar_rs::message::Message;
use pulsar_rs::producer::{Producer, ProducerConfig};

#[test]
fn test_client() {
    let c = Client::new("pulsar://localhost:6650", ClientConfig::new().unwrap()).unwrap();
    c.close().unwrap();
}

#[test]
fn test_producer() {
    let mut c = Client::new("pulsar://localhost:6650", ClientConfig::new().unwrap()).unwrap();

    let p = Producer::new(
        &mut c,
        "public/default/test",
        ProducerConfig::new().unwrap(),
    )
    .unwrap();

    p.close().unwrap();
    c.close().unwrap();
}

#[test]
fn test_produce_many() {
    let mut c = Client::new("pulsar://localhost:6650", ClientConfig::new().unwrap()).unwrap();

    let p = Producer::new(
        &mut c,
        "public/default/test",
        ProducerConfig::new().unwrap(),
    )
    .unwrap();

    for i in 1..100 {
        let mut m = Message::new();
        m.set_string(&format!("{}", i));
        p.send(m);
    }

    //p.flush();
    p.close().unwrap();
    c.close().unwrap();
}

#[test]
fn test_consumer() {
    let mut client = Client::new("pulsar://localhost:6650", ClientConfig::new().unwrap()).unwrap();
    let c = Consumer::new(
        &mut client,
        "public/default/test",
        "test",
        ConsumerConfig::new().unwrap(),
    )
    .unwrap();

    let m = c.recv().expect("no message?");
    println!("{:?}", m);
    println!("{:?}", m.get_bytes());
    c.close();
    client.close();
}

#[test]
fn test_consume_many() {
    let mut client = Client::new("pulsar://localhost:6650", ClientConfig::new().unwrap()).unwrap();
    let c = Consumer::new(
        &mut client,
        "public/default/test",
        "test",
        ConsumerConfig::new().unwrap(),
    )
    .unwrap();

    for i in 1..100 {
        let m = c.recv().expect("no message?");
        println!("{:?}", String::from_utf8(m.get_bytes()));
        assert_eq!(
            String::from_utf8(m.get_bytes()).unwrap(),
            format!("{}", i)
        );
        c.ack_async(m);
    }
    c.close();
    client.close();
}
