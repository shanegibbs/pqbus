extern crate pqbus;
extern crate postgres;

use postgres::{Connection, SslMode};
use std::env;

fn db_uri() -> String {
    env::var("TEST_DB_URI").unwrap_or("postgres://postgres@localhost/pqbus_test".to_string())
}

fn conn() -> Connection {
    match Connection::connect(db_uri().as_ref(), SslMode::None) {
        Err(e) => {
            println!("Failed to connect: {}", e);
            panic!(e);
        }
        Ok(c) => c,
    }
}

fn drop_table(name: &str) {
    conn().execute(&format!("drop table if exists {} cascade", name), &[]).unwrap();
}

#[test]
fn test_connect_fail() {
    let bus = pqbus::new("bla", "work");
    assert!(bus.is_err());
}

#[test]
fn test_push() {
    drop_table("pqbus_push_a");
    let bus = pqbus::new(db_uri(), "push").unwrap();
    let queue = bus.queue("a").unwrap();

    queue.push("a").unwrap();

}

#[test]
fn test_empty() {
    drop_table("pqbus_test_empty_test_queue_queue");
    let bus = pqbus::new(db_uri(), "test_empty").unwrap();
    let queue = bus.queue("test_queue").unwrap();

    assert!(queue.is_empty().unwrap());
    queue.push("a").unwrap();
    assert!(!queue.is_empty().unwrap());
    queue.push("a").unwrap();
    assert!(!queue.is_empty().unwrap());
}

#[test]
fn test_size() {
    drop_table("pqbus_test_size_test_queue_queue");
    let bus = pqbus::new(db_uri(), "test_size").unwrap();
    let queue = bus.queue("test_queue").unwrap();

    assert_eq!(0, queue.size().unwrap());
    assert!(queue.is_empty().unwrap());
    queue.push("a").unwrap();
    assert_eq!(1, queue.size().unwrap());
    assert!(!queue.is_empty().unwrap());
    queue.push("a").unwrap();
    assert_eq!(2, queue.size().unwrap());
    assert!(!queue.is_empty().unwrap());
}

#[test]
fn test_sequential_push_pop() {
    drop_table("pqbus_test_sequential_push_pop_test_queue_queue");
    let bus = pqbus::new(db_uri(), "test_sequential_push_pop").unwrap();
    let queue = bus.queue("test_queue").unwrap();
    assert!(queue.is_empty().unwrap());

    queue.push("Hello World!").unwrap();
    let result = queue.pop().unwrap();
    assert_eq!("Hello World!".to_string(), result)
}

#[test]
fn test_one_bus_duel_queue_push_pop_in_order() {
    drop_table("pqbus_test_sequential_push_pop_test_queue_a_queue");
    drop_table("pqbus_test_sequential_push_pop_test_queue_b_queue");

    let bus = pqbus::new(db_uri(), "test_sequential_push_pop").unwrap();
    let queue_a = bus.queue("test_queue_a").unwrap();
    let queue_b = bus.queue("test_queue_b").unwrap();
    assert!(queue_a.is_empty().unwrap());
    assert!(queue_b.is_empty().unwrap());

    queue_a.push("a").unwrap();
    queue_b.push("b").unwrap();

    let result_a = queue_a.pop().unwrap();
    let result_b = queue_b.pop().unwrap();

    assert_eq!("a".to_string(), result_a);
    assert_eq!("b".to_string(), result_b);
}

#[test]
fn test_one_bus_duel_queue_push_pop_unorder() {
    drop_table("pqbus_one_bus_duel_queue_push_pop_unorder_test_queue_a_queue");
    drop_table("pqbus_one_bus_duel_queue_push_pop_unorder_test_queue_b_queue");

    let bus = pqbus::new(db_uri(), "one_bus_duel_queue_push_pop_unorder").unwrap();
    let queue_a = bus.queue("test_queue_a").unwrap();
    let queue_b = bus.queue("test_queue_b").unwrap();
    assert!(queue_a.is_empty().unwrap());
    assert!(queue_b.is_empty().unwrap());

    queue_a.push("a").unwrap();
    queue_b.push("b").unwrap();

    let result_b = queue_b.pop().unwrap();
    let result_a = queue_a.pop().unwrap();

    assert_eq!("a".to_string(), result_a);
    assert_eq!("b".to_string(), result_b);
}
