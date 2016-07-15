extern crate pqbus;
extern crate postgres;

use postgres::{Connection, SslMode};
use std::env;

fn db_uri() -> String {
    env::var("TEST_DB_URI").unwrap_or("postgres://postgres@localhost/pqbus_test".to_string())
}

fn drop_table(name: &str) {
    let conn = Connection::connect(db_uri().as_ref(), SslMode::None).unwrap();
    conn.execute(&format!("drop table if exists {} cascade", name), &[]).unwrap();
}

#[test]
fn test_connect_fail() {
    let bus = pqbus::new("bla", "work");
    assert!(bus.is_err());
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
