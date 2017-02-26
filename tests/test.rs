extern crate pqbus;
extern crate env_logger;
extern crate postgres;
extern crate retry;

use postgres::{Connection, SslMode};
use retry::retry;

use std::time::Duration;
use std::env;
use std::sync::{Arc, Mutex};
use std::str::FromStr;
use std::thread;

use pqbus::Queue;
use pqbus::error::Error;

struct TestInit;

fn test_setup() -> TestInit {
    let _ = env_logger::init();
    TestInit
}

fn db_uri() -> String {
    env::var("TEST_DB_URI").unwrap_or("postgres://postgres@localhost/pqbus_test".to_string())
}

fn conn() -> Result<Connection, retry::RetryError> {
    match retry(10,
                100,
                || Connection::connect(db_uri().as_ref(), SslMode::None),
                |e| {
                    println!("Connection failed");
                    e.is_ok()
                }) {
        Err(e) => {
            println!("Failed to connect: {}", e);
            Err(e)
        }
        Ok(c) => Ok(c.unwrap()),
    }
}

fn drop_table(name: &str) {
    conn().unwrap().execute(&format!("drop table if exists {} cascade", name), &[]).unwrap();
}

#[test]
fn test_connect_fail() {
    test_setup();
    let bus = pqbus::new("bla", "work");
    assert!(bus.is_err());
}

#[test]
fn test_valid_bus_name() {
    test_setup();
    let bus = pqbus::new(db_uri(), "bad-name");
    match bus {
        Err(Error::InvalidBusName(n)) => assert_eq!("bad-name", &n),
        _ => unreachable!(),
    }
}

#[test]
fn test_valid_queue_name() {
    test_setup();
    let bus = pqbus::new(db_uri(), "work").unwrap();
    let queue: Result<Queue<String>, pqbus::error::Error> = bus.queue("bad-name");
    match queue.as_ref() {
        Err(&Error::InvalidQueueName(ref n)) => assert_eq!("bad-name", n),
        _ => unreachable!(),
    }
}

#[test]
fn test_push() {
    test_setup();
    drop_table("pqbus_push_a_queue");
    let bus = pqbus::new(db_uri(), "push").unwrap();
    let queue = bus.queue("a").unwrap();
    queue.push("a".to_string()).unwrap();
}

#[test]
fn test_empty() {
    test_setup();
    drop_table("pqbus_test_empty_test_queue_queue");
    let bus = pqbus::new(db_uri(), "test_empty").unwrap();
    let queue = bus.queue("test_queue").unwrap();

    assert!(queue.is_empty().unwrap());
    queue.push("a".to_string()).unwrap();
    assert!(!queue.is_empty().unwrap());
    queue.push("a".to_string()).unwrap();
    assert!(!queue.is_empty().unwrap());
}

#[test]
fn test_size() {
    test_setup();
    drop_table("pqbus_test_size_test_queue_queue");
    let bus = pqbus::new(db_uri(), "test_size").unwrap();
    let queue = bus.queue("test_queue").unwrap();

    assert_eq!(0, queue.size().unwrap());
    assert!(queue.is_empty().unwrap());
    queue.push("a".to_string()).unwrap();
    assert_eq!(1, queue.size().unwrap());
    assert!(!queue.is_empty().unwrap());
    queue.push("a".to_string()).unwrap();
    assert_eq!(2, queue.size().unwrap());
    assert!(!queue.is_empty().unwrap());
}

#[test]
fn test_sequential_push_pop() {
    test_setup();
    drop_table("pqbus_test_sequential_push_pop_test_queue_queue");
    let bus = pqbus::new(db_uri(), "test_sequential_push_pop").unwrap();
    let queue = bus.queue("test_queue").unwrap();
    assert!(queue.is_empty().unwrap());

    queue.push("Hello World!".to_string()).unwrap();
    let result = queue.pop_blocking().unwrap();
    assert_eq!("Hello World!", &result);
}

#[test]
fn test_pop_blocking() {
    test_setup();
    drop_table("pqbus_attempt_pop_a_queue");
    let bus = pqbus::new(db_uri(), "attempt_pop").unwrap();
    let queue = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    queue.push("Hello World!".to_string()).unwrap();

    let result = queue.pop_blocking().unwrap();
    assert_eq!("Hello World!", &result);
}

#[test]
fn test_one_bus_duel_queue_push_pop_in_order() {
    test_setup();
    drop_table("pqbus_test_sequential_push_pop_test_queue_a_queue");
    drop_table("pqbus_test_sequential_push_pop_test_queue_b_queue");

    let bus = pqbus::new(db_uri(), "test_sequential_push_pop").unwrap();
    let queue_a = bus.queue("test_queue_a").unwrap();
    let queue_b = bus.queue("test_queue_b").unwrap();
    assert!(queue_a.is_empty().unwrap());
    assert!(queue_b.is_empty().unwrap());

    queue_a.push("a".to_string()).unwrap();
    queue_b.push("b".to_string()).unwrap();

    let result_a = queue_a.pop().unwrap().unwrap();
    let result_b = queue_b.pop().unwrap().unwrap();

    assert_eq!("a", &result_a);
    assert_eq!("b", &result_b);
}

#[test]
fn test_one_bus_duel_queue_push_pop_unorder() {
    test_setup();
    drop_table("pqbus_one_bus_duel_queue_push_pop_unorder_test_queue_a_queue");
    drop_table("pqbus_one_bus_duel_queue_push_pop_unorder_test_queue_b_queue");

    let bus = pqbus::new(db_uri(), "one_bus_duel_queue_push_pop_unorder").unwrap();
    let queue_a = bus.queue("test_queue_a").unwrap();
    let queue_b = bus.queue("test_queue_b").unwrap();
    assert!(queue_a.is_empty().unwrap());
    assert!(queue_b.is_empty().unwrap());

    queue_a.push("a".to_string()).unwrap();
    queue_b.push("b".to_string()).unwrap();

    let result_b = queue_b.pop().unwrap().unwrap();
    let result_a = queue_a.pop().unwrap().unwrap();

    assert_eq!("a", &result_a);
    assert_eq!("b", &result_b);
}

#[test]
fn test_multithread_push_pop() {
    test_setup();
    drop_table("pqbus_multithread_push_pop_a_queue");

    let bus = pqbus::new(db_uri(), "multithread_push_pop").unwrap();
    let queue: Queue<String> = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    let child = thread::spawn(move || {
        let bus = pqbus::new(db_uri(), "multithread_push_pop").unwrap();
        let queue: Queue<String> = bus.queue("a").unwrap();
        queue.pop_blocking()
    });

    queue.push("a".to_string()).unwrap();
    let res = child.join().unwrap();

    assert_eq!("a", &res.unwrap());
}

#[test]
fn test_multithread_push_pop_many() {
    test_setup();
    drop_table("pqbus_multithread_push_pop_many_a_queue");
    let bus = pqbus::new(db_uri(), "multithread_push_pop_many").unwrap();
    let queue: Queue<String> = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    let publisher_count = 10;
    let work_per_publisher = 100;
    let worker_count = 5;
    let work_per_worker = 200;

    assert_eq!(worker_count * work_per_worker,
               publisher_count * work_per_publisher);

    let mut threads = vec![];

    for i in 0..publisher_count {
        threads.push(thread::spawn(move || {
            let bus = pqbus::new(db_uri(), "multithread_push_pop_many").unwrap();
            let queue = bus.queue("a").unwrap();
            for j in 0..work_per_publisher {
                let n = (i * work_per_publisher) + j;
                queue.push(format!("{}", n)).unwrap();
            }
        }));
    }

    let results = Arc::new(Mutex::new(vec![]));

    for _i in 0..worker_count {
        let results = results.clone();
        threads.push(thread::spawn(move || {
            let bus = pqbus::new(db_uri(), "multithread_push_pop_many").unwrap();
            let queue = bus.queue("a").unwrap();
            for _i in 0..work_per_worker {
                let r: String = queue.pop_blocking().unwrap();
                let mut mine = results.lock().unwrap();
                let n: i32 = FromStr::from_str(&r).unwrap();
                mine.push(n);
            }
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    let mut results = results.lock().unwrap();
    assert_eq!(publisher_count * work_per_publisher, results.len());
    results.sort();
    for i in 0..(publisher_count * work_per_publisher) {
        println!("Check {} == {}", i, results[i]);
        assert_eq!(i as i32, results[i]);
    }
}

#[test]
fn test_pop_wait_none() {
    test_setup();
    drop_table("pqbus_pop_wait_none_a_queue");
    let bus = pqbus::new(db_uri(), "pop_wait_none").unwrap();
    let queue: Queue<String> = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    let result = queue.pop_wait(Duration::new(1, 0));

    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(result.is_none());
}

#[test]
fn test_pop_wait_some() {
    test_setup();
    drop_table("pqbus_pop_wait_some_a_queue");
    let bus = pqbus::new(db_uri(), "pop_wait_some").unwrap();
    let queue = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    let child = thread::spawn(|| {
        let bus = pqbus::new(db_uri(), "pop_wait_some").unwrap();
        let queue: Queue<String> = bus.queue("a").unwrap();
        queue.pop_wait(Duration::new(2, 0))
    });

    // crude
    thread::sleep(Duration::new(1, 0));

    queue.push("test".to_string()).unwrap();
    let result = child.join().unwrap().unwrap();

    assert!(result.is_some());
    assert_eq!("test", &result.unwrap());
}

#[test]
fn test_messages_iter_nth() {
    test_setup();
    drop_table("pqbus_messages_iter_nth_a_queue");
    let bus = pqbus::new(db_uri(), "messages_iter_nth").unwrap();
    let queue = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    queue.push("1".to_string()).unwrap();
    queue.push("2".to_string()).unwrap();
    queue.push("3".to_string()).unwrap();
    queue.push("4".to_string()).unwrap();

    let third = queue.messages().nth(2).unwrap();
    assert_eq!("3", &third.unwrap());
}

#[test]
fn test_messages_waiting_iter() {
    test_setup();
    drop_table("pqbus_iter_nth_a_queue");
    let bus = pqbus::new(db_uri(), "iter_nth").unwrap();
    let queue = bus.queue("a").unwrap();
    assert!(queue.is_empty().unwrap());

    queue.push("1".to_string()).unwrap();
    queue.push("2".to_string()).unwrap();
    queue.push("3".to_string()).unwrap();
    queue.push("4".to_string()).unwrap();

    let mut i = 0;
    for _message in queue.messages() {
        i += 1;
    }

    assert_eq!(4, i);
}
