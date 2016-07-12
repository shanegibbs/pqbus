extern crate postgres;

use std::env;
use std::thread::sleep;
use std::time::Duration;
use postgres::{Connection, SslMode};

fn init(conn: &Connection) {
    println!("(Re)init queue");

    match conn.execute("DROP TABLE IF EXISTS queue;", &[]) {
        Err(e) => println!("Drop table failed: {}", e),
        Ok(_) => (),
    }
    match conn.execute(r#"
                CREATE TABLE queue (
                    id SERIAL PRIMARY KEY,
                    body VARCHAR NOT NULL,
                    lock VARCHAR DEFAULT NULL
                )"#,
                       &[]) {
        Err(e) => println!("Create table failed: {}", e),
        Ok(_) => (),
    }
}

fn push(conn: &Connection, body: String) {
    match conn.execute("INSERT INTO queue (body) VALUES ($1)", &[&body]) {
        Err(e) => println!("Failed to insert: {}", e),
        Ok(_) => println!("Pushed data"),
    }
    match conn.execute("NOTIFY queue, 'You got work!';", &[]) {
        Err(e) => println!("Failed to insert: {}", e),
        Ok(_) => println!("Sent NOTIFY to queue"),
    }
}

fn register(conn: &Connection) {
    match conn.execute("LISTEN queue", &[]) {
        Err(e) => println!("Failed to listen: {}", e),
        Ok(_) => println!("Listening"),
    };
}

fn attempt_pop(conn: &Connection) -> Option<String> {
    let locked = match conn.query(r#"
                        UPDATE queue q
                        SET lock = 'me'
                        FROM  (
                           SELECT id,body
                           FROM   queue
                           WHERE  lock is NULL
                           LIMIT  1
                           FOR UPDATE SKIP LOCKED
                           ) sub
                        WHERE q.id = sub.id
                        RETURNING q.id, q.body;
                        "#,
                                  &[]) {
        Err(e) => {
            println!("Failed to obtain lock: {}", e);
            return None;
        }
        Ok(r) => r,
    };

    if locked.is_empty() {
        println!("Queue is empty");
        return None;
    }
    let locked_row = locked.get(0);
    let id: i32 = match locked_row.get_opt("id") {
        None => {
            println!("No lock obtained");
            return None;
        }
        Some(Err(e)) => {
            println!("Failed to convert lock value: {}", e);
            return None;
        }
        Some(Ok(r)) => r,
    };
    println!("Got work id {}", id);

    let body: String = match locked_row.get_opt("body") {
        None => {
            println!("No body obtained");
            return None;
        }
        Some(Err(e)) => {
            println!("Failed to convert body value: {}", e);
            return None;
        }
        Some(Ok(r)) => r,
    };
    return Some(body);
}

fn consume_pending_notifications(conn: &Connection) {
    let notes = conn.notifications();
    while !notes.is_empty() {
        notes.iter().next();
    }
}

fn consume_pending_items<F>(conn: &Connection, work_fn: F)
    where F: Fn(String)
{
    loop {
        match attempt_pop(&conn) {
            None => return,
            Some(body) => {
                work_fn(body);
            }
        }
    }
}

fn wait_for_next_notification(conn: &Connection) {
    let notes = conn.notifications();
    notes.timeout_iter(Duration::new(5, 0)).next();
}

fn consume<F>(conn: &Connection, work_fn: F)
    where F: Fn(String)
{
    loop {
        consume_pending_notifications(conn);
        consume_pending_items(conn, &work_fn);
        wait_for_next_notification(conn);
    }
}

fn main() {
    let mut args = env::args();
    args.next(); // skip first

    let cmd = args.next().unwrap_or("".to_string());
    let db_uri = args.next().unwrap_or("".to_string());

    let conn = match Connection::connect(db_uri.as_ref(), SslMode::None) {
        Err(e) => {
            println!("Failed to connect to database: {}", e);
            std::process::exit(1);
        }
        Ok(c) => c,
    };

    if cmd == "init" {
        init(&conn);

    } else if cmd == "publisher" {
        for x in 0..1000 {
            let d = format!("some_data {}", x);
            push(&conn, d);
            sleep(Duration::new(1, 0)); // 1 second
        }

    } else if cmd == "consumer" {
        register(&conn);
        consume(&conn, |body| {
            println!("Body: {}", body);
        });

    }
}
