extern crate postgres;

use postgres::{Connection, SslMode};
use postgres::notification::Notifications;
use postgres::stmt::Statement;
use std::result;

use error::Error;

pub mod error;

pub type Result<T> = result::Result<T, Error>;

pub struct PqBus {
    name: String,
    conn: Connection,
}

pub struct QueuePush<'a> {
    push_stmt: Statement<'a>,
    notify_stmt: Statement<'a>,
}

pub struct QueuePop<'a> {
    notifications: Notifications<'a>,
    pop_stmt: Statement<'a>,
}

pub fn new(db_uri: &str, name: &str) -> Result<PqBus> {
    Ok(PqBus {
        conn: try!(Connection::connect(db_uri, SslMode::None)),
        name: name.to_string(),
    })
}

impl PqBus {
    fn table_name(&self, queue_name: &str) -> String {
        format!("{}_{}_queue", self.name, queue_name)
    }

    pub fn create_push<'a>(&'a self, name: &str) -> Result<QueuePush<'a>> {
        let table_name = self.table_name(name);
        try!(self.conn.execute(&format!(r#"
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    body VARCHAR NOT NULL,
                    lock VARCHAR DEFAULT NULL
                )"#,
                                        table_name),
                               &[]));
        Ok(QueuePush {
            push_stmt: try!(self.conn
                .prepare_cached(&format!("INSERT INTO {} (body) VALUES ($1)", table_name))),
            notify_stmt: try!(self.conn.prepare_cached(&format!("NOTIFY {}", table_name))),
        })
    }

    pub fn create_pop<'a>(&'a self, name: &str) -> Result<QueuePop<'a>> {
        QueuePop::new(&self.conn, &self.table_name(name))
    }

    // fn size() ?
}

impl<'a> QueuePush<'a> {
    pub fn push(&self, body: &String) -> Result<bool> {
        // TODO use bytes for body. generics for conversion?
        try!(self.push_stmt.execute(&[body]));
        try!(self.notify_stmt.execute(&[]));
        Ok(true)
    }
}

impl<'a> QueuePop<'a> {
    fn new(conn: &'a Connection, table_name: &String) -> Result<Self> {
        try!(conn.execute(&format!("LISTEN {}", table_name), &[]));
        Ok(QueuePop {
            notifications: conn.notifications(),
            pop_stmt: try!(conn.prepare_cached(&format!(r#"
                        UPDATE {n} q
                        SET lock = 'me'
                        FROM  (
                           SELECT id,body
                           FROM   {n}
                           WHERE  lock is NULL
                           LIMIT  1
                           FOR UPDATE SKIP LOCKED
                           ) sub
                        WHERE q.id = sub.id
                        RETURNING q.id, q.body;
                        "#,
                                                        n = table_name))),
        })
    }

    pub fn pop(&self) -> Result<String> {
        loop {
            let p = try!(self.attempt_pop());
            if p.is_some() {
                return Ok(p.unwrap());
            }
            self.notifications.blocking_iter().next();
        }
    }

    pub fn pop_callback<F>(&self, work_fn: F) -> Result<bool>
        where F: Fn(String)
    {
        loop {
            self.consume_pending_notifications();
            self.consume_pending_items(&work_fn);
            self.wait_for_next_notification();
        }
    }

    fn attempt_pop(&self) -> Result<Option<String>> {
        let locked = try!(self.pop_stmt.query(&[]));
        if locked.is_empty() {
            return Ok(None);
        }

        let locked_row = locked.get(0);
        let id: i32 = match locked_row.get_opt("id") {
            None => {
                println!("No lock obtained");
                return Ok(None);
            }
            Some(Err(e)) => {
                println!("Failed to convert lock value: {}", e);
                return Ok(None);
            }
            Some(Ok(r)) => r,
        };
        println!("Got work id {}", id);

        let body: String = match locked_row.get_opt("body") {
            None => {
                println!("No body obtained");
                return Ok(None);
            }
            Some(Err(e)) => {
                println!("Failed to convert body value: {}", e);
                return Ok(None);
            }
            Some(Ok(r)) => r,
        };
        return Ok(Some(body));
    }

    fn consume_pending_notifications(&self) {
        while !self.notifications.is_empty() {
            self.notifications.iter().next();
        }
    }

    fn consume_pending_items<F>(&self, work_fn: F) -> Result<u32>
        where F: Fn(String)
    {
        let mut i = 0;
        loop {
            match try!(self.attempt_pop()) {
                None => return Ok(i),
                Some(body) => {
                    work_fn(body);
                    i += 1;
                }
            }
        }
    }

    fn wait_for_next_notification(&self) {
        // self.notifications.timeout_iter(Duration::new(5, 0)).next();
        self.notifications.blocking_iter().next();
    }
}
