extern crate postgres;
extern crate retry;

use postgres::{Connection, SslMode};
use postgres::notification::Notifications;
use postgres::stmt::Statement;
use retry::retry;
use std::result;
use std::time::Duration;
// use std::error::Error as StdError;

use error::Error;

pub mod error;

pub type Result<T> = result::Result<T, Error>;

pub struct PqBus {
    name: String,
    conn: Connection,
}

pub struct Queue<'a> {
    notifications: Notifications<'a>,
    pop_stmt: Statement<'a>,
    push_stmt: Statement<'a>,
    notify_stmt: Statement<'a>,
    size_stmt: Statement<'a>,
}

pub fn new<S, T>(db_uri: S, name: T) -> Result<PqBus>
    where S: Into<String>,
          T: Into<String>
{
    let uri = db_uri.into();

    let conn = match retry(10,
                           100,
                           || Connection::connect(uri.as_ref(), SslMode::None),
                           |r| {
                               if let &Err(ref e) = r {
                                   println!("Failed to connect to postgresql server: {}", e);
                               }
                               r.is_ok()
                           }) {
        Err(e) => {
            println!("Giving up on postgresql server connection: {}", e);
            return Err(Error::Connection(uri, e));
        }
        Ok(c) => c.unwrap(),
    };

    Ok(PqBus {
        conn: conn,
        name: name.into(),
    })
}

impl PqBus {
    fn table_name<S>(&self, queue_name: S) -> String
        where S: Into<String>
    {
        format!("pqbus_{}_{}_queue", self.name, queue_name.into())
    }

    pub fn queue<'a>(&'a self, name: &str) -> Result<Queue<'a>> {
        let table_name = self.table_name(name);
        try!(self.conn
            .execute(&format!(r#"
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    body VARCHAR NOT NULL,
                    lock VARCHAR DEFAULT NULL
                )"#,
                              table_name),
                     &[])
            .map_err(|e| Error::Create(e)));
        Queue::new(&self.conn, &self.table_name(name))
    }
}

/// A push pop message queue.
impl<'a> Queue<'a> {
    fn new(conn: &'a Connection, table_name: &String) -> Result<Self> {
        try!(conn.execute(&format!("LISTEN {}", table_name), &[]).map_err(|e| Error::Listen(e)));
        Ok(Queue {
            notifications: conn.notifications(),
            push_stmt:
                try!(conn.prepare_cached(&format!("INSERT INTO {} (body) VALUES ($1)", table_name))),
            notify_stmt: try!(conn.prepare_cached(&format!("NOTIFY {}", table_name))),
            size_stmt: try!(conn.prepare_cached(&format!("SELECT count(*) FROM  {}", table_name))),
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

    /// Returns the number of messages in the queue.
    pub fn size(&self) -> Result<i64> {
        let result = try!(self.size_stmt.query(&[]).map_err(|e| Error::Size(e)));
        let row = result.get(0);
        Ok(row.get("count"))
    }

    /// Determines if there are any pending messages.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(try!(self.size()) == 0)
    }

    /// Pushes a message into the queue.
    pub fn push<S>(&self, body: S) -> Result<bool>
        where S: Into<String>
    {
        // TODO use bytes for body. generics for conversion?
        try!(self.push_stmt.execute(&[&body.into()]).map_err(|e| Error::Push(e)));
        try!(self.notify_stmt.execute(&[]).map_err(|e| Error::Notify(e)));
        Ok(true)
    }

    /// Pops a message from the queue. Blocks if there are none pending.
    pub fn pop_blocking(&self) -> Result<String> {
        loop {
            let p = try!(self.pop());
            if p.is_some() {
                return Ok(p.unwrap());
            }
            self.notifications.blocking_iter().next();
        }
    }

    /// Pops a message from the queue. Blocks if there are none pending.
    pub fn pop_wait(&self, timeout: Duration) -> Result<Option<String>> {
        {
            let p = try!(self.pop());
            if p.is_some() {
                return Ok(p);
            }
        }
        self.notifications.timeout_iter(timeout).next();
        {
            let p = try!(self.pop());
            if p.is_some() {
                return Ok(p);
            }
        }
        Ok(None)
    }

    /// Run a closure on messages in the queue. Blocks if there are none pending.
    pub fn pop_callback<F>(&self, work_fn: F) -> Result<bool>
        where F: Fn(String)
    {
        loop {
            self.consume_pending_notifications();
            try!(self.consume_pending_items(&work_fn));
            self.wait_for_next_notification();
        }
    }

    /// Pops a message from the queue if there is one pending.
    pub fn pop(&self) -> Result<Option<String>> {
        let locked = try!(self.pop_stmt.query(&[]).map_err(|e| Error::Pop(e)));
        if locked.is_empty() {
            return Ok(None);
        }

        let locked_row = locked.get(0);
        let _id: i32 = match locked_row.get_opt("id") {
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
            match try!(self.pop()) {
                None => return Ok(i),
                Some(body) => {
                    work_fn(body);
                    i += 1;
                }
            }
        }
    }

    fn wait_for_next_notification(&self) {
        self.notifications.blocking_iter().next();
    }

    /// Returns an iterator over pending messages
    pub fn messages(&'a self) -> MessageIter<'a, NextMessagePending> {
        MessageIter::new(self, NextMessagePending {})
    }

    /// Returns an iterator over messages that blocks until a message is received if none are pending.
    pub fn messages_blocking(&'a self) -> MessageIter<'a, NextMessageBlocking> {
        MessageIter::new(self, NextMessageBlocking {})
    }
}

pub trait NextMessage {
    fn next(&self, &Queue) -> Option<Result<String>>;
}

pub struct MessageIter<'a, N>
    where N: NextMessage
{
    next_message: N,
    queue: &'a Queue<'a>,
}

impl<'a, N> MessageIter<'a, N>
    where N: NextMessage
{
    fn new(queue: &'a Queue<'a>, n: N) -> Self {
        MessageIter {
            next_message: n,
            queue: queue,
        }
    }
}

impl<'a, N> Iterator for MessageIter<'a, N>
    where N: NextMessage
{
    type Item = Result<String>;

    fn next(&mut self) -> Option<Result<String>> {
        self.next_message.next(self.queue)
    }
}

pub struct NextMessageBlocking;
impl NextMessage for NextMessageBlocking {
    fn next(&self, q: &Queue) -> Option<Result<String>> {
        Some(q.pop_blocking())
    }
}

pub struct NextMessagePending;
impl NextMessage for NextMessagePending {
    fn next(&self, q: &Queue) -> Option<Result<String>> {
        match q.pop() {
            Ok(Some(m)) => Some(Ok(m)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
