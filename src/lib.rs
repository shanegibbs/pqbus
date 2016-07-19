//! A simple event driven highly available message queue backed with PostgreSQL,
//! focused on stability and performance.
//!
//! ## Producer
//!
//! ```rust,no_run
//! extern crate pqbus;
//!
//! use pqbus::Queue;
//! use pqbus::messages::StringMessage;
//!
//! fn main() {
//!     let bus = pqbus::new("postgres://postgres@localhost/pqbus", "myapp").unwrap();
//!     let queue = bus.queue("new_users").unwrap();
//!     queue.push(StringMessage::new("sgibbs"));
//! }
//! ```
//!
//! ## Consumer
//!
//! ```rust,no_run
//! extern crate pqbus;
//!
//! use pqbus::Queue;
//! use pqbus::messages::StringMessage;
//!
//! fn main() {
//!     let bus = pqbus::new("postgres://postgres@localhost/pqbus", "myapp").unwrap();
//!     let queue: Queue<StringMessage> = bus.queue("new_users").unwrap();
//!     for message in queue.messages_blocking() {
//!         println!("New User: {}", message.unwrap());
//!     }
//! }
//! ```
//!
//! ## Custom Messages
//!
//! Any struct that satisfies the trait bonds `From<Vec<u8>>` and `Into<Vec<u8>>` the can be sent
//! over a queue.
//!
//! ```rust,no_run
//! extern crate pqbus;
//!
//! struct User;
//!
//! impl Into<Vec<u8>> for User {
//!     fn into(self) -> Vec<u8> {
//!         vec![0, 1, 0, 1]
//!     }
//! }
//!
//! impl From<Vec<u8>> for User {
//!     fn from(v: Vec<u8>) -> User {
//!         User
//!     }
//! }
//!
//! fn main() {
//!     let bus = pqbus::new("postgres://postgres@localhost/pqbus", "myapp").unwrap();
//!     let queue = bus.queue("new_users").unwrap();
//!
//!     {
//!         let user = User;
//!         queue.push(user);
//!     }
//!
//!     let user = queue.pop();
//! }
//! ```
//!
#![crate_type = "lib"]
#![deny(missing_docs)]

#[macro_use]
extern crate log;
extern crate postgres;
extern crate retry;
extern crate regex;

use postgres::{Connection, SslMode};
use postgres::notification::{Notification, Notifications};
use postgres::stmt::Statement;
use retry::retry;
use std::result;
use std::time::Duration;
use std::marker::PhantomData;
use regex::Regex;

use error::Error;
use iter::{MessageIter, NextMessageBlocking, NextMessagePending};

pub mod error;
pub mod iter;
pub mod messages;

/// Convenience alias
pub type Result<T> = result::Result<T, Error>;

/// Highest level namespace. Constructs `Queue`s.
pub struct PqBus {
    name: String,
    conn: Connection,
}

/// A named message queue
pub struct Queue<'a, T>
    where T: From<Vec<u8>> + Into<Vec<u8>>
{
    notifications: Notifications<'a>,
    pop_stmt: Statement<'a>,
    push_stmt: Statement<'a>,
    notify_stmt: Statement<'a>,
    size_stmt: Statement<'a>,
    name: String,
    bus: String,
    phantom: PhantomData<T>,
}

/// Constructs a new PqBus
///
/// # Example
///
/// ```rust,no_run
/// let bus = pqbus::new("postgres://postgres@localhost/pqbus", "myapp").unwrap();
/// ```
pub fn new<S, T>(db_uri: S, name: T) -> Result<PqBus>
    where S: Into<String>,
          T: Into<String>
{
    let uri = db_uri.into();
    let name = name.into();

    if invalid_name(&name) {
        return Err(Error::InvalidBusName(name));
    }

    let mut last_err = None;

    let conn = match retry(10,
                           100,
                           || Connection::connect(uri.as_ref(), SslMode::None),
                           |r| {
        if let &Err(ref e) = r {
            warn!("Failed to connect to postgresql: {}", e);
            last_err = Some(format!("Unable to connect to {}: {}", uri, e));
        }
        r.is_ok()
    }) {
        Err(e) => {
            match last_err {
                None => error!("Giving up on connection to postgresql: {}", e),
                Some(e) => error!("{}", e),
            }
            return Err(Error::Connection(uri, e));
        }
        Ok(c) => c.unwrap(),
    };

    info!("Connected to bus {}", name.clone());

    Ok(PqBus {
        conn: conn,
        name: name.clone(),
    })
}

impl PqBus {
    /// Constructs a queue on the bus from the given `name`.
    pub fn queue<'a, N, T>(&'a self, name: N) -> Result<Queue<'a, T>>
        where T: From<Vec<u8>> + Into<Vec<u8>>,
              N: Into<String>
    {
        Queue::new(&self.conn, &name.into(), &self.name)
    }
}

fn table_name_generator(bus: &String, queue: &String) -> String {
    format!("pqbus_{}_{}_queue", bus, queue)
}

/// A push pop message queue.
impl<'a, T> Queue<'a, T>
    where T: From<Vec<u8>> + Into<Vec<u8>>
{
    fn new(conn: &'a Connection, name: &String, bus: &String) -> Result<Self> {

        if invalid_name(name) {
            return Err(Error::InvalidQueueName(name.clone()));
        }

        info!("Creating queue {}.{}", bus, name);

        let table_name = table_name_generator(bus, name);

        try!(conn.execute(&format!(r#"
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    message bytea NOT NULL,
                    lock VARCHAR DEFAULT NULL
                )"#,
                              table_name),
                     &[])
            .map_err(|e| Error::Create(e)));

        try!(conn.execute(&format!("LISTEN {}", table_name), &[]).map_err(|e| Error::Listen(e)));

        Ok(Queue {
            notifications: conn.notifications(),
            push_stmt:
                try!(conn.prepare_cached(&format!("INSERT INTO {} (message) VALUES ($1)", table_name))),
            notify_stmt: try!(conn.prepare_cached(&format!("NOTIFY {}", table_name))),
            size_stmt: try!(conn.prepare_cached(&format!("SELECT count(*) FROM  {}", table_name))),
            pop_stmt: try!(conn.prepare_cached(&format!(r#"
                        UPDATE {n} q
                        SET lock = 'me'
                        FROM  (
                           SELECT id,message
                           FROM   {n}
                           WHERE  lock is NULL
                           LIMIT  1
                           FOR UPDATE SKIP LOCKED
                           ) sub
                        WHERE q.id = sub.id
                        RETURNING q.id, q.message;
                        "#,
                                                        n = table_name))),
            name: name.clone(),
            bus: bus.clone(),
            phantom: PhantomData,
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
    pub fn push(&self, message: T) -> Result<bool>
        where T: Into<Vec<u8>>
    {
        let b: Vec<u8> = message.into();
        try!(self.push_stmt.execute(&[&b]).map_err(|e| Error::Push(e)));
        info!("Message pushed to queue {}.{}", self.bus, self.name);

        try!(self.notify_stmt.execute(&[]).map_err(|e| Error::Notify(e)));
        debug!("Sent push notification to queue {}.{}", self.bus, self.name);

        Ok(true)
    }

    /// Pops a message from the queue. Blocks if there are none pending.
    pub fn pop_blocking(&self) -> Result<T>
        where T: From<Vec<u8>>
    {
        loop {
            let p = try!(self.pop());
            if p.is_some() {
                return Ok(p.unwrap());
            }
            try!(self.handle_notification(self.notifications.blocking_iter()));
        }
    }

    /// Pops a message from the queue. Blocks for duration of `timeout` if there are none pending.
    pub fn pop_wait(&self, timeout: Duration) -> Result<Option<T>>
        where T: From<Vec<u8>>
    {
        {
            let p = try!(self.pop());
            if p.is_some() {
                return Ok(p);
            }
        }
        if let Some(_n) = try!(self.handle_notification(self.notifications.timeout_iter(timeout))) {
            let p = try!(self.pop());
            if p.is_some() {
                return Ok(p);
            }
        }
        Ok(None)
    }

    /// Run a closure on messages in the queue. Blocks if there are none pending.
    pub fn pop_callback<F>(&self, work_fn: F) -> Result<bool>
        where F: Fn(T)
    {
        loop {
            try!(self.consume_pending_notifications());
            try!(self.consume_pending_items(&work_fn));
            try!(self.wait_for_next_notification());
        }
    }

    /// Pops a message from the queue if there is one pending.
    pub fn pop(&self) -> Result<Option<T>>
        where T: From<Vec<u8>>
    {
        let locked = try!(self.pop_stmt.query(&[]).map_err(|e| Error::Pop(e)));
        if locked.is_empty() {
            debug!("No message available in {}.{}", self.bus, self.name);
            return Ok(None);
        }

        let locked_row = locked.get(0);
        let _id: i32 = match locked_row.get_opt("id") {
            None => {
                warn!("No id column in {}.{}", self.bus, self.name);
                return Ok(None);
            }
            Some(Err(e)) => {
                warn!("Failed to convert id column value in {}.{}: {}",
                      self.bus,
                      self.name,
                      e);
                return Ok(None);
            }
            Some(Ok(r)) => r,
        };

        let message: Vec<u8> = match locked_row.get_opt("message") {
            None => {
                warn!("No message column in {}.{}", self.bus, self.name);
                return Ok(None);
            }
            Some(Err(e)) => {
                warn!("Failed to convert message column value in {}.{}: {}",
                      self.bus,
                      self.name,
                      e);
                return Ok(None);
            }
            Some(Ok(r)) => r,
        };

        info!("Received message from {}.{}", self.bus, self.name);

        return Ok(Some(message.into()));
    }

    fn consume_pending_notifications(&self) -> Result<Option<Notification>> {
        let mut last = None;
        while !&self.notifications.is_empty() {
            last = try!(self.handle_notification(self.notifications.iter()));
        }
        Ok(last)
    }

    fn consume_pending_items<F>(&self, work_fn: F) -> Result<u32>
        where F: Fn(T),
              T: From<Vec<u8>>
    {
        let mut i = 0;
        loop {
            match try!(self.pop()) {
                None => return Ok(i),
                Some(message) => {
                    work_fn(message);
                    i += 1;
                }
            }
        }
    }

    fn wait_for_next_notification(&self) -> Result<Option<Notification>> {
        Ok(try!(self.handle_notification(self.notifications.blocking_iter())))
    }

    fn handle_notification<N>(&self, mut n: N) -> Result<Option<Notification>>
        where N: Iterator<Item = postgres::Result<Notification>>
    {
        match n.next() {
            None => {
                debug!("No notifications remaining for {}.{}", self.bus, self.name);
                Ok(None)
            }
            Some(Err(e)) => {
                error!("Failed to get notification from {}.{}: {}",
                       self.bus,
                       self.name,
                       e);
                Err(Error::ReceiveNotification(e))
            }
            Some(Ok(n)) => {
                debug!("Received push notification from {}.{}: pid={}, payload={}",
                       self.bus,
                       self.name,
                       n.pid,
                       n.payload);
                Ok(Some(n))
            }
        }
    }

    /// Returns an iterator over pending messages. Ends when the queue is empty.
    pub fn messages(&'a self) -> MessageIter<'a, NextMessagePending, T> {
        MessageIter::new(self, NextMessagePending {})
    }

    /// Returns an iterator over messages that blocks until a message is received if none are pending.
    /// This function never returns.
    pub fn messages_blocking(&'a self) -> MessageIter<'a, NextMessageBlocking, T> {
        MessageIter::new(self, NextMessageBlocking {})
    }
}

fn invalid_name(n: &String) -> bool {
    let re = Regex::new(r"^[A-Za-z][A-Za-z0-9_]*$").unwrap();
    !re.is_match(n)
}
