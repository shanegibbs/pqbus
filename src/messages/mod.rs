//! Built-in message types.

pub trait FromMessageBody<T, E> {
    fn from_message(m: Message) -> Result<T, E>;
}

pub trait ToMessageBody<T, E> {
    fn to_message(t: T) -> Result<Vec<u8>, E>;
}

/// Raw message format
pub struct Message {
    body: Vec<u8>
}

impl Message {
    /// Construct a new message using body
    pub fn new(body: Vec<u8>) -> Self {
        Message {
            body: body
        }
    }
    /// Get reference to body
    pub fn body(&self) -> &[u8] {
        &self.body
    }
    /// Consumes the message returning it's body
    pub fn to_body(self) -> Vec<u8> {
        self.body
    }
}

impl From<Message> for String {
    fn from(m: Message) -> String {
        let s = String::from_utf8(m.to_body()).unwrap();
        s
    }
}

impl From<String> for Message {
    fn from(s: String) -> Message {
        Message::new(s.into())
    }
}
