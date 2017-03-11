//! Built-in message types.
use std::string::FromUtf8Error;

pub trait FromMessageBody<E> {
    fn from_message_body(m: Message) -> Result<Self, E> where Self: Sized;
}

pub trait ToMessageBody<E> {
    fn to_message_body(self) -> Result<Vec<u8>, E>;
}

/// Raw message format
pub struct Message {
    body: Vec<u8>,
}

impl Message {
    /// Construct a new message using body
    pub fn new(body: Vec<u8>) -> Self {
        Message { body: body }
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

impl ToMessageBody<FromUtf8Error> for String {
    fn to_message_body(self) -> Result<Vec<u8>, FromUtf8Error> {
        Ok(self.into())
    }
}

impl<'a> ToMessageBody<FromUtf8Error> for &'a str {
    fn to_message_body(self) -> Result<Vec<u8>, FromUtf8Error> {
        Ok(self.into())
    }
}

impl FromMessageBody<FromUtf8Error> for String {
    fn from_message_body(m: Message) -> Result<Self, FromUtf8Error>
        where Self: Sized
    {
        String::from_utf8(m.to_body())
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
