//! Built-in string message.

use std::fmt;

/// Message containing a utf8 String
pub struct StringMessage {
    body: String,
}

impl StringMessage {
    /// Construct a new StringMessage from `s`.
    pub fn new<S>(s: S) -> Self
        where S: Into<String>
    {
        StringMessage { body: s.into() }
    }

    /// Returns a reference to the inner `String`.
    pub fn body(&self) -> &String {
        &self.body
    }
}

impl fmt::Display for StringMessage {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.body)
    }
}

impl Into<Vec<u8>> for StringMessage {
    fn into(self) -> Vec<u8> {
        self.body.into()
    }
}

impl From<Vec<u8>> for StringMessage {
    fn from(v: Vec<u8>) -> StringMessage {
        StringMessage { body: String::from_utf8(v).unwrap() }
    }
}
