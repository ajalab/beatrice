mod key;
mod value;

pub use self::{key::Key, value::Value};
use bytes::Bytes;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Row(Bytes);

impl Row {
    pub fn new(row: Bytes) -> Self {
        Self(row)
    }

    pub fn get(&self) -> &Bytes {
        &self.0
    }
}
