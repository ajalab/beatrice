use crate::model::{Key, Value};
use std::mem;

pub struct SingleVersionCompacter {
    key: Key,
    value: Value,
}

impl SingleVersionCompacter {
    pub fn new(key: Key, value: Value) -> Self {
        Self { key, value }
    }

    pub fn compact(&mut self, key: Key, value: Value) -> Option<(Key, Value)> {
        debug_assert!(self.key < key);
        if self.key.row() == key.row() {
            None
        } else {
            Some((
                mem::replace(&mut self.key, key),
                mem::replace(&mut self.value, value),
            ))
        }
    }

    pub fn into_key_value(self) -> (Key, Value) {
        (self.key, self.value)
    }
}
