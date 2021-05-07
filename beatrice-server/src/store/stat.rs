use crate::model::{Key, Value};

#[derive(Default)]
pub struct Stat {
    len: usize,
    /// Total key size in bytes when flushed
    key_size: usize,
    /// Total value size in bytes when flushed
    value_size: usize,
}

impl Stat {
    pub fn insert(&mut self, key: &Key, value: &Value, old_value: Option<&Value>) {
        match old_value {
            None => {
                self.len += 1;
                self.key_size += key.size();
                self.value_size += value.size();
            }
            Some(old_value) => {
                self.value_size = (self.value_size + value.size()) - old_value.size();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn key_size(&self) -> usize {
        self.key_size
    }

    pub fn value_size(&self) -> usize {
        self.value_size
    }
}
