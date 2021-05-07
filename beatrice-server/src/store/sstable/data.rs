use crate::model::{Key, Value};
use bytes::{Bytes, BytesMut};

#[derive(Clone, Default)]
pub struct Data {
    data: Bytes,
}

impl Data {
    pub fn get(&self, offset: usize) -> (Key, Value) {
        let mut buf = self.data.slice(offset..);
        let key = Key::read_from(&mut buf);
        let value = Value::read_from(&mut buf);
        (key, value)
    }
}

pub struct DataBuilder {
    data: BytesMut,
}

impl DataBuilder {
    pub fn new(size: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(size),
        }
    }

    pub fn append(&mut self, key: Key, value: Value) -> usize {
        let key_size = key.write_to(&mut self.data);
        let value_size = value.write_to(&mut self.data);
        key_size + value_size
    }

    pub fn build(self) -> Data {
        Data {
            data: self.data.freeze(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Row;

    #[test]
    fn test() {
        let kvs = vec![
            (
                Key::new(Row::new(Bytes::from("k1")), 1),
                Value::Val(Bytes::from("v1")),
            ),
            (
                Key::new(Row::new(Bytes::from("k10")), 10),
                Value::Val(Bytes::from("v10")),
            ),
            (Key::new(Row::new(Bytes::from("k100")), 100), Value::Del),
            (
                Key::new(Row::new(Bytes::from("k100")), 100),
                Value::Val(Bytes::from("v100")),
            ),
        ];
        let mut builder = DataBuilder::new(kvs.len());
        let mut offsets = Vec::with_capacity(kvs.len());
        let mut offset = 0;
        for (key, value) in kvs.iter().cloned() {
            let len = builder.append(key, value);
            offsets.push(offset);
            offset += len;
        }
        let data = builder.build();

        for ((key, value), offset) in kvs.into_iter().zip(offsets.into_iter()) {
            let (k, v) = data.get(offset);
            assert_eq!(key, k);
            assert_eq!(value, v);
        }
    }
}
