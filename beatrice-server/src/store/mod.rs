mod compacter;
mod persistent;
mod sstable;
mod stat;
mod volatile;

use self::{persistent::PersistentStore, volatile::VolatileStore};
use crate::model::{Key, Row, Value};
use bytes::Bytes;

pub struct Store {
    volatile: VolatileStore,
    persistent: PersistentStore,
}

impl Store {
    pub fn new(n: usize) -> Self {
        Self {
            volatile: VolatileStore::new(n),
            persistent: PersistentStore::default(),
        }
    }

    pub fn get_latest(&self, row: &Row) -> Option<(Key, Bytes)> {
        let volatile = self.volatile.get_latest(row);
        let persistent = self.persistent.get_latest(row);

        let kv = match (volatile, persistent) {
            (Some((k1, v1)), Some((k2, v2))) => {
                if k1.timestamp() > k2.timestamp() {
                    Some((k1.clone(), v1.clone()))
                } else {
                    Some((k2, v2))
                }
            }
            (Some((k1, v1)), None) => Some((k1.clone(), v1.clone())),
            (None, Some(kv2)) => Some(kv2),
            (None, None) => None,
        };
        kv.and_then(|(k, v)| {
            if let Value::Val(v) = v {
                Some((k, v))
            } else {
                None
            }
        })
    }

    pub fn put(&mut self, row: Row, timestamp: u64, val: Bytes) {
        let value = Value::Val(val);
        self.volatile.insert(row, timestamp, value);
    }

    pub fn flush(&mut self, cache: bool) {
        let sstable = self.volatile.flush();
        self.persistent.add(sstable, cache);
        self.volatile.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    pub fn flush() {
        let mut store = Store::new(2048);
        let commands = vec![
            Some((Row::new(Bytes::from("r1")), 1, Bytes::from("v11"))),
            Some((Row::new(Bytes::from("r2")), 2, Bytes::from("v22"))),
            Some((Row::new(Bytes::from("r1")), 3, Bytes::from("v13"))),
            None,
            Some((Row::new(Bytes::from("r2")), 4, Bytes::from("v24"))),
            Some((Row::new(Bytes::from("r3")), 5, Bytes::from("v35"))),
        ];

        for command in commands {
            match command {
                Some((row, timestamp, val)) => {
                    store.put(row, timestamp, val);
                }
                None => store.flush(true),
            }
        }

        let r = Row::new(Bytes::from("r1"));
        let (k, v) = store.get_latest(&r).unwrap();
        assert_eq!(&r, k.row());
        assert_eq!(3, k.timestamp());
        assert_eq!(v, Bytes::from("v13"));

        let r = Row::new(Bytes::from("r2"));
        let (k, v) = store.get_latest(&r).unwrap();
        assert_eq!(&r, k.row());
        assert_eq!(4, k.timestamp());
        assert_eq!(v, Bytes::from("v24"));

        let r = Row::new(Bytes::from("r3"));
        let (k, v) = store.get_latest(&r).unwrap();
        assert_eq!(&r, k.row());
        assert_eq!(5, k.timestamp());
        assert_eq!(v, Bytes::from("v35"));
    }
}
