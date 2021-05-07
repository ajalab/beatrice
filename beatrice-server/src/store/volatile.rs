use super::{
    sstable::{SSTable, SSTableBuilder},
    stat::Stat,
};
use crate::{
    collections::skip_list::SkipListMap,
    model::{Key, Row, Value},
};

fn log2(x: usize) -> usize {
    ((std::mem::size_of::<usize>() * 8) as usize) - (x.leading_zeros() as usize) - 1
}

pub struct VolatileStore {
    level: usize,
    map: SkipListMap<Key, Value>,
    stat: Stat,
}

impl VolatileStore {
    pub fn new(n: usize) -> Self {
        let level = log2(n) + 1;
        Self {
            level,
            map: SkipListMap::new(level),
            stat: Stat::default(),
        }
    }

    pub fn get_latest(&self, row: &Row) -> Option<(&Key, &Value)> {
        let key = Key::new(row.clone(), u64::max_value());
        let kv = self.map.get_smallest_key_value(&key);

        kv.and_then(|(k, v)| {
            if k.row().get() == row.get() {
                Some((k, v))
            } else {
                None
            }
        })
    }

    pub fn insert(&mut self, row: Row, timestamp: u64, value: Value) {
        let key = Key::new(row, timestamp);
        let old_value = self.map.insert(key.clone(), value.clone());
        self.stat.insert(&key, &value, old_value.as_ref());
    }

    pub fn flush(&self) -> SSTable {
        let builder = SSTableBuilder::new(&self.stat, 0.001);
        builder.load(self.map.iter().map(|(k, v)| (k.clone(), v.clone())))
    }

    pub fn clear(&mut self) {
        self.map = SkipListMap::new(self.level);
        self.stat = Stat::default();
    }
}
