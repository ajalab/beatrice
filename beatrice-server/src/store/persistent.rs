use super::sstable::{Data, Filter, Index, SSTable};
use crate::model::{Key, Row, Value};
use std::collections::HashMap;

type TableId = u64;

#[derive(Default)]
pub struct PersistentStore {
    last_table_id: TableId,

    // TODO: implement better buffer pool
    data_pool: HashMap<TableId, Data>,
    index_pool: HashMap<TableId, Index>,
    filter_pool: HashMap<TableId, Filter>,
}

impl PersistentStore {
    pub fn add(&mut self, sstable: SSTable, cache: bool) {
        let table_id = self.last_table_id + 1;
        let SSTable {
            data,
            index,
            filter,
        } = sstable;
        if cache {
            self.data_pool.insert(table_id, data);
            self.index_pool.insert(table_id, index);
            self.filter_pool.insert(table_id, filter);
        }
    }

    pub fn get_latest(&self, row: &Row) -> Option<(Key, Value)> {
        // (TableId, (Key, Offset))
        let mut res = None;
        // Assuming that filters of all sstables are loaded
        for (id, filter) in self.filter_pool.iter() {
            if filter.contains(row) {
                // TODO: Handle cases when index is dropped from the buffer pool
                let index = self
                    .index_pool
                    .get(id)
                    .expect("assuming that indices of all sstables are loaded");
                match (res, index.get_latest(row)) {
                    (None, Some(p)) => {
                        res = Some((id, p));
                    }
                    (Some((_, (key, _))), Some((k, o))) => {
                        if key.timestamp() <= k.timestamp() {
                            res = Some((id, (k, o)));
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some((id, (_, offset))) = res {
            // TODO: Handle cases when data is dropped from the buffer pool
            let data = self
                .data_pool
                .get(id)
                .expect("assuming that data of all sstables are loaded");
            Some(data.get(offset))
        } else {
            None
        }
    }
}
