use crate::collections::bloom_filter::BloomFilter;
use crate::model::Row;
pub struct Filter {
    filter: BloomFilter<Row>,
}

impl Filter {
    pub fn new(filter: BloomFilter<Row>) -> Self {
        Self { filter }
    }
    pub fn contains(&self, row: &Row) -> bool {
        self.filter.contains(row)
    }
}
