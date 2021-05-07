mod data;
mod filter;
mod index;

use self::data::DataBuilder;
pub use self::{data::Data, filter::Filter, index::Index};
use super::{compacter::SingleVersionCompacter, stat::Stat};
use crate::{
    collections::bloom_filter::BloomFilter,
    model::{Key, Row, Value},
};
use std::f64::consts::LN_2;

pub struct SSTable {
    pub data: Data,
    pub index: Index,
    pub filter: Filter,
}

pub struct SSTableBuilder {
    max_len: usize,
    len: usize,
    offset: usize,
    data: DataBuilder,
    index: Vec<(Key, usize)>,
    filter: BloomFilter<Row>,
}

impl SSTableBuilder {
    pub fn new(stat: &Stat, p: f64) -> Self {
        let max_len = stat.len();
        debug_assert!(max_len > 0);
        let m = compute_filter_bits(max_len, p);
        let data_size = stat.key_size() + stat.value_size();

        Self {
            max_len,
            len: 0,
            offset: 0,
            data: DataBuilder::new(data_size),
            index: Vec::with_capacity(max_len),
            filter: BloomFilter::new(max_len as u64, m as u64),
        }
    }

    pub fn load<I: IntoIterator<Item = (Key, Value)>>(mut self, iter: I) -> SSTable {
        let mut iter = iter.into_iter();
        let (key, value) = iter.next().unwrap();
        let mut compacter = SingleVersionCompacter::new(key, value);
        for (key, value) in iter {
            if let Some((k, v)) = compacter.compact(key, value) {
                self.append(k, v);
            }
        }
        let (k, v) = compacter.into_key_value();
        self.append(k, v);

        SSTable {
            data: self.data.build(),
            index: Index::new(self.index),
            filter: Filter::new(self.filter),
        }
    }

    fn append(&mut self, key: Key, value: Value) {
        debug_assert!(self.len < self.max_len);

        self.append_filter(&key);
        let size = self.data.append(key.clone(), value.clone());
        self.append_index(key, self.offset);
        self.len += 1;
        self.offset += size;
    }

    fn append_index(&mut self, key: Key, offset: usize) {
        self.index.push((key, offset));
    }

    fn append_filter(&mut self, key: &Key) {
        self.filter.insert(key.row().clone());
    }
}

fn compute_filter_bits(n: usize, p: f64) -> usize {
    // https://hur.st/bloomfilter
    // m = ceil((n * log(p)) / log(1 / pow(2, log(2))))

    ((n as f64 * p.ln()) / (1.0 / (2.0f64.powf(LN_2))).ln()).ceil() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_compute_filter_bits() {
        let cases = vec![((10, 1e-3), 144), ((1000, 1e-4), 19171)];
        for ((n, p), expected) in cases {
            let actual = compute_filter_bits(n, p);
            assert_eq!(expected, actual);
        }
    }
}
