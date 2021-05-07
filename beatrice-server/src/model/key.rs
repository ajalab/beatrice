use super::Row;
use bytes::{Buf, BufMut, Bytes};
use std::{cmp::Ordering, mem};

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct Key {
    row: Row,
    timestamp: u64,
}

impl Key {
    pub fn new(row: Row, timestamp: u64) -> Self {
        Self { row, timestamp }
    }

    pub fn row(&self) -> &Row {
        &self.row
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn size(&self) -> usize {
        mem::size_of_val(&self.timestamp) + mem::size_of::<u64>() + self.row.0.len()
    }

    pub fn read_from(buf: &mut Bytes) -> Self {
        let timestamp = buf.get_u64_le();
        let len = buf.get_u64_le() as usize;
        let row = Row::new(buf.slice(..len));
        buf.advance(len);
        Key { row, timestamp }
    }

    pub fn write_to<T: BufMut>(self, buf: &mut T) -> usize {
        let size = self.size();
        buf.put_u64_le(self.timestamp);
        buf.put_u64_le(self.row.0.len() as u64);
        buf.put(self.row.0);
        size
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row
            .cmp(&other.row)
            .then_with(|| self.timestamp.cmp(&other.timestamp).reverse())
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    #[test]
    fn test_read_write() {
        let mut buf = BytesMut::new();
        let key = Key::new(Row::new(Bytes::from("this is a test row")), 100);
        key.clone().write_to(&mut buf);

        let mut buf = buf.freeze();
        let k = Key::read_from(&mut buf);

        assert_eq!(key, k);
        assert_eq!(buf.remaining(), 0);
    }
}
