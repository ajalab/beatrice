use crate::model::{Key, Row};

#[derive(Clone, Default)]
pub struct Index {
    index: Vec<(Key, usize)>,
}

impl Index {
    pub fn new(index: Vec<(Key, usize)>) -> Self {
        // keys are sorted
        debug_assert!(index.len() > 0);

        Self { index }
    }

    pub fn get_latest(&self, row: &Row) -> Option<(&Key, usize)> {
        let key = Key::new(row.clone(), u64::max_value());

        let mut left = usize::max_value();
        let mut right = self.index.len();

        while right.wrapping_sub(left) > 1 {
            let m = left.wrapping_add(right.wrapping_sub(left) / 2);
            let k = &self.index[m].0;

            if &key <= k {
                right = m;
            } else {
                left = m;
            }
        }

        if right < self.index.len() {
            let (ref k, offset) = self.index[right];
            if k.row() == row {
                Some((k, offset))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    #[test]
    fn test_get_latest() {
        let idx = vec![
            (Key::new(Row::new(Bytes::from("b")), 100), 0),
            (Key::new(Row::new(Bytes::from("b")), 99), 1),
            (Key::new(Row::new(Bytes::from("c")), 200), 2),
            (Key::new(Row::new(Bytes::from("d")), 100), 3),
            (Key::new(Row::new(Bytes::from("d")), 99), 4),
            (Key::new(Row::new(Bytes::from("d")), 98), 5),
        ];
        let index = Index::new(idx.clone());

        let (k, o) = index.get_latest(&Row::new(Bytes::from("a"))).unwrap();
        assert_eq!(&idx[0].0, k);
        assert_eq!(idx[0].1, o);

        let (k, o) = index.get_latest(&Row::new(Bytes::from("b"))).unwrap();
        assert_eq!(&idx[0].0, k);
        assert_eq!(idx[0].1, o);

        let (k, o) = index.get_latest(&Row::new(Bytes::from("c"))).unwrap();
        assert_eq!(&idx[2].0, k);
        assert_eq!(idx[2].1, o);

        let (k, o) = index.get_latest(&Row::new(Bytes::from("d"))).unwrap();
        assert_eq!(&idx[3].0, k);
        assert_eq!(idx[3].1, o);

        assert!(index.get_latest(&Row::new(Bytes::from("e"))).is_none());
    }
}
