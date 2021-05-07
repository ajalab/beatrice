use std::{borrow::Borrow, hash::Hash};

pub trait Hashers {
    type H: Hashes;
    fn hash<Q: Hash, V: Borrow<Q>>(&self, value: V) -> Self::H;
}

pub trait Hashes {
    fn get(&self, i: u64) -> Option<u64>;

    fn iter(&self) -> Iter<Self> {
        Iter {
            i: 0,
            hashes: &self,
        }
    }
}

pub struct Iter<'a, H: ?Sized> {
    i: u64,
    hashes: &'a H,
}

impl<'a, H> Iterator for Iter<'a, H>
where
    H: Hashes,
{
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        let h = self.hashes.get(self.i);
        self.i += 1;
        h
    }
}

pub mod km;
