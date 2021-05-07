use self::hash::{km::KMHashers, Hashers, Hashes};
use bitvec::{bitvec, vec::BitVec};
use rustc_hash::FxHasher;
use std::{
    borrow::Borrow,
    f64::consts::LN_2,
    hash::{BuildHasherDefault, Hash},
    marker::PhantomData,
};

pub mod hash;

type FxBuildHasher = BuildHasherDefault<FxHasher>;

#[derive(Clone)]
pub struct BloomFilter<T, H = KMHashers<FxBuildHasher, FxBuildHasher>> {
    /// number of hash functions
    k: usize,
    bits: BitVec,
    hashers: H,
    _t: PhantomData<T>,
}

impl<T> BloomFilter<T>
where
    T: Hash,
{
    /// Create a new bloom-filter where
    /// - `n`: number of items
    /// - `m`: number of bits
    pub fn new(n: u64, m: u64) -> Self {
        Self::with_hashers(
            n,
            m,
            KMHashers::with_build_hashers(
                m,
                BuildHasherDefault::<FxHasher>::default(),
                BuildHasherDefault::<FxHasher>::default(),
            ),
        )
    }
}

impl<T, H> BloomFilter<T, H>
where
    T: Hash,
    H: Hashers,
{
    pub fn with_hashers(n: u64, m: u64, hashers: H) -> Self {
        let k = ((m as f64) / (n as f64) * LN_2) as usize;
        Self {
            k,
            bits: bitvec![0; m as usize],
            hashers,
            _t: PhantomData,
        }
    }

    pub fn insert<V>(&mut self, value: V)
    where
        V: Borrow<T>,
    {
        let hashes = self.hashers.hash(value);
        for h in hashes.iter().take(self.k) {
            self.bits.set(h as usize, true);
        }
    }

    pub fn contains<V>(&self, value: V) -> bool
    where
        V: Borrow<T>,
    {
        let hashes = self.hashers.hash(value);
        hashes.iter().take(self.k).all(|h| self.bits[h as usize])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn build() -> BloomFilter<u64, impl Hashers> {
        let n = 2048;
        let m = 30000;
        BloomFilter::with_hashers(
            n,
            m,
            KMHashers::with_build_hashers(
                m,
                BuildHasherDefault::<FxHasher>::default(),
                BuildHasherDefault::<FxHasher>::default(),
            ),
        )
    }

    #[test]
    fn empty() {
        let filter = build();
        assert!(!filter.contains(&10));
    }

    #[test]
    fn same() {
        let mut filter = build();
        filter.insert(10);
        assert!(filter.contains(&10));
    }
}
