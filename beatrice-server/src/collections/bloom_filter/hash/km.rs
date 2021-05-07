use super::{Hashers, Hashes};
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
};

/// A logical set of hash functions derived from two inner hash functions
/// with Kirsch-Mitzenmacher Optimization.
#[derive(Clone)]
pub struct KMHashers<B1, B2>
where
    B1: BuildHasher,
    B2: BuildHasher,
{
    bh1: B1,
    bh2: B2,
    m: u64,
}

impl<B1, B2> KMHashers<B1, B2>
where
    B1: BuildHasher,
    B2: BuildHasher,
{
    pub fn with_build_hashers(m: u64, bh1: B1, bh2: B2) -> Self
    where
        B1: BuildHasher,
        B2: BuildHasher,
    {
        Self { m, bh1, bh2 }
    }
}

impl<B1, B2> Hashers for KMHashers<B1, B2>
where
    B1: BuildHasher,
    B2: BuildHasher,
{
    type H = KMHashes;

    fn hash<Q: Hash, V: Borrow<Q>>(&self, value: V) -> KMHashes {
        let value = value.borrow();
        let mut h1 = self.bh1.build_hasher();
        let mut h2 = self.bh2.build_hasher();
        value.hash(&mut h1);
        value.hash(&mut h2);

        KMHashes {
            x1: h1.finish() % self.m,
            x2: h2.finish() % self.m,
            m: self.m,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KMHashes {
    x1: u64,
    x2: u64,
    m: u64,
}

impl Hashes for KMHashes {
    fn get(&self, i: u64) -> Option<u64> {
        Some((self.x1 + i * self.x2) % self.m)
    }
}
