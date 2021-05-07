use std::num::NonZeroUsize;

pub trait LevelGenerator {
    fn generate(&mut self) -> usize;
}

pub struct RandomLevelGenerator {
    max_level: usize,
    p: f64,
}

impl RandomLevelGenerator {
    pub fn new(max_level: usize, p: f64) -> Self {
        Self { max_level, p }
    }
}

impl LevelGenerator for RandomLevelGenerator {
    fn generate(&mut self) -> usize {
        let mut level = 1;
        while rand::random::<f64>() < self.p && level < self.max_level {
            level += 1;
        }
        level
    }
}

pub struct SkipListMap<K, V, G = RandomLevelGenerator> {
    generator: G,
    forwards: Vec<NonZeroUsize>,
    nodes: Vec<Node<K, V>>,
}

impl<K, V, G> SkipListMap<K, V, G> {
    pub fn with_generator(max_level: usize, generator: G) -> Self {
        SkipListMap {
            generator,
            forwards: Vec::with_capacity(max_level),
            nodes: vec![],
        }
    }

    pub fn level(&self) -> usize {
        self.forwards.len()
    }

    fn node(&self, i: NonZeroUsize) -> &Node<K, V> {
        &self.nodes[i.get() - 1]
    }

    fn node_mut(&mut self, i: NonZeroUsize) -> &mut Node<K, V> {
        &mut self.nodes[i.get() - 1]
    }

    fn register(&mut self, node: Node<K, V>) -> NonZeroUsize {
        self.nodes.push(node);
        unsafe { NonZeroUsize::new_unchecked(self.nodes.len()) }
    }

    fn search_iter<'a>(&'a self, key: &'a K) -> SearchIter<K, V, G> {
        SearchIter::new(self, key)
    }
}

impl<K, V> SkipListMap<K, V, RandomLevelGenerator> {
    pub fn new(max_level: usize) -> Self {
        Self::with_generator(max_level, RandomLevelGenerator::new(max_level, 0.5))
    }
}

impl<K, V, G> SkipListMap<K, V, G>
where
    K: Eq + Ord,
    G: LevelGenerator,
{
    fn next(&self, id: Option<NonZeroUsize>) -> Option<NonZeroUsize> {
        match id {
            Some(id) => self.node(id).forwards[0],
            None => Some(self.forwards[0]),
        }
    }

    fn get_smallest_id(&self, key: &K) -> Option<NonZeroUsize> {
        self.search_iter(&key).last().and_then(|id| self.next(id))
    }

    pub fn get_smallest(&self, key: &K) -> Option<&V> {
        self.get_smallest_id(key).map(|id| &self.node(id).value)
    }

    pub fn get_smallest_key_value(&self, key: &K) -> Option<(&K, &V)> {
        self.get_smallest_id(key).map(|id| {
            let node = self.node(id);
            (&node.key, &node.value)
        })
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.get_smallest_id(key).and_then(|id| {
            let node = self.node(id);
            if &node.key == key {
                Some(&node.value)
            } else {
                None
            }
        })
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let trace = self.search_iter(&key).collect::<Vec<_>>();

        if let Some(next) = trace.last().and_then(|&id| self.next(id)) {
            let node = self.node_mut(next);
            if node.key == key {
                return Some(std::mem::replace(&mut node.value, value));
            }
        }

        let new_level = self.generator.generate();
        let id = self.register(Node::new(key, value, new_level));

        let mut level = new_level;
        if new_level > trace.len() {
            for _ in trace.len()..new_level {
                self.forwards.push(id);
            }
            level = trace.len();
        }
        for (l, i) in trace.into_iter().rev().enumerate().take(level) {
            match i {
                Some(u) => {
                    let n = self.node_mut(u);
                    let i = n.forwards[l];
                    n.forwards[l] = Some(id);
                    self.node_mut(id).forwards[l] = i;
                }
                None => {
                    let i = self.forwards[l];
                    self.forwards[l] = id;
                    self.node_mut(id).forwards[l] = Some(i);
                }
            }
        }

        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        Iter::new(self)
    }
}

#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    forwards: Vec<Option<NonZeroUsize>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, level: usize) -> Self {
        Node {
            key,
            value,
            forwards: vec![None; level],
        }
    }

    fn level(&self) -> usize {
        self.forwards.len()
    }
}

struct SearchIter<'a, K, V, G> {
    list: &'a SkipListMap<K, V, G>,
    key: &'a K,
    id: Option<NonZeroUsize>,
    level: usize,
}

impl<'a, K, V, G> SearchIter<'a, K, V, G> {
    fn new(list: &'a SkipListMap<K, V, G>, key: &'a K) -> Self {
        SearchIter {
            list,
            key,
            id: None,
            level: list.level(),
        }
    }
}

impl<'a, K, V, G> Iterator for SearchIter<'a, K, V, G>
where
    K: Ord,
{
    type Item = Option<NonZeroUsize>;

    fn next(&mut self) -> Option<Self::Item> {
        let level = match self.level {
            0 => return None,
            l => l - 1,
        };

        let mut id = self.id;

        if id.is_none() {
            let i = self.list.forwards[level];
            let node = self.list.node(i);
            if &node.key < self.key {
                id = Some(i);
            }
        }

        if let Some(mut i) = id {
            let mut node = self.list.node(i);
            while let Some(&f) = node.forwards[level].as_ref() {
                let n = self.list.node(f);
                if &n.key < self.key {
                    node = n;
                    i = f;
                } else {
                    break;
                }
            }
            id = Some(i);
        }

        self.level -= 1;
        Some(id)
    }
}

struct Iter<'a, K, V, G> {
    list: &'a SkipListMap<K, V, G>,
    id: Option<NonZeroUsize>,
}

impl<'a, K, V, G> Iter<'a, K, V, G> {
    fn new(list: &'a SkipListMap<K, V, G>) -> Self {
        let id = list.forwards.first().cloned();
        Self { list, id }
    }
}

impl<'a, K, V, G> Iterator for Iter<'a, K, V, G> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        match self.id {
            None => None,
            Some(id) => {
                let node = self.list.node(id);
                self.id = node.forwards.first().cloned().flatten();
                Some((&node.key, &node.value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashMap, rc::Rc};

    use rand::Rng;

    use super::*;

    struct ControllableLevelGenerator {
        next_level: Rc<RefCell<usize>>,
    }

    impl ControllableLevelGenerator {
        fn create() -> (Self, ControllableLevelGeneratorHandle) {
            let next_level = Rc::new(RefCell::new(1));
            let handle = ControllableLevelGeneratorHandle {
                next_level: next_level.clone(),
            };
            let generator = Self { next_level };
            (generator, handle)
        }
    }

    impl LevelGenerator for ControllableLevelGenerator {
        fn generate(&mut self) -> usize {
            *self.next_level.as_ref().borrow()
        }
    }

    struct ControllableLevelGeneratorHandle {
        next_level: Rc<RefCell<usize>>,
    }

    impl ControllableLevelGeneratorHandle {
        fn set_next_level(&self, level: usize) {
            *self.next_level.as_ref().borrow_mut() = level;
        }
    }

    fn init<K, V>(
        max_level: usize,
    ) -> (
        SkipListMap<K, V, ControllableLevelGenerator>,
        ControllableLevelGeneratorHandle,
    ) {
        let (generator, handle) = ControllableLevelGenerator::create();
        let list = SkipListMap::with_generator(max_level, generator);
        (list, handle)
    }

    #[test]
    fn insert_first() {
        let (mut list, handle) = init(5);
        handle.set_next_level(3);
        list.insert(1u8, ());

        assert!(list.insert(1u8, ()).is_some());
    }

    #[test]
    fn insert_second_next_shorter() {
        let (mut list, handle) = init(5);
        handle.set_next_level(3);
        list.insert(10u8, ());
        handle.set_next_level(2);
        list.insert(20u8, ());

        assert!(list.insert(10u8, ()).is_some());
        assert!(list.insert(20u8, ()).is_some());
    }

    #[test]
    fn insert_second_next_taller() {
        let (mut list, handle) = init(5);
        handle.set_next_level(3);
        list.insert(10u8, ());
        handle.set_next_level(5);
        list.insert(30u8, ());
        handle.set_next_level(4);
        list.insert(20u8, ());

        assert!(list.insert(10u8, ()).is_some());
        assert!(list.insert(20u8, ()).is_some());
        assert!(list.insert(30u8, ()).is_some());
    }

    #[test]
    fn insert_second_prev_shorter() {
        let (mut list, handle) = init(5);
        handle.set_next_level(3);
        list.insert(10u8, ());
        handle.set_next_level(2);
        list.insert(5u8, ());

        assert!(list.insert(10u8, ()).is_some());
        assert!(list.insert(5u8, ()).is_some());
    }

    #[test]
    fn insert_second_prev_longer() {
        let (mut list, handle) = init(5);
        handle.set_next_level(3);
        list.insert(10u8, ());
        handle.set_next_level(1);
        list.insert(5u8, ());
        handle.set_next_level(2);
        list.insert(7u8, ());

        assert!(list.insert(5u8, ()).is_some());
        assert!(list.insert(7u8, ()).is_some());
        assert!(list.insert(10u8, ()).is_some());
    }

    #[test]
    fn insert_random() {
        let mut list = SkipListMap::new(16);
        let mut rng = rand::thread_rng();
        let mut map = HashMap::new();
        for _ in 0..1000 {
            let key = rng.gen::<u64>();
            let value = rng.gen::<u64>();
            list.insert(key, value);
            map.insert(key, value);
        }

        let mut expected = map.into_iter().collect::<Vec<(u64, u64)>>();
        expected.sort();

        for (k, v) in expected.iter() {
            assert_eq!(list.get(k).unwrap(), v);
        }

        let actual = list
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect::<Vec<(u64, u64)>>();

        assert_eq!(expected, actual);
    }
}
