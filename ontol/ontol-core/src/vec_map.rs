use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

pub trait VecMapKey {
    fn index(&self) -> usize;
}

/// A dense index map, very suitable for consecutive keys.
#[derive(Clone, Serialize, Deserialize)]
pub struct VecMap<K, T> {
    elements: Vec<Option<T>>,
    _key: PhantomData<K>,
}

impl<K, V> VecMap<K, V> {
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.iter().count() == 0
    }

    pub fn get(&self, key: &K) -> Option<&V>
    where
        K: VecMapKey,
    {
        self.elements.get(key.index())?.as_ref()
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V>
    where
        K: VecMapKey,
    {
        self.elements.get_mut(key.index())?.as_mut()
    }

    pub fn remove(&mut self, key: &K) -> Option<V>
    where
        K: VecMapKey,
    {
        self.elements.get_mut(key.index())?.take()
    }

    pub fn iter(&self) -> impl Iterator<Item = (usize, &V)> {
        self.elements
            .iter()
            .enumerate()
            .filter_map(|(idx, opt)| opt.as_ref().map(|element| (idx, element)))
    }

    pub fn insert(&mut self, key: K, value: V)
    where
        K: VecMapKey,
    {
        let index = key.index();
        self.elements
            .resize_with(std::cmp::max(self.elements.len(), index + 1), || None);
        self.elements[index] = Some(value);
    }
}

impl<K, V> Default for VecMap<K, V> {
    fn default() -> Self {
        Self {
            elements: vec![],
            _key: PhantomData,
        }
    }
}
