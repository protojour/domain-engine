use std::{
    collections::hash_map::Entry,
    fmt::Display,
    hash::{BuildHasher, Hasher},
    ops::Index,
};

use ahash::HashMapExt;
use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};
use thin_vec::ThinVec;

use crate::equality::{OntolEquals, OntolHash};

/// This type represents all ONTOL sequences.
///
/// Both insertion-ordered sets and lists are sequences.
#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct Sequence<T> {
    /// The attributes of this sequence
    pub(crate) elements: ThinVec<T>,
    /// The subsequence information, if any.
    /// If this is None, the sequence is considered complete.
    pub(crate) sub_seq: Option<Box<SubSequence>>,
}

impl<T> Default for Sequence<T> {
    fn default() -> Self {
        Self {
            elements: Default::default(),
            sub_seq: None,
        }
    }
}

impl<T> Sequence<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            elements: ThinVec::with_capacity(cap),
            sub_seq: None,
        }
    }

    pub fn with_sub(self, sub: SubSequence) -> Self {
        Self {
            elements: self.elements,
            sub_seq: Some(Box::new(sub)),
        }
    }

    pub fn push(&mut self, element: T) {
        self.elements.push(element);
    }

    pub fn extend(&mut self, iter: impl IntoIterator<Item = T>) {
        self.elements.extend(iter);
    }

    pub fn elements(&self) -> &[T] {
        &self.elements
    }

    pub fn elements_mut(&mut self) -> &mut [T] {
        &mut self.elements
    }

    pub fn into_elements(self) -> ThinVec<T> {
        self.elements
    }

    pub fn into_first(self) -> Option<T> {
        self.elements.into_iter().next()
    }

    pub fn sub(&self) -> Option<&SubSequence> {
        self.sub_seq.as_deref()
    }

    pub fn clone_sub(&self) -> Option<Box<SubSequence>> {
        self.sub_seq.clone()
    }

    pub fn split(self) -> (ThinVec<T>, Option<Box<SubSequence>>) {
        (self.elements, self.sub_seq)
    }
}

/// Create a new sequence that is not a subsequence.
impl<T> FromIterator<T> for Sequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            elements: iter.into_iter().collect(),
            sub_seq: None,
        }
    }
}

impl<T> From<ThinVec<T>> for Sequence<T> {
    fn from(value: ThinVec<T>) -> Self {
        Self {
            elements: value,
            sub_seq: None,
        }
    }
}

#[derive(Clone, Serialize, PartialEq, Eq, PartialOrd, Hash, Deserialize, Debug)]
pub struct SubSequence {
    /// The cursor of the _last element_ in the sub sequence
    pub end_cursor: Option<Box<[u8]>>,
    /// Are there more items in the sequence _following_ the concrete subsequence?
    pub has_next: bool,
    /// Total number of elements in the sequence
    pub total_len: Option<usize>,
}

impl SubSequence {
    pub fn total_len(&self) -> Option<usize> {
        self.total_len
    }
}

pub trait WithCapacity {
    fn with_capacity(cap: usize) -> Self;
}

pub trait SequenceBuilder<T>: Index<usize, Output = T> {
    fn try_push(&mut self, item: T) -> Result<(), DuplicateError<T>>;
    fn cur_len(&self) -> usize;

    fn build(self) -> Sequence<T>;
}

pub struct ListBuilder<T> {
    elements: ThinVec<T>,
}

impl<T> WithCapacity for ListBuilder<T> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            elements: ThinVec::with_capacity(cap),
        }
    }
}

impl<T> SequenceBuilder<T> for ListBuilder<T> {
    fn try_push(&mut self, element: T) -> Result<(), DuplicateError<T>> {
        self.elements.push(element);
        Ok(())
    }

    fn cur_len(&self) -> usize {
        self.elements.len()
    }

    fn build(self) -> Sequence<T> {
        Sequence {
            elements: self.elements,
            sub_seq: None,
        }
    }
}

impl<T> Index<usize> for ListBuilder<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.elements[index]
    }
}

/// Builder for insertion-ordered sets of attributes
///
/// Duplicates will not be appended.
pub struct IndexSetBuilder<T> {
    /// Length cached outside the ThinVec
    elements: ThinVec<T>,
    /// Length cached outside the ThinVec
    len: usize,
    /// Hash buckets, the values are IndexChain which stores indexes into the `attrs` vector.
    /// The reason hashing is used is that using [OntolHash] is much cheaper
    /// to call than comparing [Value]s.
    hash_buckets: FnvHashMap<u64, IndexChain>,
    /// A unique random seed for the hasher used in this builder
    hash_builder: ahash::RandomState,
}

impl<T> WithCapacity for IndexSetBuilder<T> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            elements: ThinVec::with_capacity(cap),
            len: 0,
            hash_buckets: FnvHashMap::with_capacity(cap),
            hash_builder: Default::default(),
        }
    }
}

impl<T> Default for IndexSetBuilder<T> {
    fn default() -> Self {
        Self {
            elements: Default::default(),
            len: 0,
            hash_buckets: Default::default(),
            hash_builder: Default::default(),
        }
    }
}

impl<T> SequenceBuilder<T> for IndexSetBuilder<T>
where
    T: OntolEquals + OntolHash,
{
    fn try_push(&mut self, element: T) -> Result<(), DuplicateError<T>> {
        let hash = {
            let mut hasher = self.hash_builder.build_hasher();
            element.ontol_hash(&mut hasher, &self.hash_builder);
            hasher.finish()
        };

        match self.hash_buckets.entry(hash) {
            // If the bucket is vacant, store IndexChain with a single index.
            // This does not allocate additional memory in excess of the hashmap itself
            Entry::Vacant(vacant) => {
                vacant.insert(IndexChain::new(self.len));
            }
            // If the bucket is occupied, compare the value
            // to the other values with the same hash
            Entry::Occupied(mut occupied) => {
                {
                    let mut next_chain = occupied.get();

                    // Check all the indexes in the same hash bucket
                    loop {
                        let other_element = self.elements.get(next_chain.index).unwrap();

                        if element.ontol_equals(other_element) {
                            return Err(DuplicateError {
                                element,
                                index: self.len,
                                equals_index: next_chain.index,
                            });
                        }

                        match next_chain.next() {
                            Some(chain) => next_chain = chain,
                            None => break,
                        }
                    }
                }

                // Nothing was equal, there was just a hash collision.
                // Insert the new index at the front of the chain.
                occupied.get_mut().insert(self.len);
            }
        }

        self.elements.push(element);
        self.len += 1;
        Ok(())
    }

    fn cur_len(&self) -> usize {
        self.elements.len()
    }

    fn build(self) -> Sequence<T> {
        Sequence {
            elements: self.elements,
            sub_seq: None,
        }
    }
}

impl<T> Index<usize> for IndexSetBuilder<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.elements[index]
    }
}

/// Error saying that a duplicate has been found in the [IndexSetBuilder]
pub struct DuplicateError<T> {
    pub element: T,
    pub index: usize,
    pub equals_index: usize,
}

impl<T> Display for DuplicateError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid index-set: attribute[{}] equals attribute[{}]",
            self.index, self.equals_index
        )
    }
}

/// A linked list of indexes
struct IndexChain {
    index: usize,
    next: Option<Box<IndexChain>>,
}

impl IndexChain {
    fn new(index: usize) -> Self {
        Self { index, next: None }
    }

    fn insert(&mut self, index: usize) {
        let next_next = self.next.take();
        let next = Box::new(IndexChain {
            index: self.index,
            next: next_next,
        });
        self.index = index;
        self.next = Some(next);
    }

    fn next(&self) -> Option<&Self> {
        self.next.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use ontol_core::tag::{DomainIndex, TagFlags};
    use tracing::debug;

    use crate::{
        DefId, DefPropTag, PropId,
        attr::Attr,
        value::{Value, ValueTag},
    };

    use super::*;

    #[test]
    fn dedup_typed_unit() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        assert!(builder.try_push(Value::unit().into()).is_ok());
        assert!(builder.try_push(Value::unit().into()).is_err());

        assert!(builder.try_push(Value::Unit(def(10000)).into()).is_ok());
        assert!(builder.try_push(Value::Unit(def(10000)).into()).is_err());
    }

    #[test]
    fn dedup_text() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        assert!(builder.try_push(text("a").into()).is_ok());
        assert!(builder.try_push(text("a").into()).is_err());
        assert!(builder.try_push(text("b").into()).is_ok());
        assert!(builder.try_push(text("b").into()).is_err());
    }

    #[test]
    fn dedup_i64() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        assert!(builder.try_push(Value::I64(42, def(2)).into()).is_ok());
        assert!(builder.try_push(Value::I64(42, def(2)).into()).is_err());
        assert!(builder.try_push(Value::I64(42, def(3)).into()).is_ok());
    }

    #[test]
    fn dedup_f64_zero() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        assert!(builder.try_push(Value::F64(0.0, def(4)).into()).is_ok());
        assert!(builder.try_push(Value::F64(-0.0, def(4)).into()).is_err());
    }

    // Not sure about the behaviour of NaN, whether they should be considered
    // OntolEquals or not
    #[test]
    fn dedup_f64_nan_can_duplicate() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        let nan = Value::F64(f64::NAN, def(3));

        // NaN can be duplicated
        assert!(builder.try_push(nan.clone().into()).is_ok());
        assert!(builder.try_push(nan.clone().into()).is_ok());
    }

    #[test]
    fn dedup_struct_empty() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        assert!(builder.try_push(struct_value([]).into()).is_ok());
        assert!(builder.try_push(struct_value([]).into()).is_err());
    }

    #[test]
    fn dedup_big_structs_different_iteration_order() {
        let mut builder: IndexSetBuilder<Attr> = Default::default();

        // structs with different insertion order
        assert!(
            builder
                .try_push(struct_value([(0, text("a")), (1, text("b"))]).into())
                .is_ok()
        );
        assert!(
            builder
                .try_push(struct_value([(1, text("b")), (0, text("a"))]).into())
                .is_err()
        );

        let (struct0, struct1) = gen_equal_structs_with_different_iteration_order();
        assert!(struct0.ontol_equals(&struct1));

        // These should have the same OntolHash
        assert!(builder.try_push(struct0.into()).is_ok());
        assert!(builder.try_push(struct1.into()).is_err());
    }

    fn def(id: u16) -> ValueTag {
        DefId::new_persistent(DomainIndex::from_u16_and_mask(1337, TagFlags::PKG_MASK), id).into()
    }

    fn text(t: &str) -> Value {
        Value::Text(t.into(), def(1))
    }

    fn struct_value(iter: impl IntoIterator<Item = (u16, Value)>) -> Value {
        let attrs = iter.into_iter().map(|(tag, val)| {
            (
                PropId(
                    DefId::new_persistent(
                        DomainIndex::from_u16_and_mask(42, TagFlags::PKG_MASK),
                        42,
                    ),
                    DefPropTag(tag),
                ),
                Attr::Unit(val),
            )
        });

        Value::Struct(Box::new(FnvHashMap::from_iter(attrs)), def(2))
    }

    /// find an n large enough to trigger FNV hash collision.
    /// This is not fast but it's only a test
    fn gen_equal_structs_with_different_iteration_order() -> (Value, Value) {
        for n in 100..1000 {
            let attributes: Vec<_> = (0..n)
                .map(|i| {
                    let tag = i as u16;
                    let value = Value::I64(i as i64, def(42));

                    (
                        PropId(
                            DefId::new_persistent(
                                DomainIndex::from_u16_and_mask(42, TagFlags::PKG_MASK),
                                42,
                            ),
                            DefPropTag(tag),
                        ),
                        Attr::Unit(value),
                    )
                })
                .collect();

            // generate two maps with keys inserted in opposite order.
            // If there is at least one hash collision, the iteration order of the maps should be different.
            let map0 = FnvHashMap::from_iter(attributes.iter().cloned());
            let map1 = FnvHashMap::from_iter(attributes.iter().rev().cloned());

            // determine different iteration order
            for (k0, k1) in map0.keys().zip(map1.keys()) {
                if k0 != k1 {
                    debug!("Found different iteration order for property maps at n={n}");
                    return (
                        Value::Struct(Box::new(map0), def(1337)),
                        Value::Struct(Box::new(map1), def(1337)),
                    );
                }
            }
        }

        panic!("Found no input that leads to collision");
    }
}
