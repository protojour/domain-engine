use thin_vec::ThinVec;

use crate::value::Attribute;

#[derive(Clone, Debug)]
pub struct Sequence {
    /// The attributes of this sequence
    pub attrs: ThinVec<Attribute>,
    /// The subsequence information, if any.
    /// If this is None, the sequence is considered complete.
    pub sub_seq: Option<Box<SubSequence>>,
}

impl Sequence {
    /// Create a new sequence that is not a subsequence.
    pub fn new(attrs: impl IntoIterator<Item = Attribute>) -> Self {
        Self {
            attrs: attrs.into_iter().collect(),
            sub_seq: None,
        }
    }

    pub fn new_sub(attrs: impl IntoIterator<Item = Attribute>, sub_seq: SubSequence) -> Self {
        Self {
            attrs: attrs.into_iter().collect(),
            sub_seq: Some(Box::new(sub_seq)),
        }
    }

    pub fn new_with_capacity(cap: usize) -> Self {
        Self {
            attrs: ThinVec::with_capacity(cap),
            sub_seq: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SubSequence {
    /// The cursor of the _last element_ in the sub sequence
    pub end_cursor: Option<Cursor>,
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

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Cursor {
    Offset(usize),
    Custom(Box<[u8]>),
}
