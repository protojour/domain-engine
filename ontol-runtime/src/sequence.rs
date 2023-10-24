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
}

#[derive(Clone, Debug)]
pub struct SubSequence {
    pub next_cursor: Cursor,
    pub total_len: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum Cursor {
    Offset(usize),
    Custom(Box<[u8]>),
}
