use std::{fmt::Debug, ops::Range};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct U32Span {
    pub start: u32,
    pub end: u32,
}

impl U32Span {
    pub fn len(&self) -> usize {
        self.end as usize - self.start as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, pos: u32) -> bool {
        self.start <= pos && self.end > pos
    }

    pub fn contains_usize(&self, pos: usize) -> bool {
        self.start as usize <= pos && self.end as usize > pos
    }

    pub fn join(self, other: Self) -> Self {
        Self {
            start: std::cmp::min(self.start, other.start),
            end: std::cmp::max(self.end, other.end),
        }
    }
}

impl Debug for U32Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

impl From<Range<usize>> for U32Span {
    fn from(value: Range<usize>) -> Self {
        Self {
            start: value.start as u32,
            end: value.end as u32,
        }
    }
}

impl From<U32Span> for Range<usize> {
    fn from(value: U32Span) -> Self {
        value.start as usize..value.end as usize
    }
}

pub trait ToUsizeRange {
    fn to_usize_range(&self) -> Range<usize>;
}

impl ToUsizeRange for U32Span {
    fn to_usize_range(&self) -> Range<usize> {
        self.start as usize..self.end as usize
    }
}
