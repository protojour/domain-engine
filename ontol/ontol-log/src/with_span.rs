use std::{borrow::Borrow, fmt::Debug, hash::Hash};

use ontol_core::span::U32Span;
use serde::{Deserialize, Serialize};

/// Spanned value, where the span does not affect equality/hash
#[derive(Clone, Copy)]
pub struct WithSpan<T>(pub T, pub U32Span);

impl<T> WithSpan<T> {
    pub fn value(self) -> T {
        self.0
    }

    pub fn span(&self) -> U32Span {
        self.1
    }
}

impl<T: PartialEq> PartialEq<WithSpan<T>> for WithSpan<T> {
    fn eq(&self, other: &WithSpan<T>) -> bool {
        self.0 == other.0
    }
}

impl<T: Eq> Eq for WithSpan<T> {}

impl<T: PartialOrd> PartialOrd<WithSpan<T>> for WithSpan<T> {
    fn partial_cmp(&self, other: &WithSpan<T>) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<T: Ord> Ord for WithSpan<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T: Hash> Hash for WithSpan<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T: Debug> Debug for WithSpan<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> Borrow<T> for WithSpan<T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

pub trait SetSpan: Sized {
    fn set_span(self, span: U32Span) -> WithSpan<Self>;
    fn no_span(self) -> WithSpan<Self>;
}

impl<T> SetSpan for T {
    #[inline]
    fn set_span(self, span: U32Span) -> WithSpan<Self> {
        WithSpan(self, span)
    }

    #[inline]
    fn no_span(self) -> WithSpan<Self> {
        WithSpan(self, Default::default())
    }
}

impl<T: Serialize> Serialize for WithSpan<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for WithSpan<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(WithSpan(T::deserialize(deserializer)?, Default::default()))
    }
}
