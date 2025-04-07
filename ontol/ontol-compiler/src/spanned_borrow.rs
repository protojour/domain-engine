use std::{fmt::Debug, ops::Deref};

use ontol_parser::source::SourceSpan;

pub struct SpannedBorrow<'m, T> {
    pub value: &'m T,
    pub span: &'m SourceSpan,
}

impl<T> Clone for SpannedBorrow<'_, T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            span: self.span,
        }
    }
}

impl<T> Deref for SpannedBorrow<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T: Debug> Debug for SpannedBorrow<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl<'m, T> SpannedBorrow<'m, T> {
    pub fn filter<U>(self, f: impl Fn(&'m T) -> Option<&'m U>) -> Option<SpannedBorrow<'m, U>> {
        f(self.value).map(|u| SpannedBorrow {
            value: u,
            span: self.span,
        })
    }
}
