use std::{fmt::Debug, ops::Deref};

use arcstr::ArcStr;
use fnv::FnvHashMap;
use ontol_parser::U32Span;
use ontol_runtime::DomainIndex;

use crate::topology::DomainUrl;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct SourceId(pub u32);

/// Native source means that it is not possible to point
/// to an ONTOL file, usually because something is built-in.
pub const NATIVE_SOURCE: SourceId = SourceId(0);

#[derive(Clone, Debug)]
pub struct Src {
    pub id: SourceId,
    pub domain_index: DomainIndex,
    pub url: DomainUrl,
}

impl Src {
    pub fn url(&self) -> &DomainUrl {
        &self.url
    }

    pub fn span(&self, span: U32Span) -> SourceSpan {
        SourceSpan {
            source_id: self.id,
            span,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Default)]
pub struct SourceSpan {
    pub source_id: SourceId,
    pub span: U32Span,
}

pub const NO_SPAN: SourceSpan = SourceSpan {
    source_id: NATIVE_SOURCE,
    span: U32Span { start: 0, end: 0 },
};

impl SourceSpan {
    pub fn is_native(&self) -> bool {
        self.source_id == NATIVE_SOURCE
    }
}

impl Debug for SourceSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.span.end - self.span.start;
        write!(f, "src@{:?}[{};{}]", self.source_id.0, self.span.start, len)
    }
}

pub struct SpannedBorrow<'m, T> {
    pub value: &'m T,
    pub span: &'m SourceSpan,
}

impl<'m, T> Clone for SpannedBorrow<'m, T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value,
            span: self.span,
        }
    }
}

impl<'m, T> Deref for SpannedBorrow<'m, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<'m, T: Debug> Debug for SpannedBorrow<'m, T> {
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

/// A structure where compiled source code can be temporarily stored.
/// This is useful for error diagnostics.
/// The compiler does not hold an instance of this.
#[derive(Default)]
pub struct SourceCodeRegistry {
    pub registry: FnvHashMap<SourceId, ArcStr>,
}

/// Sources currently being compiled
#[derive(Clone)]
pub struct Sources {
    next_source_id: SourceId,
    domain_index: DomainIndex,
    sources: FnvHashMap<SourceId, Src>,
}

impl Default for Sources {
    fn default() -> Self {
        Self {
            next_source_id: SourceId(1),
            domain_index: DomainIndex::ontol(),
            sources: Default::default(),
        }
    }
}

impl Sources {
    /// Need this while the architecture is kind of broken
    pub fn source_id_for_domain(&self, domain_index: DomainIndex) -> Option<SourceId> {
        self.sources.iter().find_map(|(_, src)| {
            if src.domain_index == domain_index {
                Some(src.id)
            } else {
                None
            }
        })
    }

    pub fn find_source_by_domain_index(&self, domain_index: DomainIndex) -> Option<&Src> {
        self.sources
            .iter()
            .find(|(_, src)| src.domain_index == domain_index)
            .map(|(_, src)| src)
    }

    pub fn get_source(&self, id: SourceId) -> Option<Src> {
        self.sources.get(&id).cloned()
    }

    pub fn add_source(&mut self, domain_index: DomainIndex, url: DomainUrl) -> Src {
        let id = self.next_source_id;
        self.next_source_id.0 += 1;
        self.domain_index = domain_index;
        let src = Src {
            id,
            domain_index,
            url,
        };
        self.sources.insert(id, src.clone());
        src
    }
}
