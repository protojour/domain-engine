use std::{fmt::Debug, sync::Arc};

use fnv::FnvHashMap;
use ontol_core::{span::U32Span, url::DomainUrl, vec_map::VecMapKey};

/// An ONTOL source during a compilation session.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct SourceId(pub u32);

/// Native source means that it is not possible to point
/// to an ONTOL file, usually because something is built-in.
pub const NATIVE_SOURCE: SourceId = SourceId(0);

impl SourceId {
    pub fn span(&self, span: U32Span) -> SourceSpan {
        SourceSpan {
            source_id: *self,
            span,
        }
    }
}

impl VecMapKey for SourceId {
    fn index(&self) -> usize {
        self.0 as usize
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

/// A structure where compiled source code can be temporarily stored.
/// This is useful for error diagnostics.
/// The compiler does not hold an instance of this.
#[derive(Default)]
pub struct SourceCodeRegistry {
    registry: FnvHashMap<SourceId, (DomainUrl, Arc<String>)>,
}

impl SourceCodeRegistry {
    pub fn register(&mut self, source_id: SourceId, url: DomainUrl, source: Arc<String>) {
        self.registry.insert(source_id, (url, source));
    }

    pub fn iter(&self) -> impl Iterator<Item = (SourceId, &DomainUrl, &Arc<String>)> {
        self.registry
            .iter()
            .map(|(source_id, (url, source))| (*source_id, url, source))
    }

    pub fn get(&self, source_id: SourceId) -> Option<(&DomainUrl, &Arc<String>)> {
        let entry = self.registry.get(&source_id)?;
        Some((&entry.0, &entry.1))
    }

    pub fn get_url(&self, source_id: SourceId) -> Option<&DomainUrl> {
        Some(&self.registry.get(&source_id)?.0)
    }

    pub fn get_source(&self, source_id: SourceId) -> Option<Arc<String>> {
        Some(self.registry.get(&source_id)?.1.clone())
    }
}
