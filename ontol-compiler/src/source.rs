use std::{
    fmt::Debug,
    ops::{Deref, Range},
    sync::Arc,
};

use fnv::FnvHashMap;
use ontol_runtime::PackageId;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SourceId(pub u32);

#[derive(Clone, Debug)]
pub struct Src {
    pub id: SourceId,
    pub package_id: PackageId,
    pub name: Arc<String>,
}

impl Src {
    pub fn span(&self, range: &Range<usize>) -> SourceSpan {
        SourceSpan {
            source_id: self.id,
            start: range.start as u32,
            end: range.end as u32,
        }
    }
}

#[derive(Clone, Copy)]
pub struct SourceSpan {
    pub source_id: SourceId,
    pub start: u32,
    pub end: u32,
}

pub const NO_SPAN: SourceSpan = SourceSpan {
    source_id: SourceId(0),
    start: 0,
    end: 0,
};

impl Debug for SourceSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#({:?}):{}..{}", self.source_id, self.start, self.end)
    }
}

pub struct SpannedBorrow<'m, T> {
    pub value: &'m T,
    pub span: &'m SourceSpan,
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
    pub registry: FnvHashMap<SourceId, String>,
}

/// Sources currently being compiled
#[derive(Clone, Debug)]
pub struct Sources {
    next_source_id: SourceId,
    package: PackageId,
    sources: FnvHashMap<SourceId, Src>,
}

impl Default for Sources {
    fn default() -> Self {
        Self {
            next_source_id: SourceId(1),
            package: PackageId(0),
            sources: Default::default(),
        }
    }
}

impl Sources {
    /// Need this while the architecture is kind of broken
    pub fn source_id_for_package(&self, package: PackageId) -> Option<SourceId> {
        self.sources.iter().find_map(|(_, src)| {
            if src.package_id == package {
                Some(src.id)
            } else {
                None
            }
        })
    }

    pub fn find_source_by_package_id(&self, package_id: PackageId) -> Option<&Src> {
        self.sources
            .iter()
            .find(|(_, src)| src.package_id == package_id)
            .map(|(_, src)| src)
    }

    pub fn get_source(&self, id: SourceId) -> Option<Src> {
        self.sources.get(&id).cloned()
    }

    pub fn add_source(&mut self, package: PackageId, name: String) -> Src {
        let id = self.next_source_id;
        self.next_source_id.0 += 1;
        self.package = package;
        let src = Src {
            id,
            package_id: package,
            name: name.into(),
        };
        self.sources.insert(id, src.clone());
        src
    }
}
