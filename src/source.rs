use std::{collections::HashMap, ops::Range, sync::Arc};

use crate::SString;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct PackageId(pub u32);

pub const CORE_PKG: PackageId = PackageId(0);

#[cfg(test)]
pub const TEST_PKG: PackageId = PackageId(1337);

pub struct Package {
    pub name: SString,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SourceId(pub u32);

pub struct Src {
    pub package_id: PackageId,
    pub name: Arc<String>,
}

#[derive(Clone)]
pub struct SourceSpan {
    pub source_id: SourceId,
    pub range: Range<u32>,
}

impl SourceSpan {
    pub fn none() -> SourceSpan {
        Self {
            source_id: SourceId(0),
            range: 0..0,
        }
    }
}

#[derive(Clone)]
pub struct CompileSrc {
    pub package: PackageId,
    pub id: SourceId,
    pub name: Arc<String>,
    pub text: Arc<String>,
}

impl CompileSrc {
    pub fn span(&self, span: Range<usize>) -> SourceSpan {
        SourceSpan {
            source_id: self.id,
            range: Range {
                start: span.start as u32,
                end: span.end as u32,
            },
        }
    }
}

/// Sources currently being compiled
pub struct Sources {
    next_source_id: SourceId,
    package: PackageId,
    sources: HashMap<SourceId, Src>,
    compiled_sources: HashMap<SourceId, CompileSrc>,
}

impl Default for Sources {
    fn default() -> Self {
        Self {
            next_source_id: SourceId(1),
            package: PackageId(0),
            sources: Default::default(),
            compiled_sources: Default::default(),
        }
    }
}

impl Sources {
    pub fn get_compiled_source(&self, id: SourceId) -> Option<CompileSrc> {
        self.compiled_sources.get(&id).cloned()
    }

    pub fn add(&mut self, package: PackageId, name: String, text: String) -> CompileSrc {
        let id = self.next_source_id;
        self.next_source_id.0 += 1;
        self.package = package;
        let src = CompileSrc {
            package,
            id,
            name: Arc::new(name),
            text: Arc::new(text),
        };
        self.compiled_sources.insert(id, src.clone());
        self.sources.insert(
            id,
            Src {
                package_id: package,
                name: src.name.clone(),
            },
        );
        src
    }

    pub fn compile_finished(&mut self) {
        self.compiled_sources.clear();
    }
}
