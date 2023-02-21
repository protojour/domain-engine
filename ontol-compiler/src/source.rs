use std::{collections::HashMap, fmt::Debug, ops::Range, sync::Arc};

use ontol_runtime::PackageId;

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SourceId(pub u32);

#[derive(Debug)]
pub struct Src {
    pub package_id: PackageId,
    pub name: Arc<String>,
}

#[derive(Clone, Copy)]
pub struct SourceSpan {
    pub source_id: SourceId,
    pub start: u32,
    pub end: u32,
}

impl Debug for SourceSpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#({:?}):{}..{}", self.source_id, self.start, self.end)
    }
}

impl SourceSpan {
    pub fn none() -> SourceSpan {
        Self {
            source_id: SourceId(0),
            start: 0,
            end: 0,
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

impl Debug for CompileSrc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompileSrc")
            .field("name", &self.name)
            .finish()
    }
}

impl CompileSrc {
    pub fn span(&self, range: &Range<usize>) -> SourceSpan {
        SourceSpan {
            source_id: self.id,
            start: range.start as u32,
            end: range.end as u32,
        }
    }
}

/// Sources currently being compiled
#[derive(Debug)]
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
    /// Need this while the architecture is kind of broken
    pub fn source_id_for_package(&self, package: PackageId) -> Option<SourceId> {
        self.compiled_sources
            .iter()
            .find_map(|(_, compiled_source)| {
                if compiled_source.package == package {
                    Some(compiled_source.id)
                } else {
                    None
                }
            })
    }

    pub fn find_compiled_source_by_name(&self, name: &str) -> Option<&CompileSrc> {
        self.compiled_sources
            .iter()
            .find(|(_, src)| src.name.as_ref() == name)
            .map(|(_, src)| src)
    }

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
