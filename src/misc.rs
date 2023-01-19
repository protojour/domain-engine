use std::{ops::Range, path::PathBuf, sync::Arc};

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct PackageId(pub u32);

pub const CORE_PKG: PackageId = PackageId(0);
pub const TEST_PKG: PackageId = PackageId(1337);

pub struct Package {
    pub name: String,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SourceId(pub u32);

pub struct Source {
    pub package_id: PackageId,
    pub path: PathBuf,
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
