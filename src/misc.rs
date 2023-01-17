use std::{
    ops::Range,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct PackageId(pub u32);

pub struct Package {
    pub name: String,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct SourceId(pub u32);

pub struct Source {
    pub package_id: PackageId,
    pub path: PathBuf,
}

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
