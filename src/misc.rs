use std::{
    ops::Range,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy)]
pub struct PackageId(u32);

pub struct Package {
    pub name: String,
}

#[derive(Clone, Copy)]
pub struct SourceId(u32);

pub struct Source {
    pub package_id: PackageId,
    pub path: PathBuf,
}

pub struct SourceSpan {
    pub source_id: SourceId,
    pub range: Range<u32>,
}
