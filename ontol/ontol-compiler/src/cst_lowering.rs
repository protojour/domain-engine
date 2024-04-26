#![allow(unused)]

use ontol_runtime::DefId;

use crate::{Compiler, Src};

pub struct CstLowering<'s, 'm> {
    compiler: &'s mut Compiler<'m>,
    src: &'s Src,
    root_defs: Vec<DefId>,
}
