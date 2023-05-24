use smallvec::SmallVec;
use tracing::debug;

use crate::typed_ontos::lang::OntosFunc;

use super::{
    proc_builder::{ProcBuilder, Scope},
    task::ProcTable,
};

pub(super) fn map_codegen_ontos(proc_table: &mut ProcTable, func: OntosFunc) -> bool {
    debug!("Generating code for\n{}", func.body);
    false
}

pub(super) struct OntosCodeGenerator<'a> {
    proc_table: &'a mut ProcTable,
    pub builder: &'a mut ProcBuilder,

    scope_stack: SmallVec<[Scope; 3]>,
}
