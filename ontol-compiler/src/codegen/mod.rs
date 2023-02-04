use std::{collections::HashMap, fmt::Debug};

use ontol_runtime::{
    proc::{Lib, Procedure},
    DefId,
};

pub mod typed_expr;

mod codegen;
mod codegen_map_obj;
mod link;
mod rewrite;

pub use codegen::execute_codegen_tasks;

use crate::SourceSpan;

use self::typed_expr::{NodeId, SealedTypedExprTable};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
    pub result_lib: Lib,
    pub result_translations: HashMap<(DefId, DefId), Procedure>,
}

impl<'m> Debug for CodegenTasks<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodegenTasks")
            .field("tasks", &self.tasks)
            .finish()
    }
}

impl<'m> CodegenTasks<'m> {
    pub fn push(&mut self, task: CodegenTask<'m>) {
        self.tasks.push(task);
    }
}

#[derive(Debug)]
pub enum CodegenTask<'m> {
    Eq(EqCodegenTask<'m>),
}

#[derive(Debug)]
pub struct EqCodegenTask<'m> {
    pub typed_expr_table: SealedTypedExprTable<'m>,
    pub node_a: NodeId,
    pub node_b: NodeId,
    pub span: SourceSpan,
}
