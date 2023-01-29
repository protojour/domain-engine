use crate::{compiler::Compiler, expr::ExprId, types::TypeRef};

#[derive(Default, Debug)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
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
    pub arm1: EqArm<'m>,
    pub arm2: EqArm<'m>,
}

#[derive(Debug)]
pub struct EqArm<'m> {
    pub expr_id: ExprId,
    pub ty: TypeRef<'m>,
}

pub fn do_codegen(compiler: &mut Compiler) {}
