use crate::{
    compiler::Compiler,
    expr::{ExprId, ExprKind},
    types::TypeRef,
};

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

pub fn do_codegen(compiler: &mut Compiler) {
    let mut tasks = vec![];
    tasks.append(&mut compiler.codegen_tasks.tasks);

    // FIXME: Do we need do topological sort of tasks?
    // At least in the beginning there is no support for recursive eq
    // (since there are no "monads" (option or iterators, etc) yet)
    for task in tasks {
        match task {
            CodegenTask::Eq(eq_task) => {
                // TODO: "mirror" this also
                eq_codegen(compiler, &eq_task.arm1, &eq_task.arm2);
            }
        }
    }
}

fn eq_codegen<'m>(compiler: &mut Compiler<'m>, from: &EqArm<'m>, to: &EqArm<'m>) {
    let from_expr = compiler.expressions.get(&from.expr_id).unwrap();
    match from_expr.kind {
        ExprKind::Obj(_, _) => {}
        _ => panic!(),
    }
}
