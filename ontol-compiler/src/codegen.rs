use crate::{
    compiler::Compiler,
    rewrite::rewrite,
    typed_expr::{NodeId, TypedExprTable},
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
    pub typed_expr_table: TypedExprTable<'m>,
    pub node_a: NodeId,
    pub node_b: NodeId,
}

pub fn do_codegen(compiler: &mut Compiler) {
    let mut tasks = vec![];
    tasks.append(&mut compiler.codegen_tasks.tasks);

    // FIXME: Do we need do topological sort of tasks?
    // At least in the beginning there is no support for recursive eq
    // (since there are no "monads" (option or iterators, etc) yet)
    for task in tasks {
        match task {
            CodegenTask::Eq(mut eq_task) => {
                // rewrite "with regards" to node_a:
                rewrite(&mut eq_task.typed_expr_table, eq_task.node_a);
                codegen_translate(
                    compiler,
                    &eq_task.typed_expr_table,
                    eq_task.node_a,
                    eq_task.node_b,
                );
            }
        }
    }
}

fn codegen_translate<'m>(
    _: &mut Compiler<'m>,
    table: &TypedExprTable<'m>,
    source_node: NodeId,
    target_node: NodeId,
) {
    let (_, src_expr) = table.fetch_expr(&table.source_rewrites, source_node);
    let (_, target_expr) = table.fetch_expr(&table.target_rewrites, target_node);

    println!(
        "src: {} trg: {}",
        table.debug_tree(&table.source_rewrites, source_node),
        table.debug_tree(&table.target_rewrites, target_node),
    );
}
