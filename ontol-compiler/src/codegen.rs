use std::{collections::HashMap, fmt::Debug};

use ontol_runtime::{
    vm::{BuiltinProc, EntryPoint, Local, NArgs, OpCode, Program},
    DefId, PropertyId,
};

use crate::{
    compiler::Compiler,
    rewrite::rewrite,
    typed_expr::{NodeId, SyntaxVar, TypedExprKind, TypedExprTable},
    types::{Type, TypeRef},
};

#[derive(Default)]
pub struct CodegenTasks<'m> {
    tasks: Vec<CodegenTask<'m>>,
    pub result_program: Program,
    pub result_translations: HashMap<(DefId, DefId), EntryPoint>,
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
    pub typed_expr_table: TypedExprTable<'m>,
    pub node_a: NodeId,
    pub node_b: NodeId,
}

pub fn do_codegen(compiler: &mut Compiler) {
    let mut tasks = vec![];
    tasks.append(&mut compiler.codegen_tasks.tasks);

    let mut program = Program::default();
    let mut translations = HashMap::default();

    // FIXME: Do we need do topological sort of tasks?
    // At least in the beginning there is no support for recursive eq
    // (since there are no "monads" (option or iterators, etc) yet)
    for task in tasks {
        match task {
            CodegenTask::Eq(mut eq_task) => {
                // rewrite "with regards" to node_a:
                match rewrite(&mut eq_task.typed_expr_table, eq_task.node_a) {
                    Ok(()) => {
                        let typed_expr_table = &eq_task.typed_expr_table;

                        let key_a =
                            find_program_key(&typed_expr_table.expr_norewrite(eq_task.node_a).ty);
                        let key_b =
                            find_program_key(&typed_expr_table.expr_norewrite(eq_task.node_b).ty);
                        match (key_a, key_b) {
                            (Some(a), Some(b)) => {
                                println!("codegen entry point for ({a:?}, {b:?}");
                                let entry_point = codegen_translate(
                                    compiler,
                                    &mut program,
                                    typed_expr_table,
                                    eq_task.node_a,
                                    eq_task.node_b,
                                );

                                translations.insert((a, b), entry_point);
                            }
                            other => {
                                println!("unable to save translation: key = {other:?}");
                            }
                        }
                    }
                    Err(error) => {
                        panic!("TODO: could not rewrite: {error:?}");
                    }
                }
            }
        }
    }

    compiler.codegen_tasks.result_program = program;
    compiler.codegen_tasks.result_translations = translations;
}

fn find_program_key(ty: &TypeRef) -> Option<DefId> {
    match ty {
        Type::Domain(def_id) => Some(*def_id),
        other => {
            println!("unable to get program key: {other:?}");
            None
        }
    }
}

fn codegen_translate<'m>(
    compiler: &mut Compiler<'m>,
    program: &mut Program,
    table: &TypedExprTable<'m>,
    source_node: NodeId,
    target_node: NodeId,
) -> EntryPoint {
    let (_, src_expr) = table.fetch_expr(&table.source_rewrites, source_node);

    println!(
        "codegen src: {} trg: {}",
        table.debug_tree(&table.source_rewrites, source_node),
        table.debug_tree(&table.target_rewrites, target_node),
    );

    match &src_expr.kind {
        TypedExprKind::ValueObj(attr_node_id) => {
            codegen_value_obj_source(program, table, target_node)
        }
        TypedExprKind::MapObj(attributes) => {
            codegen_map_obj_source(program, table, attributes, target_node)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

fn codegen_value_obj_source<'m>(
    program: &mut Program,
    table: &TypedExprTable<'m>,
    target_node: NodeId,
) -> EntryPoint {
    let (_, target_expr) = table.fetch_expr(&table.target_rewrites, target_node);

    let mut opcodes = vec![];

    match &target_expr.kind {
        TypedExprKind::ValueObj(node_id) => {
            codegen_expr(table, *node_id, &mut opcodes, |var| Local(var.0));
            opcodes.push(OpCode::Return(Local(1)));
        }
        TypedExprKind::MapObj(target_attrs) => {
            opcodes.push(OpCode::CallBuiltin(BuiltinProc::NewCompound));

            for (property_id, node) in target_attrs {
                codegen_expr(table, *node, &mut opcodes, |var| Local(var.0 + 1));
                opcodes.push(OpCode::PutAttr(Local(1), *property_id));
            }

            opcodes.push(OpCode::Return(Local(1)));
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }

    println!("{opcodes:#?}");

    program.add_procedure(NArgs(1), opcodes)
}

fn codegen_map_obj_source<'m>(
    program: &mut Program,
    table: &TypedExprTable<'m>,
    source_properties: &HashMap<PropertyId, NodeId>,
    target_node: NodeId,
) -> EntryPoint {
    let (_, target_expr) = table.fetch_expr(&table.target_rewrites, target_node);

    let mut property_unpack: Vec<_> = source_properties
        .iter()
        .map(
            |(prop_id, node_id)| match &table.expr_norewrite(*node_id).kind {
                TypedExprKind::Variable(var) => (*prop_id, var),
                _ => panic!("source property not a variable"),
            },
        )
        .collect();
    property_unpack.sort_by_key(|(_, var)| *var);

    if !property_unpack.is_empty() {
        // must start with SyntaxVar(0)
        assert!(property_unpack[0].1 == &SyntaxVar(0));
    }

    let mut opcodes = vec![];

    match &target_expr.kind {
        TypedExprKind::MapObj(target_attrs) => {
            opcodes.push(OpCode::CallBuiltin(BuiltinProc::NewCompound));

            // Add property unpack
            // this generates pretty inefficient code, let's fix that later..
            for (property_id, _) in property_unpack {
                opcodes.push(OpCode::TakeAttr(Local(0), property_id));
            }

            for (property_id, node) in target_attrs {
                codegen_expr(table, *node, &mut opcodes, |var| Local(var.0 + 2));
                opcodes.push(OpCode::PutAttr(Local(1), *property_id));
            }

            opcodes.push(OpCode::Return(Local(1)));

            println!("{opcodes:#?}");
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }
    program.add_procedure(NArgs(1), opcodes)
}

fn codegen_expr<'m>(
    table: &TypedExprTable<'m>,
    expr_id: NodeId,
    opcodes: &mut Vec<OpCode>,
    var_local: impl Fn(SyntaxVar) -> Local + Copy,
) {
    let (_, expr) = table.fetch_expr(&table.target_rewrites, expr_id);
    match &expr.kind {
        TypedExprKind::Call(proc, params) => {
            for param in params.iter() {
                codegen_expr(table, *param, opcodes, var_local);
            }

            opcodes.push(OpCode::CallBuiltin(*proc));
        }
        TypedExprKind::Constant(k) => {
            opcodes.push(OpCode::Constant(*k));
        }
        TypedExprKind::Variable(var) => {
            opcodes.push(OpCode::Clone(var_local(*var)));
        }
        TypedExprKind::ValueObj(_) => {
            todo!()
        }
        TypedExprKind::MapObj(_) => {
            todo!()
        }
        TypedExprKind::Unit => {
            todo!()
        }
    }
}
