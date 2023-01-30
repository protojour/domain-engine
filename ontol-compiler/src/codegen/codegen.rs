use std::collections::HashMap;

use ontol_runtime::{
    vm::{BuiltinProc, EntryPoint, Local, NArgs, OpCode, Program},
    DefId, PropertyId,
};

use crate::{
    codegen::typed_expr::TypedExprKind,
    compiler::Compiler,
    types::{Type, TypeRef},
};

use super::{
    rewrite::rewrite,
    typed_expr::{NodeId, SealedTypedExprTable, SyntaxVar, TypedExprTable},
    CodegenTask,
};

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
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
                // a -> b
                codegen_translate_rewrite(
                    &mut program,
                    &mut translations,
                    &mut eq_task.typed_expr_table,
                    eq_task.node_a,
                    eq_task.node_b,
                );

                eq_task.typed_expr_table.reset();

                // b -> a
                codegen_translate_rewrite(
                    &mut program,
                    &mut translations,
                    &mut eq_task.typed_expr_table,
                    eq_task.node_b,
                    eq_task.node_a,
                );
            }
        }
    }

    compiler.codegen_tasks.result_program = program;
    compiler.codegen_tasks.result_translations = translations;
}

fn codegen_translate_rewrite(
    program: &mut Program,
    translations: &mut HashMap<(DefId, DefId), EntryPoint>,
    table: &mut SealedTypedExprTable,
    origin_node: NodeId,
    dest_node: NodeId,
) -> bool {
    match rewrite(&mut table.inner, origin_node) {
        Ok(()) => {
            let key_a = find_program_key(&table.inner.get_expr_no_rewrite(origin_node).ty);
            let key_b = find_program_key(&table.inner.get_expr_no_rewrite(dest_node).ty);
            match (key_a, key_b) {
                (Some(a), Some(b)) => {
                    let entry_point =
                        codegen_translate(program, &table.inner, origin_node, dest_node);

                    translations.insert((a, b), entry_point);
                    true
                }
                other => {
                    println!("unable to save translation: key = {other:?}");
                    false
                }
            }
        }
        Err(error) => {
            panic!("TODO: could not rewrite: {error:?}");
        }
    }
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
    program: &mut Program,
    table: &TypedExprTable<'m>,
    origin_node: NodeId,
    dest_node: NodeId,
) -> EntryPoint {
    let (_, src_expr) = table.get_expr(&table.source_rewrites, origin_node);

    println!(
        "codegen origin: {} dest: {}",
        table.debug_tree(&table.source_rewrites, origin_node),
        table.debug_tree(&table.target_rewrites, dest_node),
    );

    match &src_expr.kind {
        TypedExprKind::ValueObj(_) => codegen_value_obj_source(program, table, dest_node),
        TypedExprKind::MapObj(attributes) => {
            codegen_map_obj_source(program, table, attributes, dest_node)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

fn codegen_value_obj_source<'m>(
    program: &mut Program,
    table: &TypedExprTable<'m>,
    target_node: NodeId,
) -> EntryPoint {
    let (_, target_expr) = table.get_expr(&table.target_rewrites, target_node);

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
    let (_, target_expr) = table.get_expr(&table.target_rewrites, target_node);

    let mut property_unpack: Vec<_> = source_properties
        .iter()
        .map(
            |(prop_id, node_id)| match &table.get_expr_no_rewrite(*node_id).kind {
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
    let (_, expr) = table.get_expr(&table.target_rewrites, expr_id);
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
