use ontol_runtime::{
    proc::{BuiltinProc, Local},
    DefId,
};

use crate::{
    codegen::{
        generator::{CodeGenerator, Scope},
        ir::Ir,
        proc_builder::Stack,
        Block,
    },
    hir_node::{BindDepth, HirIdx, HirKind, HirVariable},
};

use super::equation::HirEquation;

pub(super) fn codegen_value_pattern_origin(
    gen: &mut CodeGenerator,
    block: &mut Block,
    equation: &HirEquation,
    to: HirIdx,
    to_def: DefId,
) {
    let (_, to_expr, span) = equation.resolve_node(&equation.expansions, to);

    let mut scope = Scope::default();
    scope
        .in_scope
        // FIXME, this won't always be variable 0
        .insert(HirVariable(0, BindDepth(0)), Local(0));

    gen.enter_scope(scope, |generator| match &to_expr.kind {
        HirKind::ValuePattern(node_id) => {
            generator.codegen_expr(block, equation, *node_id);
        }
        HirKind::StructPattern(dest_attrs) => {
            generator.builder.push(
                block,
                Ir::CallBuiltin(BuiltinProc::NewMap, to_def),
                Stack(1),
                span,
            );

            for (property_id, node) in dest_attrs {
                generator.codegen_expr(block, equation, *node);
                generator.builder.push(
                    block,
                    Ir::PutAttrValue(Local(1), *property_id),
                    Stack(-1),
                    span,
                );
            }
        }
        kind => {
            todo!("target: {kind:?}");
        }
    })
}
