use indexmap::IndexMap;
use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams},
    value::PropertyId,
};

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::{Ir, Terminator},
        proc_builder::Block,
    },
    typed_expr::{BindDepth, ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{equation::TypedExprEquation, proc_builder::ProcBuilder, ProcTable};

/// Generate code originating from a map obj destructuring
pub(super) fn codegen_map_obj_origin(
    proc_table: &mut ProcTable,
    equation: &TypedExprEquation,
    to: ExprRef,
    origin_attrs: &IndexMap<PropertyId, ExprRef>,
) -> ProcBuilder {
    let (_, to_expr, span) = equation.resolve_expr(&equation.expansions, to);

    // always start at 0 (for now), there are no recursive map_objs (yet)
    let input_local = 0;

    // debug!("origin attrs: {origin_attrs:#?}");
    // debug!("reductions: {:?}", equation.reductions.debug_table());

    let mut origin_properties: Vec<_> = origin_attrs
        .iter()
        .map(|(prop_id, expr_ref)| {
            match &equation
                .resolve_expr(&equation.reductions, *expr_ref)
                .1
                .kind
            {
                TypedExprKind::Variable(var) => (*prop_id, *var),
                other => {
                    panic!(
                        "reduced property {prop_id:?} {expr_ref:?} not a variable, but {other:?}"
                    )
                }
            }
        })
        .collect();
    origin_properties.sort_by_key(|(_, var)| *var);

    if !origin_properties.is_empty() {
        // must start with SyntaxVar(0)
        assert!(origin_properties[0].1 == SyntaxVar(0, BindDepth(0)));
    }

    struct MapCodegen {
        attr_start: u16,
    }

    impl CodegenVariable for MapCodegen {
        fn codegen_variable(
            &mut self,
            builder: &mut ProcBuilder,
            block: &mut Block,
            var: SyntaxVar,
            span: &SourceSpan,
        ) {
            // To find the value local, we refer to the first local resulting from TakeAttr2,
            // after the definition of the return value
            let value_local = self.attr_start + (var.0 * 2) + 1;

            builder.push(1, Ir::Clone(Local(value_local)), *span, block);
        }
    }

    let mut builder = ProcBuilder::new(NParams(1));

    let block = match &to_expr.kind {
        TypedExprKind::MapObjPattern(dest_attrs) => {
            let mut block = builder.new_block(span);
            let return_def_id = to_expr.ty.get_single_def_id().unwrap();

            // Local(1), this is the return value:
            builder.push(
                1,
                Ir::CallBuiltin(BuiltinProc::NewMap, return_def_id),
                span,
                &mut block,
            );

            // unpack attributes
            for (property_id, _) in &origin_properties {
                builder.push(
                    2,
                    Ir::TakeAttr2(Local(input_local), *property_id),
                    span,
                    &mut block,
                );
            }

            CodeGenerator::default().enter_bind_level(
                MapCodegen {
                    attr_start: input_local + 2,
                },
                |generator| {
                    for (property_id, expr_ref) in dest_attrs {
                        generator.codegen_expr(
                            proc_table,
                            &mut builder,
                            &mut block,
                            equation,
                            *expr_ref,
                        );
                        builder.push(
                            -1,
                            Ir::PutAttrValue(Local(1), *property_id),
                            span,
                            &mut block,
                        );
                    }
                },
            );

            block.terminator = Some(Terminator::Return(Local(1)));

            block
        }
        TypedExprKind::ValueObjPattern(expr_ref) => {
            let mut block = builder.new_block(span);

            // unpack attributes
            for (property_id, _) in &origin_properties {
                builder.push(
                    2,
                    Ir::TakeAttr2(Local(input_local), *property_id),
                    span,
                    &mut block,
                );
            }

            CodeGenerator::default().enter_bind_level(
                MapCodegen {
                    attr_start: input_local + 1,
                },
                |generator| {
                    generator.codegen_expr(
                        proc_table,
                        &mut builder,
                        &mut block,
                        equation,
                        *expr_ref,
                    );
                    block.terminator = Some(Terminator::Return(builder.top()));
                },
            );

            block
        }
        kind => {
            todo!("to: {kind:?}");
        }
    };

    builder.commit(block);

    builder
}
