use indexmap::IndexMap;
use ontol_runtime::{proc::Local, value::PropertyId};

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::Ir,
        proc_builder::{Block, Stack},
    },
    typed_expr::{BindDepth, ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{equation::TypedExprEquation, proc_builder::ProcBuilder};

struct UnpackProp {
    var: SyntaxVar,
    kind: UnpackKind,
}

enum UnpackKind {
    Leaf(PropertyId),
    Struct {
        property_id: PropertyId,
        unpack_props: Vec<UnpackProp>,
    },
}

/// Generate code originating from a struct destructuring
pub(super) fn codegen_struct_pattern_origin(
    gen: &mut CodeGenerator,
    block: &mut Block,
    equation: &TypedExprEquation,
    to: ExprRef,
    origin_attrs: &IndexMap<PropertyId, ExprRef>,
) {
    let (_, to_expr, span) = equation.resolve_expr(&equation.expansions, to);

    // always start at 0 (for now), there are no recursive structs (yet)
    let input_local = 0;

    // debug!("origin attrs: {origin_attrs:#?}");
    // debug!("reductions: {:?}", equation.reductions.debug_table());

    let unpack_props = analyze_unpack(equation, origin_attrs);

    if !unpack_props.is_empty() {
        // must start with SyntaxVar(0)
        assert!(unpack_props[0].var == SyntaxVar(0, BindDepth(0)));
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

            builder.push(block, Ir::Clone(Local(value_local)), Stack(1), *span);
        }
    }

    generate_prop_unpack(gen, block, Local(input_local), &unpack_props, span);

    let map_codegen = MapCodegen {
        attr_start: input_local + 1,
    };

    match &to_expr.kind {
        TypedExprKind::StructPattern(_) => gen.enter_bind_level(map_codegen, |generator| {
            generator.codegen_expr(block, equation, to);
        }),
        TypedExprKind::ValuePattern(expr_ref) => gen.enter_bind_level(map_codegen, |generator| {
            generator.codegen_expr(block, equation, *expr_ref);
        }),
        TypedExprKind::MapSequenceBalanced(..) => gen.enter_bind_level(map_codegen, |generator| {
            generator.codegen_expr(block, equation, to);
        }),
        kind => {
            todo!("to: {kind:?}");
        }
    }
}

fn analyze_unpack(
    equation: &TypedExprEquation,
    attrs: &IndexMap<PropertyId, ExprRef>,
) -> Vec<UnpackProp> {
    let mut destr: Vec<_> = attrs
        .iter()
        .filter_map(|(prop_id, expr_ref)| {
            match &equation
                .resolve_expr(&equation.reductions, *expr_ref)
                .1
                .kind
            {
                TypedExprKind::Variable(var) => Some(UnpackProp {
                    var: *var,
                    kind: UnpackKind::Leaf(*prop_id),
                }),
                TypedExprKind::StructPattern(inner_attrs) => {
                    let unpack_props = analyze_unpack(equation, inner_attrs);
                    let first = unpack_props.first()?;

                    Some(UnpackProp {
                        var: first.var,
                        kind: UnpackKind::Struct {
                            property_id: *prop_id,
                            unpack_props,
                        },
                    })
                }
                other => {
                    panic!(
                        "reduced property {prop_id:?} {expr_ref:?} not a variable, but {other:?}"
                    )
                }
            }
        })
        .collect();

    destr.sort_by_key(|destr| destr.var);
    destr
}

fn generate_prop_unpack(
    gen: &mut CodeGenerator,
    block: &mut Block,
    source_local: Local,
    unpack_props: &[UnpackProp],
    span: SourceSpan,
) {
    for unpack_prop in unpack_props {
        match &unpack_prop.kind {
            UnpackKind::Leaf(property_id) => {
                gen.builder.push(
                    block,
                    Ir::TakeAttr2(source_local, *property_id),
                    Stack(2),
                    span,
                );
            }
            UnpackKind::Struct {
                property_id,
                unpack_props: inner_unpack_props,
            } => {
                gen.builder.push(
                    block,
                    Ir::TakeAttr2(source_local, *property_id),
                    Stack(2),
                    span,
                );

                let struct_local = gen.builder.top();
                let rel_params_local = gen.builder.top_minus(1);

                generate_prop_unpack(gen, block, struct_local, inner_unpack_props, span);

                // Remove the struct from the stack..
                // FIXME: This is inefficient, should instead just keep it on the stack,
                // but then SyntaxVar no longer has a linear relationship with which Local to use,
                // so a lookup table must be used instead
                gen.builder
                    .push(block, Ir::Remove(struct_local), Stack(-1), span);
                gen.builder
                    .push(block, Ir::Remove(rel_params_local), Stack(-1), span);
            }
        }
    }
}
