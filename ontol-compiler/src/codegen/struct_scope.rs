use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, vm::proc::Local};

use crate::{
    codegen::{
        generator::CodeGenerator,
        ir::Ir,
        proc_builder::{Block, Stack},
    },
    hir_node::{HirIdx, HirKind, HirVariable},
    SourceSpan,
};

use super::{equation::HirEquation, generator::Scope};

struct UnpackProp {
    var: HirVariable,
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
pub(super) fn codegen_struct_pattern_scope(
    gen: &mut CodeGenerator,
    block: &mut Block,
    equation: &HirEquation,
    struct_local: Local,
    origin_attrs: &IndexMap<PropertyId, HirIdx>,
    span: SourceSpan,
) -> Scope {
    let unpack_props = analyze_unpack(equation, origin_attrs);

    let mut scope = Scope::default();
    define_locals(gen, block, struct_local, &unpack_props, &mut scope, span);
    scope
}

fn analyze_unpack(equation: &HirEquation, attrs: &IndexMap<PropertyId, HirIdx>) -> Vec<UnpackProp> {
    let mut destr: Vec<_> = attrs
        .iter()
        .filter_map(|(prop_id, node_id)| {
            match &equation.resolve_node(&equation.reductions, *node_id).1.kind {
                HirKind::Variable(var) => Some(UnpackProp {
                    var: *var,
                    kind: UnpackKind::Leaf(*prop_id),
                }),
                HirKind::StructPattern(inner_attrs) => {
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
                    panic!("reduced property {prop_id:?} {node_id:?} not a variable, but {other:?}")
                }
            }
        })
        .collect();

    destr.sort_by_key(|destr| destr.var);
    destr
}

fn define_locals(
    gen: &mut CodeGenerator,
    block: &mut Block,
    source_local: Local,
    unpack_props: &[UnpackProp],
    scope: &mut Scope,
    span: SourceSpan,
) {
    for unpack_prop in unpack_props {
        match &unpack_prop.kind {
            UnpackKind::Leaf(property_id) => {
                gen.builder.append(
                    block,
                    Ir::TakeAttr2(source_local, *property_id),
                    Stack(2),
                    span,
                );
                scope.in_scope.insert(unpack_prop.var, gen.builder.top());
            }
            UnpackKind::Struct {
                property_id,
                unpack_props: inner_unpack_props,
            } => {
                gen.builder.append(
                    block,
                    Ir::TakeAttr2(source_local, *property_id),
                    Stack(2),
                    span,
                );

                let struct_local = gen.builder.top();

                define_locals(gen, block, struct_local, inner_unpack_props, scope, span);
            }
        }
    }
}
