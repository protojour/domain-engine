use fnv::FnvHashMap;
use ontol_runtime::smart_format;
use ontos::{kind::NodeKind, visitor::OntosMutVisitor};

use crate::{
    error::CompileError,
    typed_ontos::lang::{Meta, OntosNode, TypedOntos},
    types::Types,
    CompileErrors,
};

use super::{
    inference::{Infer, TypeVar},
    unify_ctx::{Arm, VariableMapping},
    TypeError,
};

pub(super) struct OntosArmTypeInference<'c, 'm> {
    pub(super) types: &'c mut Types<'m>,
    pub(super) eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
    pub(super) errors: &'c mut CompileErrors,
}

impl<'c, 'm> OntosMutVisitor<'m, TypedOntos> for OntosArmTypeInference<'c, 'm> {
    fn visit_node(&mut self, index: usize, node: &mut <TypedOntos as ontos::Lang>::Node<'m>) {
        let mut infer = Infer {
            types: self.types,
            eq_relations: self.eq_relations,
        };
        match infer.infer_recursive(node.meta.ty) {
            Ok(ty) => node.meta.ty = ty,
            Err(TypeError::Propagated) => {}
            Err(TypeError::NotEnoughInformation) => {
                self.errors.push(
                    CompileError::TODO(smart_format!("Not enough type information"))
                        .spanned(&node.meta.span),
                );
            }
            _ => panic!("Unexpected inference error"),
        }

        self.visit_kind(index, &mut node.kind);
    }
}

pub(super) struct OntosVariableMapper<'c, 'm> {
    pub variable_mapping: &'c FnvHashMap<ontos::Variable, VariableMapping<'m>>,
    pub arm: Arm,
}

impl<'c, 'm> OntosMutVisitor<'m, TypedOntos> for OntosVariableMapper<'c, 'm> {
    fn visit_node(&mut self, index: usize, node: &mut <TypedOntos as ontos::Lang>::Node<'m>) {
        self.visit_kind(index, &mut node.kind);

        if let NodeKind::VariableRef(var) = &node.kind {
            if let Some(var_mapping) = self.variable_mapping.get(var) {
                let mapped_type = match self.arm {
                    Arm::First => var_mapping.second_arm_type,
                    Arm::Second => var_mapping.first_arm_type,
                };

                let variable_ref = OntosNode {
                    kind: NodeKind::VariableRef(*var),
                    meta: node.meta,
                };
                let map = OntosNode {
                    kind: NodeKind::Map(Box::new(variable_ref)),
                    meta: Meta {
                        ty: mapped_type,
                        span: node.meta.span,
                    },
                };
                *node = map;
            }
        }
    }
}
