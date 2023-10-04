use ontol_hir::visitor::HirVisitor;
use tracing::warn;

use crate::{
    mem::Intern,
    typed_hir::{TypedHir, TypedNodeRef},
    types::{Type, TypeRef, Types},
};

use super::flat_scope::OutputVar;

#[derive(Debug)]
pub(super) struct SeqTypeInfer<'m> {
    pub output_seq_var: OutputVar,
    pub types: Vec<(TypeRef<'m>, TypeRef<'m>)>,
}

impl<'m> SeqTypeInfer<'m> {
    pub fn new(output_seq_var: OutputVar) -> Self {
        Self {
            output_seq_var,
            types: vec![],
        }
    }

    pub fn infer(self, types: &mut Types<'m>) -> TypeRef<'m> {
        let seq_type_pair = match self.types.len() {
            0 => panic!("Type of seq not inferrable"),
            1 => self.types.into_iter().next().unwrap(),
            _ => {
                warn!("Multiple seq types, imprecise inference");
                self.types.into_iter().next().unwrap()
            }
        };
        types.intern(Type::Seq(seq_type_pair.0, seq_type_pair.1))
    }

    pub fn traverse(&mut self, node_ref: TypedNodeRef<'_, 'm>) {
        SeqTypeLocator {
            output_seq_var: self.output_seq_var,
            types: &mut self.types,
        }
        .visit_node(0, node_ref);
    }
}

struct SeqTypeLocator<'i, 'm> {
    output_seq_var: OutputVar,
    types: &'i mut Vec<(TypeRef<'m>, TypeRef<'m>)>,
}

impl<'i, 'h, 'm: 'h> ontol_hir::visitor::HirVisitor<'h, 'm, TypedHir> for SeqTypeLocator<'i, 'm> {
    fn visit_node(&mut self, _: usize, node_ref: TypedNodeRef<'h, 'm>) {
        let arena = node_ref.arena();
        if let ontol_hir::Kind::SeqPush(seq_var, attr) = node_ref.kind() {
            if seq_var == &self.output_seq_var.0 {
                self.types
                    .push((arena[attr.rel].ty(), arena[attr.val].ty()));
            }
        }
        self.traverse_node(node_ref);
    }
}
