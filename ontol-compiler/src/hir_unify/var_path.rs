use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::{
    kind::{Dimension, NodeKind, PropVariant},
    visitor::HirVisitor,
    Node, Variable,
};
use smallvec::SmallVec;

use crate::typed_hir::{TypedHir, TypedHirNode};

use super::UnifierError;

#[derive(Clone, Default, Debug)]
pub struct Path(pub SmallVec<[u16; 32]>);

pub fn locate_variables(
    node: &TypedHirNode,
    variables: &BitSet,
) -> Result<FnvHashMap<Variable, Path>, UnifierError> {
    let mut locator = VarLocator::new(variables);
    locator.traverse_kind(node.kind());
    locator.finish()
}

struct VarLocator<'a> {
    variables: &'a BitSet,
    current_path: Path,

    duplicates: BitSet,
    output: FnvHashMap<Variable, Path>,
}

impl<'a> VarLocator<'a> {
    fn finish(self) -> Result<FnvHashMap<Variable, Path>, UnifierError> {
        if !self.duplicates.is_empty() {
            Err(UnifierError::NonUniqueVariableDatapoints(self.duplicates))
        } else {
            Ok(self.output)
        }
    }

    pub(super) fn new(variables: &'a BitSet) -> Self {
        Self {
            variables,
            current_path: Path::default(),
            duplicates: BitSet::new(),
            output: FnvHashMap::default(),
        }
    }

    fn enter_child(&mut self, index: usize, func: impl FnOnce(&mut Self)) {
        self.current_path.0.push(index as u16);
        func(self);
        self.current_path.0.pop();
    }

    fn register_var(&mut self, var: u32) {
        if self.variables.contains(var as usize)
            && self
                .output
                .insert(Variable(var), self.current_path.clone())
                .is_some()
        {
            self.duplicates.insert(var as usize);
        }
    }
}

impl<'a, 'm> HirVisitor<'m, TypedHir> for VarLocator<'a> {
    fn visit_variable(&mut self, variable: &Variable) {
        self.register_var(variable.0);
    }

    fn visit_label(&mut self, label: &ontol_hir::Label) {
        self.register_var(label.0);
    }

    fn visit_kind(&mut self, index: usize, kind: &NodeKind<'m, TypedHir>) {
        self.enter_child(index, |_self| _self.traverse_kind(kind));
    }

    fn visit_prop_variant(&mut self, index: usize, variant: &PropVariant<'m, TypedHir>) {
        self.enter_child(index, |_self| {
            match &variant.dimension {
                Dimension::Singular => {
                    _self.traverse_prop_variant(variant);
                }
                Dimension::Seq(label) => {
                    // The search stops here for now, sequence mappings are black boxes,
                    // only register the label:
                    _self.visit_label(label);
                }
            }
        });
    }

    fn visit_match_arm(
        &mut self,
        index: usize,
        match_arm: &ontol_hir::kind::MatchArm<'m, TypedHir>,
    ) {
        self.enter_child(index, |_self| _self.traverse_match_arm(match_arm));
    }

    fn visit_pattern_binding(&mut self, index: usize, binding: &ontol_hir::kind::PatternBinding) {
        self.enter_child(index, |_self| _self.traverse_pattern_binding(binding));
    }
}
