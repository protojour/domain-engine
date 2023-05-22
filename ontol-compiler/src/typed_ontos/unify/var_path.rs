use bit_set::BitSet;
use fnv::FnvHashMap;
use ontos::{kind::NodeKind, visitor::OntosVisitor, Node, Variable};
use smallvec::SmallVec;

use crate::typed_ontos::lang::{OntosNode, TypedOntos};

use super::UnifierError;

#[derive(Clone, Default, Debug)]
pub struct Path(pub SmallVec<[u16; 32]>);

pub fn locate_variables(
    node: &mut OntosNode,
    variables: &BitSet,
) -> Result<FnvHashMap<Variable, Path>, UnifierError> {
    let mut locator = VarLocator::new(variables);

    locator.traverse_kind(node.kind_mut());

    if !locator.duplicates.is_empty() {
        Err(UnifierError::NonUniqueVariableDatapoints(
            locator.duplicates,
        ))
    } else {
        Ok(locator.output)
    }
}

struct VarLocator<'a> {
    variables: &'a BitSet,
    current_path: Path,

    duplicates: BitSet,
    output: FnvHashMap<Variable, Path>,
}

impl<'a> VarLocator<'a> {
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
}

impl<'a, 'm> OntosVisitor<'m, TypedOntos> for VarLocator<'a> {
    fn visit_variable(&mut self, variable: &mut Variable) {
        if self.variables.contains(variable.0 as usize)
            && self
                .output
                .insert(*variable, self.current_path.clone())
                .is_some()
        {
            self.duplicates.insert(variable.0 as usize);
        }
    }

    fn visit_kind(&mut self, index: usize, kind: &mut NodeKind<'m, TypedOntos>) {
        self.enter_child(index, |ctx| ctx.traverse_kind(kind));
    }

    fn visit_match_arm(
        &mut self,
        index: usize,
        match_arm: &mut ontos::kind::MatchArm<'m, TypedOntos>,
    ) {
        self.enter_child(index, |ctx| ctx.traverse_match_arm(match_arm));
    }

    fn visit_pattern_binding(&mut self, index: usize, binding: &mut ontos::kind::PatternBinding) {
        self.enter_child(index, |ctx| ctx.traverse_pattern_binding(binding));
    }
}
