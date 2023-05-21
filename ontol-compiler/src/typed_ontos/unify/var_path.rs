use bit_set::BitSet;
use fnv::FnvHashMap;
use ontos::{kind::NodeKind, visitor::OntosVisitor, Variable};
use smallvec::SmallVec;

use crate::typed_ontos::lang::TypedOntos;

#[derive(Clone, Default, Debug)]
pub struct Path(pub SmallVec<[u16; 32]>);

pub struct LocateCtx<'a> {
    variables: &'a BitSet,
    current_path: Path,

    pub(super) output: FnvHashMap<Variable, Path>,
}

impl<'a> LocateCtx<'a> {
    pub(super) fn new(variables: &'a BitSet) -> Self {
        Self {
            variables,
            output: FnvHashMap::default(),
            current_path: Path::default(),
        }
    }

    fn enter_child(&mut self, index: usize, func: impl Fn(&mut Self)) {
        self.current_path.0.push(index as u16);
        func(self);
        self.current_path.0.pop();
    }
}

impl<'a> OntosVisitor<TypedOntos> for LocateCtx<'a> {
    fn visit_variable(&mut self, variable: &Variable) {
        if self.variables.contains(variable.0 as usize)
            && self
                .output
                .insert(*variable, self.current_path.clone())
                .is_some()
        {
            panic!("BUG: Variable appears more than once");
        }
    }

    fn visit_kind(&mut self, index: usize, kind: &NodeKind<'_, TypedOntos>) {
        self.enter_child(index, |ctx| ctx.traverse_kind(kind));
    }

    fn visit_match_arm(&mut self, index: usize, match_arm: &ontos::kind::MatchArm<'_, TypedOntos>) {
        self.enter_child(index, |ctx| ctx.traverse_match_arm(match_arm));
    }

    fn visit_pattern_binding(&mut self, index: usize, binding: &ontos::kind::PatternBinding) {
        self.enter_child(index, |ctx| ctx.traverse_pattern_binding(binding));
    }
}
