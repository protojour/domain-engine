use fnv::FnvHashMap;
use ontol_hir::{Label, Variable};

use crate::{expr::ExprId, types::TypeRef, SourceSpan};

use super::inference::Inference;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct CtrlFlowDepth(pub u16);

pub struct HirBuildCtx<'m> {
    pub inference: Inference<'m>,
    pub explicit_variables: FnvHashMap<ExprId, ExplicitVariable>,
    pub label_map: FnvHashMap<ExprId, Label>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub variable_mapping: FnvHashMap<Variable, VariableMapping<'m>>,

    pub partial: bool,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,
    ctrl_flow_depth: CtrlFlowDepth,
    next_variable: Variable,
}

impl<'m> HirBuildCtx<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            explicit_variables: Default::default(),
            label_map: Default::default(),
            ctrl_flow_forest: Default::default(),
            variable_mapping: Default::default(),
            partial: false,
            arm: Arm::First,
            ctrl_flow_depth: CtrlFlowDepth(0),
            next_variable: Variable(0),
        }
    }

    pub fn current_ctrl_flow_depth(&self) -> CtrlFlowDepth {
        self.ctrl_flow_depth
    }

    pub fn enter_ctrl<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        self.ctrl_flow_depth.0 += 1;
        let ret = f(self);
        self.ctrl_flow_depth.0 -= 1;

        ret
    }

    pub fn alloc_variable(&mut self) -> Variable {
        let next = self.next_variable;
        self.next_variable.0 += 1;
        next
    }
}

pub struct ExplicitVariable {
    pub variable: Variable,
    pub ctrl_group: Option<CtrlFlowGroup>,
    pub hir_arms: FnvHashMap<Arm, ExplicitVariableArm>,
}

pub struct VariableMapping<'m> {
    pub first_arm_type: TypeRef<'m>,
    pub second_arm_type: TypeRef<'m>,
}

pub struct ExplicitVariableArm {
    // In hir, the variable has a different expr id depending on which arm it's in
    pub expr_id: ExprId,
    pub span: SourceSpan,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CtrlFlowGroup {
    pub label: Label,
    pub bind_depth: CtrlFlowDepth,
}

/// Tracks which control flow "statements" are children of other control flow "statements"
#[derive(Default)]
pub struct CtrlFlowForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<Label, Label>,
}

impl CtrlFlowForest {
    pub fn insert(&mut self, label: Label, parent_label: Option<Label>) {
        self.map.insert(label, parent_label.unwrap_or(label));
    }

    pub fn find_parent(&self, label: Label) -> Option<Label> {
        let parent = self.map.get(&label).unwrap();
        if parent == &label {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut label: Label) -> Label {
        loop {
            let parent = self.map.get(&label).unwrap();
            if parent == &label {
                return label;
            }
            label = *parent;
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Arm {
    First,
    Second,
}

impl Arm {
    pub fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }
}
