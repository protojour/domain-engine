use fnv::FnvHashMap;
use ontol_hir::{Label, Variable};

use crate::{
    expr::{Expr, ExprId},
    types::TypeRef,
    SourceSpan,
};

use super::inference::Inference;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct BindDepth(pub u16);

pub struct CheckUnifyExprContext<'m> {
    pub inference: Inference<'m>,
    pub bodies: Vec<CtrlFlowBody<'m>>,
    pub explicit_variables: FnvHashMap<ExprId, ExplicitVariable>,
    pub label_map: FnvHashMap<ExprId, Label>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub variable_mapping: FnvHashMap<Variable, VariableMapping<'m>>,

    pub partial: bool,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,
    bind_depth: BindDepth,
    ontos_var_allocations: Vec<u32>,
}

impl<'m> CheckUnifyExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            bodies: Default::default(),
            explicit_variables: Default::default(),
            label_map: Default::default(),
            ctrl_flow_forest: Default::default(),
            variable_mapping: Default::default(),
            partial: false,
            arm: Arm::First,
            bind_depth: BindDepth(0),
            ontos_var_allocations: vec![0],
        }
    }

    pub fn current_bind_depth(&self) -> BindDepth {
        self.bind_depth
    }

    pub fn enter_ctrl<T>(&mut self, f: impl FnOnce(&mut Self, ontol_hir::Variable) -> T) -> T {
        // There is a unique bind depth for the control flow variable:
        let ctrl_flow_var = self.alloc_variable();

        self.bind_depth.0 += 1;
        let ret = f(self, ctrl_flow_var);
        self.bind_depth.0 -= 1;

        ret
    }

    pub fn alloc_variable(&mut self) -> Variable {
        let alloc = self.ontos_var_allocations.get_mut(0).unwrap();
        let var = Variable(*alloc);
        *alloc += 1;
        var
    }
}

#[derive(Default)]
pub struct CtrlFlowBody<'m> {
    pub first: Option<ExprRoot<'m>>,
    pub second: Option<ExprRoot<'m>>,
}

pub struct ExprRoot<'m> {
    pub id: ExprId,
    pub expr: Expr,
    pub expected_ty: Option<TypeRef<'m>>,
}

pub struct ExplicitVariable {
    pub variable: Variable,
    pub ctrl_group: Option<CtrlFlowGroup>,
    pub ontos_arms: FnvHashMap<Arm, ExplicitVariableArm>,
}

pub struct VariableMapping<'m> {
    pub first_arm_type: TypeRef<'m>,
    pub second_arm_type: TypeRef<'m>,
}

pub struct ExplicitVariableArm {
    // In ontos, the variable has a different expr id depending on which arm it's in
    pub expr_id: ExprId,
    pub span: SourceSpan,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CtrlFlowGroup {
    pub label: Label,
    pub bind_depth: BindDepth,
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
