use fnv::FnvHashMap;

use crate::{
    expr::{Expr, ExprId},
    hir_node::{BindDepth, HirBodyIdx, HirIdx, HirNodeTable},
    types::TypeRef,
};

use super::inference::Inference;

pub struct CheckUnifyExprContext<'m> {
    pub inference: Inference<'m>,
    pub ontos_inference: Inference<'m>,
    pub bodies: Vec<CtrlFlowBody<'m>>,
    pub nodes: HirNodeTable<'m>,
    pub explicit_variables: FnvHashMap<ExprId, ExplicitVariable>,
    pub body_variables: FnvHashMap<HirBodyIdx, HirIdx>,
    pub body_map: FnvHashMap<ExprId, HirBodyIdx>,
    pub ontos_seq_labels: FnvHashMap<HirBodyIdx, ontos::Label>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub variable_mapping: FnvHashMap<ontos::Variable, VariableMapping<'m>>,

    pub partial: bool,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,
    bind_depth: BindDepth,
    ontos_var_allocations: Vec<u32>,
    body_id_counter: u32,
}

impl<'m> CheckUnifyExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            ontos_inference: Inference::new(),
            bodies: Default::default(),
            nodes: HirNodeTable::default(),
            explicit_variables: Default::default(),
            body_variables: Default::default(),
            body_map: Default::default(),
            ontos_seq_labels: Default::default(),
            ctrl_flow_forest: Default::default(),
            variable_mapping: Default::default(),
            partial: false,
            arm: Arm::First,
            bind_depth: BindDepth(0),
            ontos_var_allocations: vec![0],
            body_id_counter: 0,
        }
    }

    pub fn current_bind_depth(&self) -> BindDepth {
        self.bind_depth
    }

    pub fn enter_ctrl<T>(&mut self, f: impl FnOnce(&mut Self, ontos::Variable) -> T) -> T {
        // There is a unique bind depth for the control flow variable:
        let ctrl_flow_var = self.alloc_ontos_variable();

        self.bind_depth.0 += 1;
        let ret = f(self, ctrl_flow_var);
        self.bind_depth.0 -= 1;

        ret
    }

    pub fn alloc_hir_body_idx(&mut self) -> HirBodyIdx {
        let next = self.body_id_counter;
        self.body_id_counter += 1;
        self.bodies.push(CtrlFlowBody::default());
        HirBodyIdx(next)
    }

    pub fn expr_body_mut(&mut self, id: HirBodyIdx) -> &mut CtrlFlowBody<'m> {
        self.bodies.get_mut(id.0 as usize).unwrap()
    }

    pub fn alloc_ontos_variable(&mut self) -> ontos::Variable {
        let alloc = self.ontos_var_allocations.get_mut(0).unwrap();
        let var = ontos::Variable(*alloc);
        *alloc += 1;
        var
    }

    pub fn get_or_compute_seq_label(&mut self, body_idx: HirBodyIdx) -> ontos::Label {
        if let Some(label) = self.ontos_seq_labels.get(&body_idx) {
            return *label;
        }

        let label = ontos::Label(self.alloc_ontos_variable().0);
        self.ontos_seq_labels.insert(body_idx, label);
        label
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
    pub variable: ontos::Variable,
    pub node_id: HirIdx,
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
}

impl Default for ExplicitVariableArm {
    fn default() -> Self {
        Self { expr_id: ExprId(0) }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct CtrlFlowGroup {
    pub body_id: HirBodyIdx,
    pub bind_depth: BindDepth,
}

/// Tracks which control flow "statements" are children of other control flow "statements"
#[derive(Default)]
pub struct CtrlFlowForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<HirBodyIdx, HirBodyIdx>,
}

impl CtrlFlowForest {
    pub fn insert(&mut self, idx: HirBodyIdx, parent: Option<HirBodyIdx>) {
        self.map.insert(idx, parent.unwrap_or(idx));
    }

    pub fn find_parent(&self, idx: HirBodyIdx) -> Option<HirBodyIdx> {
        let parent = self.map.get(&idx).unwrap();
        if parent == &idx {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut idx: HirBodyIdx) -> HirBodyIdx {
        loop {
            let parent = self.map.get(&idx).unwrap();
            if parent == &idx {
                return idx;
            }
            idx = *parent;
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum Arm {
    First,
    Second,
}

impl Arm {
    pub fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }
}
