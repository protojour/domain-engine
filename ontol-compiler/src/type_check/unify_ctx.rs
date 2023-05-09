use fnv::FnvHashMap;

use crate::{
    expr::{Expr, ExprId},
    hir_node::{BindDepth, HirBodyIdx, HirIdx, HirNodeTable, HirVariable},
    types::TypeRef,
};

use super::inference::Inference;

pub struct UnifyExprContext<'m> {
    pub inference: Inference<'m>,
    pub bodies: Vec<CtrlFlowBody<'m>>,
    pub nodes: HirNodeTable<'m>,
    pub bound_variables: FnvHashMap<ExprId, BoundVariable>,
    pub body_variables: FnvHashMap<HirBodyIdx, HirIdx>,
    pub body_map: FnvHashMap<ExprId, HirBodyIdx>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub partial: bool,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,
    bind_depth: BindDepth,
    hir_var_allocations: Vec<u16>,
    body_id_counter: u32,
}

impl<'m> UnifyExprContext<'m> {
    pub fn new() -> Self {
        Self {
            inference: Inference::new(),
            bodies: Default::default(),
            nodes: HirNodeTable::default(),
            bound_variables: Default::default(),
            body_variables: Default::default(),
            body_map: Default::default(),
            ctrl_flow_forest: Default::default(),
            partial: false,
            arm: Arm::First,
            bind_depth: BindDepth(0),
            hir_var_allocations: vec![0],
            body_id_counter: 0,
        }
    }

    pub fn current_bind_depth(&self) -> BindDepth {
        self.bind_depth
    }

    pub fn enter_ctrl<T>(&mut self, f: impl FnOnce(&mut Self, HirVariable) -> T) -> T {
        // There is a unique bind depth for the control flow variable:
        let ctrl_flow_var = self.alloc_hir_variable();

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

    pub fn alloc_hir_variable(&mut self) -> HirVariable {
        let alloc = self.hir_var_allocations.get_mut(0).unwrap();
        let hir_var = HirVariable(*alloc);
        *alloc += 1;
        hir_var
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

pub struct BoundVariable {
    pub syntax_var: HirVariable,
    pub node_id: HirIdx,
    pub ctrl_group: Option<CtrlFlowGroup>,
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

#[derive(Clone, Copy)]
pub enum Arm {
    First,
    Second,
}

impl Arm {
    pub fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }
}
