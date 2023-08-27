use fnv::FnvHashMap;
use smartstring::alias::String;

use crate::{expr::ExprId, types::TypeRef, SourceSpan};

use super::inference::Inference;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct CtrlFlowDepth(pub u16);

pub struct HirBuildCtx<'m> {
    pub map_kw_span: SourceSpan,
    pub inference: Inference<'m>,
    pub expr_variables: FnvHashMap<ontol_hir::Var, ExpressionVariable>,
    pub label_map: FnvHashMap<ExprId, ontol_hir::Label>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub variable_mapping: FnvHashMap<ontol_hir::Var, VariableMapping<'m>>,

    /// Used for implicit edge/rel param mapping.
    /// Given an object variable, get its corresponding edge variable
    pub object_to_edge_var_table: FnvHashMap<ontol_hir::Var, ontol_hir::Var>,

    pub partial: bool,

    pub var_allocator: ontol_hir::VarAllocator,

    /// Which Arm is currently processed in a map statement:
    pub arm: Arm,

    ctrl_flow_depth: CtrlFlowDepth,

    pub missing_properties: FnvHashMap<Arm, FnvHashMap<SourceSpan, Vec<String>>>,
}

impl<'m> HirBuildCtx<'m> {
    pub fn new(map_kw_span: SourceSpan, var_allocator: ontol_hir::VarAllocator) -> Self {
        Self {
            map_kw_span,
            inference: Inference::new(),
            expr_variables: Default::default(),
            label_map: Default::default(),
            ctrl_flow_forest: Default::default(),
            variable_mapping: Default::default(),
            object_to_edge_var_table: Default::default(),
            partial: false,
            arm: Arm::First,
            ctrl_flow_depth: CtrlFlowDepth(0),
            var_allocator,
            missing_properties: FnvHashMap::default(),
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
}

pub struct ExpressionVariable {
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
    pub label: ontol_hir::Label,
    pub bind_depth: CtrlFlowDepth,
}

/// Tracks which control flow "statements" are children of other control flow "statements"
#[derive(Default)]
pub struct CtrlFlowForest {
    /// if the map is self-referential, that means a root
    map: FnvHashMap<ontol_hir::Label, ontol_hir::Label>,
}

impl CtrlFlowForest {
    pub fn insert(&mut self, label: ontol_hir::Label, parent_label: Option<ontol_hir::Label>) {
        self.map.insert(label, parent_label.unwrap_or(label));
    }

    pub fn find_parent(&self, label: ontol_hir::Label) -> Option<ontol_hir::Label> {
        let parent = self.map.get(&label).unwrap();
        if parent == &label {
            None
        } else {
            Some(*parent)
        }
    }

    pub fn find_root(&self, mut label: ontol_hir::Label) -> ontol_hir::Label {
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
