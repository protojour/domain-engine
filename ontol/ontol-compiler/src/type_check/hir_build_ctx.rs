use fnv::FnvHashMap;
use ontol_runtime::var::{Var, VarAllocator};
use smartstring::alias::String;
use tracing::debug_span;

use crate::{
    pattern::PatId,
    typed_hir::{self, Meta, TypedHir, TypedHirData},
    types::TypeRef,
    SourceSpan, NO_SPAN,
};

use super::ena_inference::Inference;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct CtrlFlowDepth(pub u16);

pub struct HirBuildCtx<'m> {
    /// The ontol-hir arena that is getting built
    pub hir_arena: ontol_hir::arena::Arena<'m, TypedHir>,
    pub map_kw_span: SourceSpan,
    pub inference: Inference<'m>,
    pub pattern_variables: FnvHashMap<Var, PatternVariable>,
    pub label_map: FnvHashMap<PatId, ontol_hir::Label>,

    pub ctrl_flow_forest: CtrlFlowForest,

    pub variable_mapping: FnvHashMap<Var, VariableMapping<'m>>,

    /// Used for implicit edge/rel param mapping.
    /// Given an object variable, get its corresponding edge variable
    pub object_to_edge_var_table: FnvHashMap<Var, Var>,

    pub partial: bool,

    pub var_allocator: VarAllocator,

    /// Which Arm is currently processed in a map statement:
    pub current_arm: Arm,

    ctrl_flow_depth: CtrlFlowDepth,

    pub missing_properties: FnvHashMap<Arm, FnvHashMap<SourceSpan, Vec<String>>>,
}

impl<'m> HirBuildCtx<'m> {
    pub fn new(map_kw_span: SourceSpan, var_allocator: VarAllocator) -> Self {
        Self {
            hir_arena: Default::default(),
            map_kw_span,
            inference: Inference::new(),
            pattern_variables: Default::default(),
            label_map: Default::default(),
            ctrl_flow_forest: Default::default(),
            variable_mapping: Default::default(),
            object_to_edge_var_table: Default::default(),
            partial: false,
            current_arm: Arm::First,
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

    pub fn mk_node(
        &mut self,
        kind: ontol_hir::Kind<'m, TypedHir>,
        meta: typed_hir::Meta<'m>,
    ) -> ontol_hir::Node {
        self.hir_arena.add(TypedHirData(kind, meta))
    }

    pub fn mk_unit_node_no_span(&mut self) -> ontol_hir::Node {
        self.hir_arena
            .add(TypedHirData(ontol_hir::Kind::Unit, Meta::unit(NO_SPAN)))
    }
}

pub struct PatternVariable {
    pub set_element_group: Option<SetElementGroup>,
    pub hir_arms: FnvHashMap<Arm, ExplicitVariableArm>,
}

pub enum VariableMapping<'m> {
    /// A mapping from the type in the first arm to the type in the second arm
    Mapping([TypeRef<'m>; 2]),
    /// Just write the type into the ontol-hir metadata, don't perform explicit map
    Overwrite(TypeRef<'m>),
}

pub struct ExplicitVariableArm {
    // In hir, the variable has a different pat id depending on which arm it's in
    pub pat_id: PatId,
    pub span: SourceSpan,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SetElementGroup {
    pub label: ontol_hir::Label,
    pub iterated: bool,
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

pub const ARMS: [Arm; 2] = [Arm::First, Arm::Second];

impl Arm {
    pub fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }

    pub fn tracing_debug_span(&self) -> tracing::Span {
        match self {
            Self::First => debug_span!("1st"),
            Self::Second => debug_span!("2nd"),
        }
    }
}
