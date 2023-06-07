use std::fmt::Debug;

use bit_set::BitSet;
use fnv::FnvHashMap;
use ontol_hir::{kind, visitor::HirVisitor, Node, Variable};
use ontol_runtime::value::PropertyId;
use smallvec::SmallVec;
use tracing::debug;

use crate::typed_hir::{TypedHir, TypedHirNode};

use super::UnifierError;

#[derive(Clone, Copy)]
pub enum NodeRef<'s, 'm> {
    Node(&'s TypedHirNode<'m>),
    Variant(&'s kind::PropVariant<'m, TypedHir>),
    MatchArm(&'s kind::MatchArm<'m, TypedHir>),
    PatternBinding(kind::PatternBinding),
}

#[derive(Clone, Copy)]
pub struct ChildRef<'s, 'm> {
    parent: NodeRef<'s, 'm>,
    child: usize,
}

pub type NodePath<'s, 'm> = SmallVec<[ChildRef<'s, 'm>; 16]>;

pub type Path = SmallVec<[u16; 32]>;

#[derive(Clone)]
pub struct VarPath<'s, 'm> {
    pub root: &'s TypedHirNode<'m>,
    pub path: Path,
}

impl<'s, 'm> Debug for VarPath<'s, 'm> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.path.fmt(f)
    }
}

pub fn locate_variables<'s, 'm, 'b>(
    node: &'s TypedHirNode<'m>,
    variables: &'b BitSet,
) -> Result<FnvHashMap<Variable, VarPath<'s, 'm>>, UnifierError> {
    let mut locator = VarLocator::new(node, variables);
    locator.traverse_kind(node.kind());
    locator.var_paths()
}

pub fn variable_node_paths<'s, 'm, 'b>(
    node: &'s TypedHirNode<'m>,
    variables: &'b BitSet,
) -> Result<FnvHashMap<Variable, NodePath<'s, 'm>>, UnifierError> {
    let mut locator = VarLocator::new(node, variables);
    locator.traverse_kind(node.kind());
    locator.node_paths()
}

struct VarLocator<'s, 'm, 'b> {
    root: &'s TypedHirNode<'m>,
    parent: NodeRef<'s, 'm>,
    variables: &'b BitSet,
    current_path: Path,
    current_node_path: NodePath<'s, 'm>,

    duplicates: BitSet,
    var_paths: FnvHashMap<Variable, VarPath<'s, 'm>>,
    node_paths: FnvHashMap<Variable, NodePath<'s, 'm>>,
}

impl<'s, 'm, 'b> VarLocator<'s, 'm, 'b> {
    fn var_paths(self) -> Result<FnvHashMap<Variable, VarPath<'s, 'm>>, UnifierError> {
        if !self.duplicates.is_empty() {
            Err(UnifierError::NonUniqueVariableDatapoints(self.duplicates))
        } else {
            Ok(self.var_paths)
        }
    }

    fn node_paths(self) -> Result<FnvHashMap<Variable, NodePath<'s, 'm>>, UnifierError> {
        if !self.duplicates.is_empty() {
            Err(UnifierError::NonUniqueVariableDatapoints(self.duplicates))
        } else {
            Ok(self.node_paths)
        }
    }

    pub(super) fn new(root: &'s TypedHirNode<'m>, variables: &'b BitSet) -> Self {
        Self {
            root,
            parent: NodeRef::Node(root),
            variables,
            current_path: Path::default(),
            current_node_path: NodePath::default(),
            duplicates: BitSet::new(),
            var_paths: FnvHashMap::default(),
            node_paths: FnvHashMap::default(),
        }
    }

    fn enter_child(
        &mut self,
        index: usize,
        mut node: NodeRef<'s, 'm>,
        func: impl FnOnce(&mut Self),
    ) {
        self.current_node_path.push(ChildRef {
            parent: self.parent,
            child: index,
        });
        self.current_path.push(index as u16);
        std::mem::swap(&mut self.parent, &mut node);

        func(self);

        std::mem::swap(&mut node, &mut self.parent);
        self.current_path.pop();
        self.current_node_path.pop();
    }

    fn register_var(&mut self, var: u32) {
        if self.variables.contains(var as usize)
            && self
                .var_paths
                .insert(
                    Variable(var),
                    VarPath {
                        root: self.root,
                        path: self.current_path.clone(),
                    },
                )
                .is_some()
        {
            self.duplicates.insert(var as usize);
        }

        if self.variables.contains(var as usize)
            && self
                .node_paths
                .insert(Variable(var), self.current_node_path.clone())
                .is_some()
        {
            self.duplicates.insert(var as usize);
        }
    }
}

impl<'s, 'm, 'b> HirVisitor<'s, 'm, TypedHir> for VarLocator<'s, 'm, 'b> {
    fn visit_node(&mut self, index: usize, node: &'s TypedHirNode<'m>) {
        self.enter_child(index, NodeRef::Node(node), |zelf| {
            zelf.visit_kind(index, node.kind())
        });
    }

    fn visit_variable(&mut self, variable: &Variable) {
        self.register_var(variable.0);
    }

    fn visit_label(&mut self, label: &ontol_hir::Label) {
        self.register_var(label.0);
    }

    fn visit_prop(
        &mut self,
        optional: &'s kind::Optional,
        struct_var: &'s Variable,
        id: &'s PropertyId,
        variants: &'s Vec<kind::PropVariant<'m, TypedHir>>,
    ) {
        self.traverse_prop(struct_var, id, variants);
    }

    fn visit_prop_variant(&mut self, index: usize, variant: &'s kind::PropVariant<'m, TypedHir>) {
        self.enter_child(index, NodeRef::Variant(variant), |zelf| {
            let kind::PropVariant { dimension, .. } = variant;
            match dimension {
                kind::Dimension::Singular => {
                    zelf.traverse_prop_variant(variant);
                }
                kind::Dimension::Seq(label) => {
                    // The search stops here for now, sequence mappings are black boxes,
                    // only register the label:
                    zelf.visit_label(label);
                }
            }
        });
    }

    fn visit_match_arm(
        &mut self,
        index: usize,
        match_arm: &'s ontol_hir::kind::MatchArm<'m, TypedHir>,
    ) {
        self.enter_child(index, NodeRef::MatchArm(match_arm), |zelf| {
            zelf.traverse_match_arm(match_arm)
        });
    }

    fn visit_pattern_binding(
        &mut self,
        index: usize,
        binding: &'s ontol_hir::kind::PatternBinding,
    ) {
        self.enter_child(index, NodeRef::PatternBinding(*binding), |zelf| {
            zelf.traverse_pattern_binding(binding)
        });
    }
}

// TODO: No cloning?
pub fn full_var_path<'s, 'm>(
    free_variables: &BitSet,
    variable_paths: &FnvHashMap<ontol_hir::Variable, VarPath<'s, 'm>>,
) -> PathsIterator<'s, 'm> {
    PathsIterator::new(
        free_variables
            .iter()
            .filter_map(|var_index| variable_paths.get(&ontol_hir::Variable(var_index as u32)))
            .cloned()
            .collect(),
    )
}

pub struct PathsIterator<'s, 'm> {
    full_var_path: Vec<VarPath<'s, 'm>>,
    outer_index: usize,
    inner_index: usize,
}

pub enum PathSegment<'s, 'm> {
    Sub(usize),
    Root(&'s TypedHirNode<'m>, usize),
}

impl<'s, 'm> PathsIterator<'s, 'm> {
    pub fn new(full_var_path: Vec<VarPath<'s, 'm>>) -> Self {
        Self {
            full_var_path,
            outer_index: 0,
            inner_index: 0,
        }
    }
}

impl<'s, 'm> Iterator for PathsIterator<'s, 'm> {
    type Item = PathSegment<'s, 'm>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.outer_index >= self.full_var_path.len() {
                return None;
            } else {
                let var_path = &self.full_var_path[self.outer_index];
                let inner_index = self.inner_index;

                if inner_index >= var_path.path.len() {
                    self.outer_index += 1;
                    self.inner_index = 0;
                } else {
                    self.inner_index += 1;
                    let segment = var_path.path[inner_index];

                    return Some(if inner_index == 0 {
                        debug!("next path segment: Root({segment})");
                        PathSegment::Root(var_path.root, segment as usize)
                    } else {
                        debug!("next path segment: Sub({segment})");
                        PathSegment::Sub(segment as usize)
                    });
                }
            }
        }
    }
}
