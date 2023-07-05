//! High-level data flow of struct properties

use std::collections::BTreeSet;

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::Node;
use ontol_runtime::{
    env::{DataFlow, PropertyFlow, PropertyFlowData},
    value::PropertyId,
    DefId,
};
use tracing::info;

use crate::{hir_unify::VarSet, typed_hir::TypedHirNode, types::TypeRef};

pub struct DataFlowAnalyzer;

impl DataFlowAnalyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn analyze(&mut self, arg: ontol_hir::Binder, body: &TypedHirNode) -> Option<DataFlow> {
        match body.kind() {
            ontol_hir::Kind::Struct(struct_binder, nodes) => {
                let mut struct_analyzer = StructAnalyzer {
                    var_to_property: FnvHashMap::default(),
                    var_dependencies: FnvHashMap::default(),
                    var_types: FnvHashMap::default(),
                    property_flow: BTreeSet::default(),
                };
                struct_analyzer
                    .var_dependencies
                    .insert(arg.0, VarSet::default());
                struct_analyzer
                    .var_dependencies
                    .insert(struct_binder.0, VarSet::default());
                for node in nodes {
                    struct_analyzer.analyze_node(node);
                }

                Some(DataFlow {
                    properties: struct_analyzer.property_flow.into_iter().collect(),
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
struct StructAnalyzer<'m> {
    /// A table of which variable produce which properties
    var_to_property: FnvHashMap<ontol_hir::Var, FnvHashSet<PropertyId>>,
    /// A mapping from variable to its dependencies
    var_dependencies: FnvHashMap<ontol_hir::Var, VarSet>,
    var_types: FnvHashMap<ontol_hir::Var, TypeRef<'m>>,
    property_flow: BTreeSet<PropertyFlow>,
}

impl<'m> StructAnalyzer<'m> {
    fn analyze_node(&mut self, node: &TypedHirNode<'m>) -> VarSet {
        match node.kind() {
            ontol_hir::Kind::Var(var) => {
                self.var_types.insert(*var, node.meta().ty);
                VarSet::from([*var])
            }
            ontol_hir::Kind::Unit => VarSet::default(),
            ontol_hir::Kind::Int(_) => VarSet::default(),
            ontol_hir::Kind::String(_) => VarSet::default(),
            ontol_hir::Kind::Let(binder, definition, body) => {
                self.var_types.insert(binder.0, definition.meta().ty);

                let var_deps = self.analyze_node(definition);
                self.var_dependencies.insert(binder.0, var_deps);

                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node(node));
                }

                var_set
            }
            ontol_hir::Kind::Call(_, params) => {
                let mut var_set = VarSet::default();
                for node in params {
                    var_set.union_with(&self.analyze_node(node));
                }
                var_set
            }
            ontol_hir::Kind::Map(node) => self.analyze_node(node),
            ontol_hir::Kind::Seq(_, _) => {
                todo!()
            }
            ontol_hir::Kind::Struct(_, body) => {
                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node(node));
                }
                var_set
            }
            ontol_hir::Kind::Prop(_, struct_var, property_id, variants) => {
                self.var_to_property
                    .insert(*struct_var, FnvHashSet::from_iter([*property_id]));

                let mut var_set = VarSet::default();

                for variant in variants {
                    var_set.union_with(&self.analyze_node(&variant.attr.rel));
                    var_set.union_with(&self.analyze_node(&variant.attr.val));
                }

                self.reg_output_prop(*struct_var, *property_id, var_set);

                Default::default()
            }
            ontol_hir::Kind::MatchProp(struct_var, property_id, arms) => {
                self.var_to_property
                    .insert(*struct_var, FnvHashSet::from_iter([*property_id]));

                let mut var_set = VarSet::default();

                let mut type_var = None;

                for arm in arms {
                    match &arm.pattern {
                        ontol_hir::PropPattern::Attr(rel, val) => {
                            if let ontol_hir::Binding::Binder(var) = rel {
                                self.add_dep(*var, *struct_var);
                            }
                            if let ontol_hir::Binding::Binder(var) = val {
                                self.add_dep(*var, *struct_var);
                                type_var = Some(var);
                            }
                        }
                        ontol_hir::PropPattern::Seq(binding) => {
                            if let ontol_hir::Binding::Binder(var) = binding {
                                self.add_dep(*var, *struct_var);
                            }
                        }
                        ontol_hir::PropPattern::Absent => {}
                    }

                    for node in &arm.nodes {
                        var_set.0.extend(&self.analyze_node(node).0);
                    }
                }

                self.reg_scope_prop(
                    *struct_var,
                    *property_id,
                    type_var
                        .and_then(|var| {
                            let ty = self.var_types.get(var).copied();
                            info!("Type for {var}: {ty:?}");

                            ty
                        })
                        .unwrap_or(node.meta().ty),
                );

                var_set
            }
            ontol_hir::Kind::Gen(..) => Default::default(),
            ontol_hir::Kind::Iter(..) => Default::default(),
            ontol_hir::Kind::Push(..) => Default::default(),
        }
    }

    fn add_dep(&mut self, var: ontol_hir::Var, dep: ontol_hir::Var) {
        self.var_dependencies.entry(var).or_default().insert(dep);
    }

    fn reg_output_prop(
        &mut self,
        _struct_var: ontol_hir::Var,
        property_id: PropertyId,
        var_dependencies: VarSet,
    ) {
        for var in &var_dependencies {
            if let Some(deps) = self.var_to_property.get(&var) {
                for dep in deps {
                    self.property_flow.insert(PropertyFlow {
                        id: property_id,
                        data: PropertyFlowData::DependentOn(*dep),
                    });
                }
            } else if let Some(parent_vars) = self.var_dependencies.get(&var) {
                for parent_var in parent_vars {
                    if let Some(deps) = self.var_to_property.get(&parent_var) {
                        for dep in deps {
                            self.property_flow.insert(PropertyFlow {
                                id: property_id,
                                data: PropertyFlowData::DependentOn(*dep),
                            });
                        }
                    }
                }
            }
        }
    }

    fn reg_scope_prop(
        &mut self,
        struct_var: ontol_hir::Var,
        property_id: PropertyId,
        ty: TypeRef<'m>,
    ) {
        let def_id = ty.get_single_def_id().unwrap_or(DefId::unit());

        self.property_flow.insert(PropertyFlow {
            id: property_id,
            data: PropertyFlowData::Type(def_id),
        });

        if let Some(dependencies) = self.var_dependencies.get(&struct_var) {
            for var_dependency in dependencies {
                if let Some(props) = self.var_to_property.get(&var_dependency) {
                    for prop in props {
                        self.property_flow.insert(PropertyFlow {
                            id: property_id,
                            data: PropertyFlowData::ChildOf(*prop),
                        });
                    }
                }
            }
        }
    }
}
