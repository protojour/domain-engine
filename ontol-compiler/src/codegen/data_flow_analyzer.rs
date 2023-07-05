//! High-level data flow of struct properties

use std::collections::BTreeSet;

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::Node;
use ontol_runtime::{
    env::{DataFlow, PropertyFlow, PropertyFlowRelationship},
    value::PropertyId,
};
use tracing::debug;

use crate::hir_unify::VarSet;

pub struct DataFlowAnalyzer {}

impl DataFlowAnalyzer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn analyze<L: ontol_hir::Lang>(
        &mut self,
        arg: ontol_hir::Binder,
        body: &L::Node<'_>,
    ) -> Option<DataFlow> {
        match body.kind() {
            ontol_hir::Kind::Struct(struct_binder, nodes) => {
                let mut struct_analyzer = StructAnalyzer {
                    scope: FnvHashMap::default(),
                    var_derivations: FnvHashMap::default(),
                    property_flow: BTreeSet::default(),
                };
                struct_analyzer
                    .var_derivations
                    .insert(arg.0, VarSet::default());
                struct_analyzer
                    .var_derivations
                    .insert(struct_binder.0, VarSet::default());
                for node in nodes {
                    struct_analyzer.analyze_node::<L>(node);
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
struct StructAnalyzer {
    scope: FnvHashMap<ontol_hir::Var, FnvHashSet<PropertyId>>,
    // var_deps_old: FnvHashMap<ontol_hir::Var, Option<ontol_hir::Var>>,
    var_derivations: FnvHashMap<ontol_hir::Var, VarSet>,
    property_flow: BTreeSet<PropertyFlow>,
}

impl StructAnalyzer {
    fn analyze_node<L: ontol_hir::Lang>(&mut self, node: &L::Node<'_>) -> VarSet {
        match node.kind() {
            ontol_hir::Kind::Var(var) => VarSet::from([*var]),
            ontol_hir::Kind::Unit => VarSet::default(),
            ontol_hir::Kind::Int(_) => VarSet::default(),
            ontol_hir::Kind::String(_) => VarSet::default(),
            ontol_hir::Kind::Let(binder, definition, body) => {
                let var_deps = self.analyze_node::<L>(definition);
                self.var_derivations.insert(binder.0, var_deps);

                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node::<L>(node));
                }

                var_set
            }
            ontol_hir::Kind::Call(_, params) => {
                let mut var_set = VarSet::default();
                for node in params {
                    var_set.union_with(&self.analyze_node::<L>(node));
                }
                var_set
            }
            ontol_hir::Kind::Map(node) => self.analyze_node::<L>(node),
            ontol_hir::Kind::Seq(_, _) => {
                todo!()
            }
            ontol_hir::Kind::Struct(_, body) => {
                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node::<L>(node));
                }
                var_set
            }
            ontol_hir::Kind::Prop(_, struct_var, property_id, variants) => {
                self.scope
                    .insert(*struct_var, FnvHashSet::from_iter([*property_id]));

                let mut var_set = VarSet::default();

                for variant in variants {
                    var_set.union_with(&self.analyze_node::<L>(&variant.attr.rel));
                    var_set.union_with(&self.analyze_node::<L>(&variant.attr.val));
                }

                // debug!("post analyze Prop({struct_var}, {property_id}) {self:#?}");
                // debug!("Prop {property_id} dependent on {var_set:?}");

                self.reg_output_prop(*struct_var, *property_id, var_set);

                Default::default()
            }
            ontol_hir::Kind::MatchProp(struct_var, property_id, arms) => {
                // debug!("analyze MatchProp({struct_var}, {property_id}) {self:#?}");

                self.scope
                    .insert(*struct_var, FnvHashSet::from_iter([*property_id]));

                // let deps: FnvHashSet<PropertyId> = if *struct_var == self.input_var {
                //     FnvHashSet::from_iter([*property_id])
                // } else {
                //     FnvHashSet::default()
                // };

                let mut var_set = VarSet::default();

                for arm in arms {
                    match &arm.pattern {
                        ontol_hir::PropPattern::Attr(rel, val) => {
                            self.handle_prop_binding(*struct_var, rel);
                            self.handle_prop_binding(*struct_var, val);
                        }
                        ontol_hir::PropPattern::Seq(binding) => {
                            self.handle_prop_binding(*struct_var, binding);
                        }
                        ontol_hir::PropPattern::Absent => {}
                    }

                    for node in &arm.nodes {
                        var_set.0.extend(&self.analyze_node::<L>(node).0);
                    }
                }

                self.reg_input_prop(*struct_var, *property_id);

                var_set
            }
            ontol_hir::Kind::Gen(..) => Default::default(),
            ontol_hir::Kind::Iter(..) => Default::default(),
            ontol_hir::Kind::Push(..) => Default::default(),
        }
    }

    fn handle_prop_binding(&mut self, struct_var: ontol_hir::Var, binding: &ontol_hir::Binding) {
        if let ontol_hir::Binding::Binder(var) = binding {
            self.var_derivations
                .entry(*var)
                .or_default()
                .insert(struct_var);
        }
    }

    fn reg_input_prop(&mut self, struct_var: ontol_hir::Var, property_id: PropertyId) {
        let derivations = self.var_derivations.get(&struct_var);

        match derivations {
            Some(derivations) if !derivations.0.is_empty() => {
                for derived_from in derivations {
                    debug!(
                        "reg input prop {struct_var} {property_id} - derived from {derived_from}"
                    );
                    if let Some(props) = self.scope.get(&derived_from) {
                        for prop in props {
                            self.property_flow.insert(PropertyFlow {
                                property: property_id,
                                relationship: PropertyFlowRelationship::ChildOf(*prop),
                            });
                        }
                    }
                }
            }
            _ => {
                debug!("reg input prop {struct_var} {property_id} - not derived");
                self.property_flow.insert(PropertyFlow {
                    property: property_id,
                    relationship: PropertyFlowRelationship::None,
                });
            }
        }
    }

    fn reg_output_prop(
        &mut self,
        _struct_var: ontol_hir::Var,
        property_id: PropertyId,
        var_dependencies: VarSet,
    ) {
        for var in &var_dependencies {
            if let Some(deps) = self.scope.get(&var) {
                for dep in deps {
                    self.property_flow.insert(PropertyFlow {
                        property: property_id,
                        relationship: PropertyFlowRelationship::DependentOn(*dep),
                    });
                }
            } else if let Some(parent_vars) = self.var_derivations.get(&var) {
                for parent_var in parent_vars {
                    if let Some(deps) = self.scope.get(&parent_var) {
                        for dep in deps {
                            self.property_flow.insert(PropertyFlow {
                                property: property_id,
                                relationship: PropertyFlowRelationship::DependentOn(*dep),
                            });
                        }
                    }
                }
            }
        }
    }
}
