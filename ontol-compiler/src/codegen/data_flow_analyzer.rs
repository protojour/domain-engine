//! High-level data flow of struct properties

use std::{collections::BTreeSet, fmt::Debug};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::{GetKind, PropVariant, SeqPropertyVariant};
use ontol_runtime::{
    ontology::{PropertyFlow, PropertyFlowData},
    value::PropertyId,
    DefId,
};

use crate::{def::LookupRelationshipMeta, hir_unify::VarSet, typed_hir::TypedHirNode, types::Type};

pub struct DataFlowAnalyzer<'c, R> {
    defs: &'c R,
    /// A table of which variable produce which properties
    var_to_property: FnvHashMap<ontol_hir::Var, FnvHashSet<PropertyId>>,
    /// A mapping from variable to its dependencies
    var_dependencies: FnvHashMap<ontol_hir::Var, VarSet>,
    property_flow: BTreeSet<PropertyFlow>,
}

impl<'c, R> Debug for DataFlowAnalyzer<'c, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFlowAnalyzer")
            .field("var_to_property", &self.var_to_property)
            .field("var_dependencies", &self.var_dependencies)
            .finish()
    }
}

impl<'c, 'm, R> DataFlowAnalyzer<'c, R>
where
    R: LookupRelationshipMeta<'m>,
{
    pub fn new(defs: &'c R) -> Self {
        Self {
            defs,
            var_to_property: FnvHashMap::default(),
            var_dependencies: FnvHashMap::default(),
            property_flow: BTreeSet::default(),
        }
    }

    pub fn analyze(
        &mut self,
        arg: ontol_hir::Var,
        body: &TypedHirNode,
    ) -> Option<Vec<PropertyFlow>> {
        match body.kind() {
            ontol_hir::Kind::Struct(struct_binder, _flags, nodes) => {
                self.var_dependencies.insert(arg, VarSet::default());
                self.var_dependencies
                    .insert(struct_binder.var, VarSet::default());
                for node in nodes {
                    self.analyze_node(node);
                }

                Some(
                    std::mem::take(&mut self.property_flow)
                        .into_iter()
                        .collect(),
                )
            }
            _ => None,
        }
    }

    fn analyze_node(&mut self, node: &TypedHirNode) -> VarSet {
        match node.kind() {
            ontol_hir::Kind::Var(var) => VarSet::from([*var]),
            ontol_hir::Kind::Unit => VarSet::default(),
            ontol_hir::Kind::I64(_) => VarSet::default(),
            ontol_hir::Kind::F64(_) => VarSet::default(),
            ontol_hir::Kind::String(_) => VarSet::default(),
            ontol_hir::Kind::Const(_) => VarSet::default(),
            ontol_hir::Kind::Let(binder, definition, body) => {
                let var_deps = self.analyze_node(definition);
                self.var_dependencies.insert(binder.var, var_deps);

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
            ontol_hir::Kind::DeclSeq(_, _) => {
                unreachable!()
            }
            ontol_hir::Kind::Struct(_, _, body) => {
                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node(node));
                }
                var_set
            }
            ontol_hir::Kind::Prop(_, struct_var, prop_id, variants) => {
                self.var_to_property
                    .insert(*struct_var, FnvHashSet::from_iter([*prop_id]));

                let mut var_set = VarSet::default();

                for variant in variants {
                    match variant {
                        PropVariant::Singleton(attr) => {
                            var_set.union_with(&self.analyze_node(&attr.rel));
                            var_set.union_with(&self.analyze_node(&attr.val));
                        }
                        PropVariant::Seq(SeqPropertyVariant { elements, .. }) => {
                            for (_iter, attr) in elements {
                                var_set.union_with(&self.analyze_node(&attr.rel));
                                var_set.union_with(&self.analyze_node(&attr.val));
                            }
                        }
                    }
                }

                self.reg_output_prop(*struct_var, *prop_id, var_set);

                Default::default()
            }
            ontol_hir::Kind::MatchProp(struct_var, property_id, arms) => {
                self.var_to_property
                    .insert(*struct_var, FnvHashSet::from_iter([*property_id]));

                let mut var_set = VarSet::default();

                let mut value_def_id = None;

                for arm in arms {
                    match &arm.pattern {
                        ontol_hir::PropPattern::Attr(rel, val) => {
                            if let ontol_hir::Binding::Binder(binder) = rel {
                                self.add_dep(binder.var, *struct_var);
                            }
                            if let ontol_hir::Binding::Binder(binder) = val {
                                self.add_dep(binder.var, *struct_var);
                                value_def_id = binder.meta.ty.get_single_def_id();
                            }
                        }
                        ontol_hir::PropPattern::Seq(binding, _has_default) => {
                            if let ontol_hir::Binding::Binder(binder) = binding {
                                self.add_dep(binder.var, *struct_var);
                                value_def_id = match binder.meta.ty {
                                    Type::Seq(_, val) => val.get_single_def_id(),
                                    _ => None,
                                };
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
                    value_def_id.unwrap_or(DefId::unit()),
                );

                var_set
            }
            ontol_hir::Kind::Sequence(_, body) => {
                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node(node));
                }
                var_set
            }
            ontol_hir::Kind::ForEach(var, (rel_binding, val_binding), body) => {
                if let ontol_hir::Binding::Binder(binder) = rel_binding {
                    self.add_dep(binder.var, *var);
                }
                if let ontol_hir::Binding::Binder(binder) = val_binding {
                    self.add_dep(binder.var, *var);
                }
                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node(node));
                }
                var_set
            }
            ontol_hir::Kind::SeqPush(var, attr) => {
                let mut var_set = self.analyze_node(&attr.rel);
                var_set.union_with(&self.analyze_node(&attr.val));

                self.var_dependencies.insert(*var, var_set.clone());

                var_set
            }
            ontol_hir::Kind::StringPush(to_var, node) => {
                let var_set = self.analyze_node(node);
                self.var_dependencies.insert(*to_var, var_set.clone());
                var_set
            }
            ontol_hir::Kind::MatchRegex(var, _, match_arms) => {
                let mut var_set = VarSet::default();
                for match_arm in match_arms {
                    for group in &match_arm.capture_groups {
                        self.add_dep(group.binder.var, *var);
                    }
                    for node in &match_arm.nodes {
                        var_set.union_with(&self.analyze_node(node));
                    }
                }
                var_set
            }
            ontol_hir::Kind::Regex(..) => VarSet::default(),
        }
    }

    fn add_dep(&mut self, var: ontol_hir::Var, dep: ontol_hir::Var) {
        self.var_dependencies.entry(var).or_default().insert(dep);
    }

    fn reg_output_prop(
        &mut self,
        _struct_var: ontol_hir::Var,
        property_id: PropertyId,
        mut var_dependencies: VarSet,
    ) {
        while !var_dependencies.0.is_empty() {
            let mut next_deps = VarSet::default();

            for var in &var_dependencies {
                if let Some(deps) = self.var_to_property.get(&var) {
                    for dep in deps {
                        self.property_flow.insert(PropertyFlow {
                            id: property_id,
                            data: PropertyFlowData::DependentOn(*dep),
                        });
                    }
                } else if let Some(parent_vars) = self.var_dependencies.get(&var) {
                    next_deps.union_with(parent_vars);
                }
            }

            var_dependencies = next_deps;
        }
    }

    /// The purpose of the value_def_id is for the query engine
    /// to understand which entity must be looked up.
    /// rel_params is ignored here.
    fn reg_scope_prop(
        &mut self,
        struct_var: ontol_hir::Var,
        property_id: PropertyId,
        value_def_id: DefId,
    ) {
        let meta = self.defs.relationship_meta(property_id.relationship_id);
        let (_, cardinality, _) = meta.relationship.by(property_id.role);

        self.property_flow.insert(PropertyFlow {
            id: property_id,
            data: PropertyFlowData::Type(value_def_id),
        });
        self.property_flow.insert(PropertyFlow {
            id: property_id,
            data: PropertyFlowData::Cardinality(cardinality),
        });

        register_children_recursive(
            struct_var,
            property_id,
            &self.var_dependencies,
            &self.var_to_property,
            &mut self.property_flow,
        );
    }
}

fn register_children_recursive(
    var: ontol_hir::Var,
    property_id: PropertyId,
    var_dependencies: &FnvHashMap<ontol_hir::Var, VarSet>,
    var_to_property: &FnvHashMap<ontol_hir::Var, FnvHashSet<PropertyId>>,
    output: &mut BTreeSet<PropertyFlow>,
) {
    if let Some(dependencies) = var_dependencies.get(&var) {
        for var_dependency in dependencies {
            if let Some(props) = var_to_property.get(&var_dependency) {
                // recursion stops here, as there is a property associated with the variable:
                for prop in props {
                    output.insert(PropertyFlow {
                        id: property_id,
                        data: PropertyFlowData::ChildOf(*prop),
                    });
                }
            } else {
                register_children_recursive(
                    var_dependency,
                    property_id,
                    var_dependencies,
                    var_to_property,
                    output,
                );
            }
        }
    }
}
