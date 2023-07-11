//! High-level data flow of struct properties

use std::{collections::BTreeSet, fmt::Debug};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::GetKind;
use ontol_runtime::{
    env::{PropertyFlow, PropertyFlowData},
    value::PropertyId,
    DefId,
};

use crate::{
    def::LookupRelationshipMeta,
    hir_unify::VarSet,
    typed_hir::TypedHirNode,
    types::{Type, TypeRef},
};

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
            ontol_hir::Kind::Struct(struct_binder, nodes) => {
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
            ontol_hir::Kind::Int(_) => VarSet::default(),
            ontol_hir::Kind::String(_) => VarSet::default(),
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

                let mut property_type = None;

                for arm in arms {
                    match &arm.pattern {
                        ontol_hir::PropPattern::Attr(rel, val) => {
                            if let ontol_hir::Binding::Binder(binder) = rel {
                                self.add_dep(binder.var, *struct_var);
                            }
                            if let ontol_hir::Binding::Binder(binder) = val {
                                self.add_dep(binder.var, *struct_var);
                                property_type = Some(binder.ty);
                            }
                        }
                        ontol_hir::PropPattern::Seq(binding, _has_default) => {
                            if let ontol_hir::Binding::Binder(binder) = binding {
                                self.add_dep(binder.var, *struct_var);
                                if !matches!(binder.ty, Type::Seq(..)) {
                                    panic!("Sequence binder was not seq but {:?}", binder.ty);
                                }
                                property_type = Some(binder.ty);
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
                    property_type.unwrap_or_else(|| {
                        panic!("No type found for {property_id}");
                        node.meta().ty
                    }),
                );

                var_set
            }
            ontol_hir::Kind::Gen(var, iter_binder, body) => {
                if let ontol_hir::Binding::Binder(binder) = iter_binder.seq {
                    self.add_dep(binder.var, *var);
                }
                if let ontol_hir::Binding::Binder(binder) = iter_binder.rel {
                    self.add_dep(binder.var, *var);
                }
                if let ontol_hir::Binding::Binder(binder) = iter_binder.val {
                    self.add_dep(binder.var, *var);
                }
                let mut var_set = VarSet::default();
                for node in body {
                    var_set.union_with(&self.analyze_node(node));
                }
                var_set
            }
            ontol_hir::Kind::Iter(..) => todo!(),
            ontol_hir::Kind::Push(var, attr) => {
                let mut var_set = self.analyze_node(&attr.rel);
                var_set.union_with(&self.analyze_node(&attr.val));

                self.var_dependencies.insert(*var, var_set.clone());

                var_set
            }
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

    fn reg_scope_prop(&mut self, struct_var: ontol_hir::Var, property_id: PropertyId, ty: TypeRef) {
        let def_id = ty.get_single_def_id().unwrap_or(DefId::unit());

        let meta = self
            .defs
            .lookup_relationship_meta(property_id.relationship_id)
            .unwrap();
        let (_, _, cardinality) = meta.relationship.left_side(property_id.role);

        self.property_flow.insert(PropertyFlow {
            id: property_id,
            data: PropertyFlowData::Type(def_id),
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
