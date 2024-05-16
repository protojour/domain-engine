//! High-level data flow of struct properties

use std::{collections::BTreeSet, fmt::Debug};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::{PropVariant, StructFlags};
use ontol_runtime::{
    ontology::map::{PropertyFlow, PropertyFlowData},
    property::{PropertyId, Role},
    value::Attribute,
    var::{Var, VarSet},
    DefId, RelationshipId,
};

use crate::{
    def::LookupRelationshipMeta,
    typed_hir::TypedNodeRef,
    types::{Type, UNIT_TYPE},
};

pub struct DataFlowAnalyzer<'c, R> {
    defs: &'c R,
    prop_origins: FnvHashMap<PropertyId, Var>,
    /// A table of which variable produce which properties
    prop_origins_inverted: FnvHashMap<Var, FnvHashSet<PropertyId>>,
    /// A mapping from a variable to its origin property
    var_origins: FnvHashMap<Var, PropertyId>,
    /// A mapping from variable to its dependencies
    var_dependencies: FnvHashMap<Var, VarSet>,
    property_flow: BTreeSet<PropertyFlow>,
}

impl<'c, R> Debug for DataFlowAnalyzer<'c, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFlowAnalyzer")
            .field("prop_origins", &self.prop_origins)
            .field("prop_origins_inv", &self.prop_origins_inverted)
            .field("var_origins", &self.var_origins)
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
            prop_origins: FnvHashMap::default(),
            prop_origins_inverted: FnvHashMap::default(),
            var_origins: FnvHashMap::default(),
            var_dependencies: FnvHashMap::default(),
            property_flow: BTreeSet::default(),
        }
    }

    pub fn analyze(&mut self, arg: Var, body: TypedNodeRef<'_, 'm>) -> Option<Vec<PropertyFlow>> {
        self.var_dependencies.insert(arg, VarSet::default());

        let unit_prop_id = PropertyId {
            role: Role::Subject,
            relationship_id: RelationshipId(DefId::unit()),
        };

        match body.kind() {
            ontol_hir::Kind::Struct(binder, flags, nodes) => {
                for node_ref in body.arena().node_refs(nodes) {
                    self.analyze_node(node_ref, unit_prop_id);
                }

                if flags.contains(StructFlags::MATCH) {
                    self.property_flow.insert(PropertyFlow {
                        id: unit_prop_id,
                        data: PropertyFlowData::Match(binder.0.var),
                    });
                }
            }
            _ => {
                let deps = self.analyze_node(body, unit_prop_id);
                self.reg_output_prop(arg, unit_prop_id, deps);
            }
        }

        for (property_id, var) in &self.prop_origins {
            if let Some(parent) = self.var_origins.get(var) {
                self.property_flow.insert(PropertyFlow {
                    id: *property_id,
                    data: PropertyFlowData::ChildOf(*parent),
                });
            }
        }

        Some(
            std::mem::take(&mut self.property_flow)
                .into_iter()
                .collect(),
        )
    }

    fn analyze_node(&mut self, node_ref: TypedNodeRef<'_, 'm>, parent_prop: PropertyId) -> VarSet {
        // debug!("{node_ref}");

        let arena = node_ref.arena();
        match node_ref.kind() {
            ontol_hir::Kind::NoOp => VarSet::default(),
            ontol_hir::Kind::Var(var) => VarSet::from([*var]),
            ontol_hir::Kind::Block(body)
            | ontol_hir::Kind::Catch(_, body)
            | ontol_hir::Kind::CatchFunc(_, body) => {
                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }
                var_set
            }
            ontol_hir::Kind::Try(..) => VarSet::default(),
            ontol_hir::Kind::LetProp(Attribute { rel, val }, (struct_var, prop_id))
            | ontol_hir::Kind::LetPropDefault(Attribute { rel, val }, (struct_var, prop_id), _)
            | ontol_hir::Kind::TryLetProp(_, Attribute { rel, val }, (struct_var, prop_id)) => {
                self.prop_origins.insert(*prop_id, *struct_var);
                self.prop_origins_inverted
                    .entry(*struct_var)
                    .or_default()
                    .insert(*prop_id);

                let mut value_ty = &UNIT_TYPE;

                if let ontol_hir::Binding::Binder(binder) = rel {
                    self.var_origins.insert(binder.hir().var, *prop_id);
                    self.add_dep(binder.hir().var, *struct_var);
                }
                if let ontol_hir::Binding::Binder(binder) = val {
                    self.var_origins.insert(binder.hir().var, *prop_id);
                    self.add_dep(binder.hir().var, *struct_var);
                    value_ty = binder.ty();
                }

                let value_def_id = match value_ty {
                    Type::Seq(_rel, val) => val.get_single_def_id(),
                    other => other.get_single_def_id(),
                };

                self.reg_scope_prop(*struct_var, *prop_id, value_def_id.unwrap_or(DefId::unit()));

                VarSet::default()
            }
            ontol_hir::Kind::Let(binder, definition)
            | ontol_hir::Kind::TryLet(_, binder, definition) => {
                let var_deps = self.analyze_node(arena.node_ref(*definition), parent_prop);
                self.var_dependencies.insert(binder.hir().var, var_deps);
                VarSet::default()
            }
            ontol_hir::Kind::TryLetTup(_, bindings, source) => {
                let var_deps = self.analyze_node(arena.node_ref(*source), parent_prop);
                for binding in bindings {
                    if let ontol_hir::Binding::Binder(binder) = binding {
                        self.var_dependencies
                            .insert(binder.hir().var, var_deps.clone());
                    }
                }
                VarSet::default()
            }
            ontol_hir::Kind::LetRegex(groups_list, _regex_def_id, var) => {
                for groups in groups_list {
                    for group in groups {
                        self.add_dep(group.binder.hir().var, *var);
                    }
                }
                VarSet::default()
            }
            ontol_hir::Kind::LetRegexIter(binder, _groups_list, _regex_def_id, var) => {
                self.add_dep(binder.hir().var, *var);
                VarSet::default()
            }
            ontol_hir::Kind::Unit => VarSet::default(),
            ontol_hir::Kind::I64(_) => VarSet::default(),
            ontol_hir::Kind::F64(_) => VarSet::default(),
            ontol_hir::Kind::Text(_) => VarSet::default(),
            ontol_hir::Kind::Const(_) => VarSet::default(),
            ontol_hir::Kind::With(binder, definition, body) => {
                let var_deps = self.analyze_node(arena.node_ref(*definition), parent_prop);
                self.var_dependencies.insert(binder.hir().var, var_deps);

                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }

                var_set
            }
            ontol_hir::Kind::Call(_, params) => {
                let mut var_set = VarSet::default();
                for child in arena.node_refs(params) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }
                var_set
            }
            ontol_hir::Kind::Map(node) | ontol_hir::Kind::Narrow(node) => {
                self.analyze_node(arena.node_ref(*node), parent_prop)
            }
            ontol_hir::Kind::Set(entries) => {
                let mut var_set = VarSet::default();
                for set_entry in entries {
                    var_set.union_with(
                        &self.analyze_node(arena.node_ref(set_entry.1.rel), parent_prop),
                    );
                    var_set.union_with(
                        &self.analyze_node(arena.node_ref(set_entry.1.val), parent_prop),
                    );
                }
                var_set
            }
            ontol_hir::Kind::Struct(binder, flags, body) => {
                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }

                if flags.contains(StructFlags::MATCH) {
                    self.property_flow.insert(PropertyFlow {
                        id: parent_prop,
                        data: PropertyFlowData::Match(binder.0.var),
                    });
                }

                var_set
            }
            ontol_hir::Kind::Prop(_, struct_var, prop_id, variant) => {
                self.prop_origins.insert(*prop_id, *struct_var);
                self.prop_origins_inverted
                    .entry(*struct_var)
                    .or_default()
                    .insert(*prop_id);

                let mut var_set = VarSet::default();

                match variant {
                    PropVariant::Value(Attribute { rel, val }) => {
                        var_set.union_with(&self.analyze_node(arena.node_ref(*rel), *prop_id));
                        var_set.union_with(&self.analyze_node(arena.node_ref(*val), *prop_id));
                    }
                    PropVariant::Predicate(..) => {
                        todo!()
                    }
                }

                self.reg_output_prop(*struct_var, *prop_id, var_set);

                Default::default()
            }
            ontol_hir::Kind::MakeSeq(_, body) => {
                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }
                var_set
            }
            ontol_hir::Kind::ForEach(var, (rel_binding, val_binding), body) => {
                if let ontol_hir::Binding::Binder(binder) = rel_binding {
                    self.add_dep(binder.hir().var, *var);
                }
                if let ontol_hir::Binding::Binder(binder) = val_binding {
                    self.add_dep(binder.hir().var, *var);
                }
                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }
                var_set
            }
            ontol_hir::Kind::Insert(var, Attribute { rel, val }) => {
                let mut var_set = self.analyze_node(arena.node_ref(*rel), parent_prop);
                var_set.union_with(&self.analyze_node(arena.node_ref(*val), parent_prop));

                self.var_dependencies.insert(*var, var_set.clone());

                var_set
            }
            ontol_hir::Kind::StringPush(to_var, node) => {
                let var_set = self.analyze_node(arena.node_ref(*node), parent_prop);
                self.var_dependencies.insert(*to_var, var_set.clone());
                var_set
            }
            ontol_hir::Kind::Regex(..)
            | ontol_hir::Kind::PushCondClauses(..)
            | ontol_hir::Kind::MoveRestAttrs(..)
            | ontol_hir::Kind::CopySubSeq(..)
            | ontol_hir::Kind::LetCondVar(..)
            | ontol_hir::Kind::TryNarrow(..) => VarSet::default(),
        }
    }

    fn add_dep(&mut self, var: Var, dep: Var) {
        self.var_dependencies.entry(var).or_default().insert(dep);
    }

    fn reg_output_prop(
        &mut self,
        _struct_var: Var,
        property_id: PropertyId,
        mut var_dependencies: VarSet,
    ) {
        while !var_dependencies.0.is_empty() {
            let mut next_deps = VarSet::default();

            for var in &var_dependencies {
                if true {
                    if let Some(source_prop) = self.var_origins.get(&var) {
                        self.property_flow.insert(PropertyFlow {
                            id: property_id,
                            data: PropertyFlowData::DependentOn(*source_prop),
                        });
                    } else if let Some(parent_vars) = self.var_dependencies.get(&var) {
                        next_deps.union_with(parent_vars);
                    }
                } else if let Some(deps) = self.prop_origins_inverted.get(&var) {
                    for dep_prop_id in deps {
                        // debug!("      insert {dep_prop_id}");
                        self.property_flow.insert(PropertyFlow {
                            id: property_id,
                            data: PropertyFlowData::DependentOn(*dep_prop_id),
                        });
                    }
                } else if let Some(parent_vars) = self.var_dependencies.get(&var) {
                    next_deps.union_with(parent_vars);
                }
            }

            // debug!("  next deps: {next_deps:?}");

            var_dependencies = next_deps;
        }
    }

    /// The purpose of the value_def_id is for the query engine
    /// to understand which entity must be looked up.
    /// rel_params is ignored here.
    fn reg_scope_prop(&mut self, struct_var: Var, property_id: PropertyId, value_def_id: DefId) {
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
            &self.var_origins,
            &mut self.property_flow,
        );
    }
}

fn register_children_recursive(
    var: Var,
    property_id: PropertyId,
    var_dependencies: &FnvHashMap<Var, VarSet>,
    // var_to_prop: &FnvHashMap<Var, FnvHashSet<PropertyId>>,
    var_origins: &FnvHashMap<Var, PropertyId>,
    output: &mut BTreeSet<PropertyFlow>,
) {
    if let Some(dependencies) = var_dependencies.get(&var) {
        for var_dependency in dependencies {
            if let Some(prop_id) = var_origins.get(&var_dependency) {
                // recursion stops here, as there is a property associated with the variable:
                output.insert(PropertyFlow {
                    id: property_id,
                    data: PropertyFlowData::ChildOf(*prop_id),
                });
            } else {
                register_children_recursive(
                    var_dependency,
                    property_id,
                    var_dependencies,
                    var_origins,
                    output,
                );
            }
        }
    }
}
