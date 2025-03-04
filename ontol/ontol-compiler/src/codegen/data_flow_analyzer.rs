//! High-level data flow of struct properties

use std::{collections::BTreeSet, fmt::Debug};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::{Pack, PropVariant, StructFlags};
use ontol_runtime::{
    DefId, DefPropTag, PropId,
    ontology::map::{PropertyFlow, PropertyFlowData},
    property::Cardinality,
    var::{Var, VarSet},
};
use tracing::warn;

use crate::{
    def::Defs, properties::PropCtx, relation::RelCtx, typed_hir::TypedNodeRef, types::Type,
};

pub struct DataFlowAnalyzer<'c, 'm> {
    defs: &'c Defs<'m>,
    rel_ctx: &'c RelCtx,
    prop_ctx: &'c PropCtx,
    get_subject_cardinality:
        &'c dyn Fn(PropId, &'c RelCtx, &'c PropCtx, &'c Defs<'m>) -> Cardinality,
    prop_origins: FnvHashMap<PropId, Var>,
    /// A table of which variable produce which properties
    prop_origins_inverted: FnvHashMap<Var, FnvHashSet<PropId>>,
    /// A mapping from a variable to its origin property
    var_origins: FnvHashMap<Var, PropId>,
    /// A mapping from variable to its dependencies
    var_dependencies: FnvHashMap<Var, VarSet>,
    property_flow: BTreeSet<PropertyFlow>,
}

impl Debug for DataFlowAnalyzer<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFlowAnalyzer")
            .field("prop_origins", &self.prop_origins)
            .field("prop_origins_inv", &self.prop_origins_inverted)
            .field("var_origins", &self.var_origins)
            .field("var_dependencies", &self.var_dependencies)
            .finish()
    }
}

impl<'c, 'm> DataFlowAnalyzer<'c, 'm> {
    pub fn new(
        defs: &'c Defs<'m>,
        rel_ctx: &'c RelCtx,
        prop_ctx: &'c PropCtx,
        get_subject_cardinality: &'c dyn Fn(
            PropId,
            &'c RelCtx,
            &'c PropCtx,
            &'c Defs<'m>,
        ) -> Cardinality,
    ) -> Self {
        Self {
            defs,
            rel_ctx,
            prop_ctx,
            get_subject_cardinality,
            prop_origins: FnvHashMap::default(),
            prop_origins_inverted: FnvHashMap::default(),
            var_origins: FnvHashMap::default(),
            var_dependencies: FnvHashMap::default(),
            property_flow: BTreeSet::default(),
        }
    }

    pub fn analyze(&mut self, arg: Var, body: TypedNodeRef<'_, 'm>) -> Option<Vec<PropertyFlow>> {
        self.var_dependencies.insert(arg, VarSet::default());

        let unit_prop_id = PropId(DefId::unit(), DefPropTag(0));

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

        for (prop_id, var) in &self.prop_origins {
            if let Some(parent) = self.var_origins.get(var) {
                self.property_flow.insert(PropertyFlow {
                    id: *prop_id,
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

    fn analyze_node(&mut self, node_ref: TypedNodeRef<'_, 'm>, parent_prop: PropId) -> VarSet {
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
            ontol_hir::Kind::LetProp(bind_pack, (struct_var, prop_id))
            | ontol_hir::Kind::LetPropDefault(bind_pack, (struct_var, prop_id), _)
            | ontol_hir::Kind::TryLetProp(_, bind_pack, (struct_var, prop_id)) => {
                self.prop_origins.insert(*prop_id, *struct_var);
                self.prop_origins_inverted
                    .entry(*struct_var)
                    .or_default()
                    .insert(*prop_id);

                let mut types = vec![];

                match bind_pack {
                    Pack::Unit(binding) => {
                        if let ontol_hir::Binding::Binder(binder) = binding {
                            self.var_origins.insert(binder.hir().var, *prop_id);
                            self.add_dep(binder.hir().var, *struct_var);
                            types.push(binder.ty());
                        }
                    }
                    Pack::Tuple(t) => {
                        for binding in t {
                            if let ontol_hir::Binding::Binder(binder) = binding {
                                self.var_origins.insert(binder.hir().var, *prop_id);
                                self.add_dep(binder.hir().var, *struct_var);
                                types.push(binder.ty());
                            }
                        }
                    }
                }

                for ty in types {
                    self.reg_scope_prop(*struct_var, *prop_id, ty);
                }

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
            ontol_hir::Kind::Map(node)
            | ontol_hir::Kind::Pun(node)
            | ontol_hir::Kind::Narrow(node) => {
                self.analyze_node(arena.node_ref(*node), parent_prop)
            }
            ontol_hir::Kind::Matrix(rows) => {
                let mut var_set = VarSet::default();
                for row in rows {
                    for element in &row.1 {
                        var_set
                            .union_with(&self.analyze_node(arena.node_ref(*element), parent_prop));
                    }
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
                    PropVariant::Unit(node) => {
                        var_set.union_with(&self.analyze_node(arena.node_ref(*node), *prop_id));
                    }
                    PropVariant::Tuple(tup) => {
                        for node in tup {
                            var_set.union_with(&self.analyze_node(arena.node_ref(*node), *prop_id));
                        }
                    }
                    PropVariant::Predicate(..) => {
                        todo!()
                    }
                }

                self.reg_output_prop(*struct_var, *prop_id, var_set);

                Default::default()
            }
            ontol_hir::Kind::MakeSeq(_, body) | ontol_hir::Kind::MakeMatrix(_, body) => {
                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }
                var_set
            }
            ontol_hir::Kind::ForEach(elements, body) => {
                for (var, binding) in elements {
                    if let ontol_hir::Binding::Binder(binder) = binding {
                        self.add_dep(binder.hir().var, *var);
                    }
                }
                let mut var_set = VarSet::default();
                for child in arena.node_refs(body) {
                    var_set.union_with(&self.analyze_node(child, parent_prop));
                }
                var_set
            }
            ontol_hir::Kind::Insert(var, node) => {
                let var_set = self.analyze_node(arena.node_ref(*node), parent_prop);
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

    fn reg_output_prop(&mut self, _struct_var: Var, prop_id: PropId, mut var_dependencies: VarSet) {
        while !var_dependencies.0.is_empty() {
            let mut next_deps = VarSet::default();

            for var in &var_dependencies {
                if true {
                    if let Some(source_prop) = self.var_origins.get(&var) {
                        self.property_flow.insert(PropertyFlow {
                            id: prop_id,
                            data: PropertyFlowData::DependentOn(*source_prop),
                        });
                    } else if let Some(parent_vars) = self.var_dependencies.get(&var) {
                        next_deps.union_with(parent_vars);
                    }
                } else if let Some(deps) = self.prop_origins_inverted.get(&var) {
                    for dep_prop_id in deps {
                        // debug!("      insert {dep_prop_id}");
                        self.property_flow.insert(PropertyFlow {
                            id: prop_id,
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
    fn reg_scope_prop(&mut self, struct_var: Var, prop_id: PropId, ty: &Type) {
        let subject_cardinality =
            (*self.get_subject_cardinality)(prop_id, self.rel_ctx, self.prop_ctx, self.defs);

        match ty {
            Type::Seq(ty) => {
                if let Some(value_def_id) = ty.get_single_def_id() {
                    self.property_flow.insert(PropertyFlow {
                        id: prop_id,
                        data: PropertyFlowData::UnitType(value_def_id),
                    });
                }
            }
            Type::Matrix(tuple) => {
                for (idx, ty) in tuple.iter().enumerate() {
                    let idx: Result<u8, _> = idx.try_into();
                    let Ok(idx) = idx else {
                        warn!("no type found for tuple");
                        continue;
                    };

                    if let Some(value_def_id) = ty.get_single_def_id() {
                        self.property_flow.insert(PropertyFlow {
                            id: prop_id,
                            data: PropertyFlowData::TupleType(idx, value_def_id),
                        });
                    }
                }
            }
            ty => {
                if let Some(value_def_id) = ty.get_single_def_id() {
                    self.property_flow.insert(PropertyFlow {
                        id: prop_id,
                        data: PropertyFlowData::UnitType(value_def_id),
                    });
                }
            }
        }

        self.property_flow.insert(PropertyFlow {
            id: prop_id,
            data: PropertyFlowData::Cardinality(subject_cardinality),
        });

        register_children_recursive(
            struct_var,
            prop_id,
            &self.var_dependencies,
            &self.var_origins,
            &mut self.property_flow,
        );
    }
}

fn register_children_recursive(
    var: Var,
    prop_id: PropId,
    var_dependencies: &FnvHashMap<Var, VarSet>,
    // var_to_prop: &FnvHashMap<Var, FnvHashSet<PropertyId>>,
    var_origins: &FnvHashMap<Var, PropId>,
    output: &mut BTreeSet<PropertyFlow>,
) {
    if let Some(dependencies) = var_dependencies.get(&var) {
        for var_dependency in dependencies {
            if let Some(parent_prop_id) = var_origins.get(&var_dependency) {
                // recursion stops here, as there is a property associated with the variable:
                output.insert(PropertyFlow {
                    id: prop_id,
                    data: PropertyFlowData::ChildOf(*parent_prop_id),
                });
            } else {
                register_children_recursive(
                    var_dependency,
                    prop_id,
                    var_dependencies,
                    var_origins,
                    output,
                );
            }
        }
    }
}
