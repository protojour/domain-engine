use std::cmp::Ordering;

use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    env::{DataFlow, Env, PropertyCardinality, PropertyFlow, PropertyFlowData, ValueCardinality},
    query::{EntityQuery, Query, StructOrUnionQuery, StructQuery},
    value::PropertyId,
    DefId, MapKey, PackageId,
};
use tracing::{debug, trace};

#[derive(Clone, Copy)]
struct IsDep(bool);

pub fn translate_entity_query(query: &mut EntityQuery, from: MapKey, to: MapKey, env: &Env) {
    let map_meta = env
        .get_map_meta(to, from)
        .expect("No mapping procedure for query transformer");

    trace!(
        "translate_entity_query flow props: {:#?}",
        map_meta.data_flow.properties
    );

    match &mut query.source {
        StructOrUnionQuery::Struct(struct_query) => {
            let processor = QueryFlowProcessor {
                env,
                data_flow: &map_meta.data_flow,
            };

            debug!("Input query: {struct_query:#?}");

            processor.autoselect_output_properties(
                struct_query.def_id.package_id(),
                &mut struct_query.properties,
            );

            debug!("Input query (after auto select): {struct_query:#?}");

            struct_query.def_id = to.def_id;

            let query_props = std::mem::take(&mut struct_query.properties);
            for (property_id, query) in query_props {
                processor.translate_property(
                    property_id,
                    query,
                    IsDep(false),
                    &mut struct_query.properties,
                )
            }

            debug!("Translated query: {struct_query:#?}");
        }
        _ => todo!(),
    }
}

struct QueryFlowProcessor<'e> {
    env: &'e Env,
    data_flow: &'e DataFlow,
}

impl<'e> QueryFlowProcessor<'e> {
    fn autoselect_output_properties(
        &self,
        output_package_id: PackageId,
        target: &mut FnvHashMap<PropertyId, Query>,
    ) {
        for (property_id, flows) in &self.data_flow.properties.iter().group_by(|flow| flow.id) {
            // Only consider output properties:
            if property_id.relationship_id.0.package_id() != output_package_id {
                continue;
            }

            for property_flow in flows {
                match &property_flow.data {
                    PropertyFlowData::ChildOf(parent_property_id) => self.with_parent_query(
                        *parent_property_id,
                        target,
                        &|parent_property_id| {
                            self.depends_on_mandatory_entity(parent_property_id, IsDep(false))
                        },
                        &|target| {
                            target.insert(property_flow.id, Query::Leaf);
                        },
                    ),
                    PropertyFlowData::Type(_) | PropertyFlowData::DependentOn(_) => {
                        if self.depends_on_mandatory_entity(property_id, IsDep(false)) {
                            target.insert(property_flow.id, Query::Leaf);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    fn translate_property(
        &self,
        property_id: PropertyId,
        query: Query,
        is_dep: IsDep,
        target: &mut FnvHashMap<PropertyId, Query>,
    ) {
        let mut has_parent = false;

        for property_flow in self.property_flows_for(property_id) {
            match &property_flow.data {
                PropertyFlowData::DependentOn(dependent_property_id) => {
                    if is_dep.0 {
                        panic!("Transitive dependency");
                    } else {
                        // change to dep mode
                        self.translate_property(
                            *dependent_property_id,
                            query.clone(),
                            IsDep(true),
                            target,
                        );
                    }
                }
                PropertyFlowData::ChildOf(parent_property_id) => {
                    if is_dep.0 {
                        self.with_parent_query(*parent_property_id, target, &|_| true, &|target| {
                            target.insert(property_flow.id, Query::Leaf);
                        });
                        has_parent = true;
                    } else {
                        self.translate_property(
                            *parent_property_id,
                            query.clone(),
                            IsDep(false),
                            target,
                        );
                    }
                }
                PropertyFlowData::Type(_def_id) => {}
                PropertyFlowData::Cardinality(_) => {}
            }
        }

        if !has_parent && is_dep.0 {
            target.insert(property_id, Query::Leaf);
        }
    }

    fn with_parent_query(
        &self,
        property_id: PropertyId,
        target: &mut FnvHashMap<PropertyId, Query>,
        parent_predicate: &dyn Fn(PropertyId) -> bool,
        child_func: &dyn Fn(&mut FnvHashMap<PropertyId, Query>),
    ) {
        fn handle_child_query(
            target: &mut FnvHashMap<PropertyId, Query>,
            property_id: PropertyId,
            parent_def_id: DefId,
            parent_predicate: impl Fn(PropertyId) -> bool,
            child_func: impl Fn(&mut FnvHashMap<PropertyId, Query>),
        ) {
            if !parent_predicate(property_id) {
                return;
            }

            let child_query = target.entry(property_id).or_insert_with(|| {
                Query::Struct(StructQuery {
                    def_id: parent_def_id,
                    properties: Default::default(),
                })
            });
            child_func(match child_query {
                Query::Struct(struct_query) => &mut struct_query.properties,
                _ => todo!(),
            });
        }

        let def_id = self.find_def_id(property_id).unwrap();
        let mut is_root = true;

        for property_flow in self.property_flows_for(property_id) {
            if let PropertyFlowData::ChildOf(parent_property_id) = &property_flow.data {
                is_root = false;
                self.with_parent_query(
                    *parent_property_id,
                    target,
                    parent_predicate,
                    &|child_properties| {
                        handle_child_query(
                            child_properties,
                            property_id,
                            def_id,
                            parent_predicate,
                            child_func,
                        );
                    },
                );
            }
        }

        if is_root {
            handle_child_query(target, property_id, def_id, parent_predicate, child_func);
        }
    }

    fn depends_on_mandatory_entity(&self, property_id: PropertyId, is_dep: IsDep) -> bool {
        let mut is_entity = false;
        let mut is_required = false;

        for flow in self.property_flows_for(property_id) {
            match &flow.data {
                PropertyFlowData::DependentOn(dependent_property_id) => {
                    if self.depends_on_mandatory_entity(*dependent_property_id, IsDep(true)) {
                        return true;
                    }
                }
                PropertyFlowData::Type(def_id) if is_dep.0 => {
                    let type_info = self.env.get_type_info(*def_id);
                    is_entity = type_info.entity_info.is_some();
                }
                PropertyFlowData::Cardinality((
                    PropertyCardinality::Mandatory,
                    ValueCardinality::One,
                )) if is_dep.0 => {
                    is_required = true;
                }
                PropertyFlowData::ChildOf(parent_property_id) => {
                    if self.depends_on_mandatory_entity(*parent_property_id, is_dep) {
                        return true;
                    }
                }
                _ => {}
            }
        }

        trace!("property_id {property_id} depends on mandatory entity: {is_entity}, {is_required}");

        is_entity && is_required
    }

    fn find_def_id(&self, property_id: PropertyId) -> Option<DefId> {
        self.property_flows_for(property_id)
            .find_map(|property_flow| match &property_flow.data {
                PropertyFlowData::Type(def_id) => Some(*def_id),
                _ => None,
            })
    }

    fn property_flows_for(&self, property_id: PropertyId) -> impl Iterator<Item = &PropertyFlow> {
        // "fast forward" to the first property flow for the given property_id:
        let lower_bound = self
            .data_flow
            .properties
            .binary_search_by(|flow| match flow.id.cmp(&property_id) {
                Ordering::Equal => Ordering::Greater,
                ord => ord,
            })
            .unwrap_or_else(|err| err);

        self.data_flow.properties[lower_bound..]
            .iter()
            .take_while(move |flow| flow.id == property_id)
    }
}
