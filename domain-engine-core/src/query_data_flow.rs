use std::cmp::Ordering;

use fnv::FnvHashMap;
use ontol_runtime::{
    env::{DataFlow, PropertyFlow, PropertyFlowData},
    query::{Query, StructQuery},
    value::PropertyId,
    DefId,
};
use tracing::debug;

struct IsDep(bool);

pub struct QueryFlowProcessor<'e> {
    pub data_flow: &'e DataFlow,
}

impl<'e> QueryFlowProcessor<'e> {
    pub fn new(data_flow: &'e DataFlow) -> Self {
        Self { data_flow }
    }

    pub fn translate_query_property(
        &self,
        property_id: PropertyId,
        query: Query,
        target: &mut FnvHashMap<PropertyId, Query>,
    ) {
        debug!(
            "translate property_id {property_id} flow: {:#?}",
            self.data_flow.properties
        );

        self.traverse(property_id, query, IsDep(false), target);
    }

    fn traverse(
        &self,
        property_id: PropertyId,
        query: Query,
        is_dep: IsDep,
        target: &mut FnvHashMap<PropertyId, Query>,
    ) {
        for property_flow in self.flow_iter(property_id) {
            match &property_flow.data {
                PropertyFlowData::DependentOn(dependent_property_id) => {
                    if is_dep.0 {
                        panic!("Transitive dependency");
                    } else {
                        // change to dep mode
                        self.traverse(*dependent_property_id, query.clone(), IsDep(true), target);
                    }
                }
                PropertyFlowData::ChildOf(parent_property_id) => {
                    if is_dep.0 {
                        self.with_parent_query(*parent_property_id, target, &|target| {
                            target.insert(property_flow.id, Query::Leaf);
                        })
                    } else {
                        self.traverse(*parent_property_id, query.clone(), IsDep(false), target);
                    }
                }
                PropertyFlowData::Type(_def_id) => {
                    if is_dep.0 {
                        target.insert(property_flow.id, Query::Leaf);
                    }
                }
            }
        }
    }

    fn with_parent_query(
        &self,
        property_id: PropertyId,
        target: &mut FnvHashMap<PropertyId, Query>,
        func: &dyn Fn(&mut FnvHashMap<PropertyId, Query>),
    ) {
        fn handle_child_query(
            target: &mut FnvHashMap<PropertyId, Query>,
            property_id: PropertyId,
            parent_def_id: DefId,
            func: impl Fn(&mut FnvHashMap<PropertyId, Query>),
        ) {
            let child_query = target.entry(property_id).or_insert_with(|| {
                Query::Struct(StructQuery {
                    def_id: parent_def_id,
                    properties: Default::default(),
                })
            });
            func(match child_query {
                Query::Struct(struct_query) => &mut struct_query.properties,
                _ => todo!(),
            });
        }

        let def_id = self.find_def_id(property_id).unwrap();
        let mut is_root = true;

        for property_flow in self.flow_iter(property_id) {
            if let PropertyFlowData::ChildOf(parent_property_id) = &property_flow.data {
                is_root = false;
                self.with_parent_query(*parent_property_id, target, &|child_properties| {
                    handle_child_query(child_properties, property_id, def_id, func);
                });
            }
        }

        if is_root {
            handle_child_query(target, property_id, def_id, func);
        }
    }

    fn find_def_id(&self, property_id: PropertyId) -> Option<DefId> {
        self.flow_iter(property_id)
            .find_map(|property_flow| match &property_flow.data {
                PropertyFlowData::Type(def_id) => Some(*def_id),
                _ => None,
            })
    }

    fn flow_iter(&self, property_id: PropertyId) -> impl Iterator<Item = &PropertyFlow> {
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
