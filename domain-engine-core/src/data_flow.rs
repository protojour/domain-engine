use std::cmp::Ordering;

use fnv::FnvHashMap;
use ontol_runtime::{
    env::{DataFlow, PropertyFlow, PropertyFlowData},
    query::Query,
    value::PropertyId,
};
use tracing::info;

struct IsDep(bool);

pub fn translate_query_property(
    property_id: PropertyId,
    query: Query,
    data_flow: &DataFlow,
    target: &mut FnvHashMap<PropertyId, Query>,
) {
    info!(
        "translate property_id {property_id} flow: {:#?}",
        data_flow.properties
    );

    traverse(property_id, query, data_flow, IsDep(false), target);
}

fn traverse(
    property_id: PropertyId,
    query: Query,
    data_flow: &DataFlow,
    is_dep: IsDep,
    target: &mut FnvHashMap<PropertyId, Query>,
) {
    for property_flow in flow_iter(data_flow, property_id) {
        match &property_flow.data {
            PropertyFlowData::DependentOn(dependent_property_id) => {
                if is_dep.0 {
                    panic!("Transitive dependency");
                } else {
                    // change to dep mode
                    traverse(
                        *dependent_property_id,
                        query.clone(),
                        data_flow,
                        IsDep(true),
                        target,
                    );
                }
            }
            PropertyFlowData::ChildOf(parent_property_id) => {
                if is_dep.0 {
                    todo!("Need to make a sub query, but don't have the DefId");
                } else {
                    traverse(
                        *parent_property_id,
                        query.clone(),
                        data_flow,
                        IsDep(false),
                        target,
                    );
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

fn flow_iter(data_flow: &DataFlow, property_id: PropertyId) -> impl Iterator<Item = &PropertyFlow> {
    // "fast forward" to the first property flow for the given property_id:
    let lower_bound = data_flow
        .properties
        .binary_search_by(|flow| match flow.id.cmp(&property_id) {
            Ordering::Equal => Ordering::Greater,
            ord => ord,
        })
        .unwrap_or_else(|err| err);

    data_flow.properties[lower_bound..]
        .iter()
        .take_while(move |flow| flow.id == property_id)
}
