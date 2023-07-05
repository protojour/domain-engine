use std::cmp::Ordering;

use fnv::FnvHashMap;
use ontol_runtime::{
    env::{DataFlow, PropertyFlow, PropertyFlowRelationship},
    query::Query,
    value::PropertyId,
};
use tracing::{debug, info};

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
    if let Some(flow_range) = flow_range(data_flow, property_id) {
        for property_flow in flow_range {
            match &property_flow.relationship {
                PropertyFlowRelationship::DependentOn(dependent_property_id) => {
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
                PropertyFlowRelationship::ChildOf(parent_property_id) => {
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
                PropertyFlowRelationship::None => {
                    if is_dep.0 {
                        target.insert(property_flow.property, Query::Leaf);
                    }
                }
            }
        }
    } else {
        panic!(
            "No flow range for {property_id} in {:#?}",
            data_flow.properties
        );
    }
}

// FIXME: Could use an iterator that stops when out of range instead of doing upper_bound
fn flow_range(data_flow: &DataFlow, property_id: PropertyId) -> Option<&[PropertyFlow]> {
    let lower_bound = data_flow
        .properties
        .binary_search_by(|flow| match flow.property.cmp(&property_id) {
            Ordering::Equal => Ordering::Greater,
            ord => ord,
        })
        .or_else(|idx| match data_flow.properties.get(idx) {
            Some(flow) if flow.property == property_id => Ok(idx),
            _ => Err(idx),
        });

    let upper_bound = data_flow
        .properties
        .binary_search_by(|flow| match flow.property.cmp(&property_id) {
            Ordering::Equal => Ordering::Less,
            ord => ord,
        })
        .unwrap_err();

    debug!("flow_range lower/upper: {lower_bound:?} / {upper_bound:?}");

    Some(&data_flow.properties[lower_bound.ok()?..upper_bound])
}
