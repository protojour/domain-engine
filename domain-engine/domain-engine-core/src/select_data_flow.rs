use std::cmp::Ordering;

use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    ontology::{
        map::{PropertyFlow, PropertyFlowData},
        Ontology,
    },
    property::{PropertyCardinality, PropertyId, ValueCardinality},
    query::select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    DefId, MapKey, PackageId,
};
use tracing::{debug, trace};

#[derive(Clone, Copy)]
struct IsDep(bool);

pub fn translate_select(select: &mut Select, key: &MapKey, ontology: &Ontology) {
    match select {
        Select::Entity(entity_select) => {
            translate_entity_select(entity_select, key, ontology);
        }
        Select::Struct(struct_select) => {
            translate_struct_select(struct_select, key, ontology);
        }
        Select::StructUnion(_def_id, selects) => {
            for select in selects {
                translate_struct_select(select, key, ontology);
            }
        }
        Select::EntityId => {}
        Select::Leaf => {}
    }
}

pub fn translate_entity_select(select: &mut EntitySelect, key: &MapKey, ontology: &Ontology) {
    match &mut select.source {
        StructOrUnionSelect::Struct(struct_select) => {
            translate_struct_select(struct_select, key, ontology);
        }
        _ => todo!(),
    }
}

fn translate_struct_select(struct_select: &mut StructSelect, key: &MapKey, ontology: &Ontology) {
    debug!(
        "translate struct select {:?} for {:?}",
        struct_select.def_id,
        key.def_ids()
    );

    let map_meta = ontology
        .get_map_meta(key)
        .expect("No mapping procedure for select transformer");
    let Some(prop_flow_slice) = ontology.get_prop_flow_slice(map_meta) else {
        debug!("clearing properties since there is no property flow");
        struct_select.properties.clear();
        return;
    };

    trace!(
        "translate_entity_select flow props: {:#?}",
        ontology.debug(prop_flow_slice)
    );

    let processor = SelectFlowProcessor {
        ontology,
        prop_flow_slice,
    };

    processor.autoselect_output_properties(
        struct_select.def_id.package_id(),
        &mut struct_select.properties,
    );

    struct_select.def_id = key.input.def_id;

    debug!("Input select (after auto select): {struct_select:#?}");

    let select_props = std::mem::take(&mut struct_select.properties);
    for (property_id, select) in select_props {
        processor.translate_property(
            property_id,
            select,
            IsDep(false),
            &mut struct_select.properties,
        )
    }

    debug!("Translated select: {struct_select:#?}");
}

struct SelectFlowProcessor<'on> {
    ontology: &'on Ontology,
    prop_flow_slice: &'on [PropertyFlow],
}

impl<'on> SelectFlowProcessor<'on> {
    fn autoselect_output_properties(
        &self,
        output_package_id: PackageId,
        target: &mut FnvHashMap<PropertyId, Select>,
    ) {
        for (property_id, flows) in &self.prop_flow_slice.iter().group_by(|flow| flow.id) {
            // Only consider output properties:
            if property_id.relationship_id.0.package_id() != output_package_id {
                continue;
            }

            for property_flow in flows {
                match &property_flow.data {
                    PropertyFlowData::ChildOf(parent_property_id) => self.with_parent_select(
                        *parent_property_id,
                        target,
                        &|parent_property_id| {
                            self.depends_on_mandatory_entity(parent_property_id, IsDep(false))
                        },
                        &|target| {
                            target.insert(property_flow.id, Select::Leaf);
                        },
                    ),
                    PropertyFlowData::Type(_) | PropertyFlowData::DependentOn(_) => {
                        if self.depends_on_mandatory_entity(property_id, IsDep(false)) {
                            target.insert(property_flow.id, Select::Leaf);
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
        select: Select,
        is_dep: IsDep,
        target: &mut FnvHashMap<PropertyId, Select>,
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
                            select.clone(),
                            IsDep(true),
                            target,
                        );
                    }
                }
                PropertyFlowData::ChildOf(parent_property_id) => {
                    if is_dep.0 {
                        self.with_parent_select(
                            *parent_property_id,
                            target,
                            &|_| true,
                            &|target| {
                                target.insert(property_flow.id, Select::Leaf);
                            },
                        );
                        has_parent = true;
                    } else {
                        self.translate_property(
                            *parent_property_id,
                            select.clone(),
                            IsDep(false),
                            target,
                        );
                    }
                }
                PropertyFlowData::Type(_)
                | PropertyFlowData::Cardinality(_)
                | PropertyFlowData::Match(_) => {}
            }
        }

        if !has_parent && is_dep.0 {
            target.insert(property_id, Select::Leaf);
        }
    }

    fn with_parent_select(
        &self,
        property_id: PropertyId,
        target: &mut FnvHashMap<PropertyId, Select>,
        parent_predicate: &dyn Fn(PropertyId) -> bool,
        child_func: &dyn Fn(&mut FnvHashMap<PropertyId, Select>),
    ) {
        fn handle_child_select(
            target: &mut FnvHashMap<PropertyId, Select>,
            property_id: PropertyId,
            parent_def_id: DefId,
            parent_predicate: impl Fn(PropertyId) -> bool,
            child_func: impl Fn(&mut FnvHashMap<PropertyId, Select>),
        ) {
            if !parent_predicate(property_id) {
                return;
            }

            let child_select = target.entry(property_id).or_insert_with(|| {
                Select::Struct(StructSelect {
                    def_id: parent_def_id,
                    properties: Default::default(),
                })
            });
            child_func(match child_select {
                Select::Struct(struct_select) => &mut struct_select.properties,
                _ => todo!(),
            });
        }

        let def_id = self.find_def_id(property_id).unwrap();
        let mut is_root = true;

        for property_flow in self.property_flows_for(property_id) {
            if let PropertyFlowData::ChildOf(parent_property_id) = &property_flow.data {
                is_root = false;
                self.with_parent_select(
                    *parent_property_id,
                    target,
                    parent_predicate,
                    &|child_properties| {
                        handle_child_select(
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
            handle_child_select(target, property_id, def_id, parent_predicate, child_func);
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
                    let type_info = self.ontology.get_type_info(*def_id);
                    is_entity = type_info.entity_info().is_some();
                }
                PropertyFlowData::Cardinality((
                    PropertyCardinality::Mandatory,
                    ValueCardinality::Unit,
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
            .prop_flow_slice
            .binary_search_by(|flow| match flow.id.cmp(&property_id) {
                Ordering::Equal => Ordering::Greater,
                ord => ord,
            })
            .unwrap_or_else(|err| err);

        self.prop_flow_slice[lower_bound..]
            .iter()
            .take_while(move |flow| flow.id == property_id)
    }
}
