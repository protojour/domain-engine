use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{DefId, DefKind, Defs},
    namespace::{Namespaces, Space},
    relation::{Properties, Relations, SubjectProperties},
    serde::{MapType, SerdeOperator, SerdeOperatorId, SerdeProperty, ValueType},
    types::{DefTypes, Type},
    PackageId,
};

/// A binding to a specific domain,
/// so it may be interacted with from the external world
pub struct DomainBinding {
    pub(crate) serde_operators: HashMap<String, SerdeOperatorId>,
}

#[derive(Default, Debug)]
pub struct Bindings {
    pub(crate) serde_operators: Vec<SerdeOperator>,
    pub(crate) serde_operator_def_cache: HashMap<DefId, SerdeOperatorId>,
}

impl<'m> Compiler<'m> {
    pub fn bindings_builder<'e>(&'e mut self) -> BindingsBuilder<'e, 'm> {
        BindingsBuilder {
            type_stack: Default::default(),
            bindings: &mut self.bindings,
            namespaces: &self.namespaces,
            defs: &self.defs,
            def_types: &self.def_types,
            relations: &self.relations,
        }
    }
}

pub struct BindingsBuilder<'e, 'm> {
    type_stack: HashSet<DefId>,
    bindings: &'e mut Bindings,
    namespaces: &'e Namespaces,
    defs: &'e Defs,
    def_types: &'e DefTypes<'m>,
    relations: &'e Relations,
}

impl<'e, 'm> BindingsBuilder<'e, 'm> {
    fn get_serde_operator_id(&mut self, type_def_id: DefId) -> Option<SerdeOperatorId> {
        if let Some(id) = self.bindings.serde_operator_def_cache.get(&type_def_id) {
            return Some(*id);
        }

        if let Some((operator_id, kind)) = self.create_serde_operator(type_def_id) {
            self.bindings.serde_operators[operator_id.0 as usize] = kind;
            Some(operator_id)
        } else {
            None
        }
    }

    fn create_serde_operator(
        &mut self,
        type_def_id: DefId,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match self.get_def_type(type_def_id) {
            Some(Type::Number) => {
                Some((self.alloc_operator_id(type_def_id), SerdeOperator::Number))
            }
            Some(Type::String) => {
                Some((self.alloc_operator_id(type_def_id), SerdeOperator::String))
            }
            Some(Type::Domain(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::Type(ident)) => ident.clone(),
                    _ => "Unknown type".into(),
                };
                let operator_id = self.alloc_operator_id(type_def_id);
                Some((
                    operator_id,
                    self.create_domain_type_serde_operator(typename, properties),
                ))
            }
            _ => None,
        }
    }

    fn alloc_operator_id(&mut self, def_id: DefId) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.bindings.serde_operators.len() as u32);
        // We just need a temporary placeholder for this operator kind,
        // this will be properly overwritten after it's created:
        self.bindings.serde_operators.push(SerdeOperator::Unit);
        self.bindings
            .serde_operator_def_cache
            .insert(def_id, operator_id);
        operator_id
    }

    fn create_domain_type_serde_operator(
        &mut self,
        typename: String,
        properties: Option<&Properties>,
    ) -> SerdeOperator {
        match properties.map(|prop| &prop.subject) {
            Some(SubjectProperties::Unit) | None => SerdeOperator::MapType(MapType {
                typename,
                properties: Default::default(),
            }),
            Some(SubjectProperties::Anonymous(property_id)) => {
                let Ok((_, relationship, _)) = self.get_property_meta(*property_id) else {
                    panic!("Problem getting property meta");
                };

                let operator_id = self
                    .get_serde_operator_id(relationship.object)
                    .expect("No inner operator");

                SerdeOperator::ValueType(ValueType {
                    typename,
                    property: SerdeProperty {
                        property_id: *property_id,
                        operator_id,
                    },
                })
            }
            Some(SubjectProperties::Named(properties)) => {
                let serde_properties = properties.iter().map(|property_id| {
                    let Ok((_, relationship, relation)) = self.get_property_meta(*property_id) else {
                        panic!("Problem getting property meta");
                    };

                    let object_key = relation.object_prop().expect("Property has no name").clone();
                    let operator_id =
                        self.get_serde_operator_id(relationship.object)
                            .expect("No inner operator");

                    (
                        object_key,
                        SerdeProperty {
                            property_id: *property_id,
                            operator_id,
                        }
                    )
                }).collect::<IndexMap<_, _>>();

                SerdeOperator::MapType(MapType {
                    typename,
                    properties: serde_properties,
                })
            }
        }
    }
}

impl<'e, 'm> BindingsBuilder<'e, 'm> {
    pub fn new_binding(&'e mut self, package_id: PackageId) -> DomainBinding {
        let namespace = self
            .namespaces
            .namespaces
            .get(&package_id)
            .expect("package id does not exist, cannot create binding");

        let serde_operators = namespace
            .space(Space::Type)
            .iter()
            .filter_map(|(typename, type_def_id)| {
                self.get_serde_operator_id(*type_def_id)
                    .map(|kind| (typename.clone(), kind))
            })
            .collect();

        DomainBinding { serde_operators }
    }
}

impl<'e, 'm> AsRef<Defs> for BindingsBuilder<'e, 'm> {
    fn as_ref(&self) -> &Defs {
        &self.defs
    }
}

impl<'e, 'm> AsRef<DefTypes<'m>> for BindingsBuilder<'e, 'm> {
    fn as_ref(&self) -> &DefTypes<'m> {
        &self.def_types
    }
}

impl<'e, 'm> AsRef<Relations> for BindingsBuilder<'e, 'm> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}
