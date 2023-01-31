use std::collections::HashMap;

use indexmap::IndexMap;
use ontol_runtime::{
    serde::{MapType, SerdeOperator, SerdeOperatorId, SerdeProperty, ValueType, ValueUnionType},
    DefId,
};
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{DefKind, Defs},
    relation::{Properties, Relations, SubjectProperties},
    types::{DefTypes, Type},
};

impl<'m> Compiler<'m> {
    pub fn serde_generator(&self) -> SerdeGenerator<'_, 'm> {
        SerdeGenerator {
            defs: &self.defs,
            def_types: &self.def_types,
            relations: &self.relations,
            serde_operators: Default::default(),
            serde_operator_def_cache: Default::default(),
        }
    }
}

pub struct SerdeGenerator<'c, 'm> {
    defs: &'c Defs,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,
    serde_operators: Vec<SerdeOperator>,
    serde_operator_def_cache: HashMap<DefId, SerdeOperatorId>,
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(self) -> Vec<SerdeOperator> {
        self.serde_operators
    }

    pub fn get_serde_operator_id(&mut self, type_def_id: DefId) -> Option<SerdeOperatorId> {
        if let Some(id) = self.serde_operator_def_cache.get(&type_def_id) {
            return Some(*id);
        }

        if let Some((operator_id, kind)) = self.create_serde_operator(type_def_id) {
            self.serde_operators[operator_id.0 as usize] = kind;
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
        let operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        // We just need a temporary placeholder for this operator kind,
        // this will be properly overwritten after it's created:
        self.serde_operators.push(SerdeOperator::Unit);
        self.serde_operator_def_cache.insert(def_id, operator_id);
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
            Some(SubjectProperties::Value(property_id, _)) => {
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
            Some(SubjectProperties::ValueUnion(properties)) => {
                let serde_properties = properties
                    .iter()
                    .map(|(property_id, _)| {
                        let Ok((_, relationship, _)) = self.get_property_meta(*property_id) else {
                            panic!("Problem getting property meta");
                        };

                        let operator_id = self
                            .get_serde_operator_id(relationship.object)
                            .expect("No inner operator");

                        SerdeProperty {
                            property_id: *property_id,
                            operator_id,
                        }
                    })
                    .collect::<Vec<_>>();

                SerdeOperator::ValueUnionType(ValueUnionType {
                    typename,
                    properties: serde_properties,
                })
            }
            Some(SubjectProperties::Map(property_set)) => {
                let serde_properties = property_set.iter().map(|property_id| {
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

impl<'c, 'm> AsRef<Defs> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Defs {
        &self.defs
    }
}

impl<'c, 'm> AsRef<DefTypes<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &DefTypes<'m> {
        &self.def_types
    }
}

impl<'c, 'm> AsRef<Relations> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}
