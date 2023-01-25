use std::collections::HashMap;

use smartstring::alias::String;

use crate::{
    def::{DefId, DefKind, Defs, Namespaces},
    env::Env,
    env_queries::{GetDefType, GetPropertyMeta},
    mem::Mem,
    relation::{Properties, Relations, SubjectProperties},
    serde::{MapType, SerdeOperator, SerdeOperatorKind, SerdeProperty, ValueType},
    types::{DefTypes, Type},
    PackageId,
};

/// A binding to a specific domain,
/// so it may be interacted with from the external world
pub struct DomainBinding<'m> {
    serde_operators: HashMap<String, SerdeOperator<'m>>,
}

impl<'m> DomainBinding<'m> {
    pub fn get_serde_operator(&self, type_name: &str) -> Option<SerdeOperator<'m>> {
        self.serde_operators.get(type_name).cloned()
    }
}

#[derive(Debug)]
pub struct Bindings<'m> {
    mem: &'m Mem,
    serde_operators: HashMap<DefId, Option<SerdeOperator<'m>>>,
}

impl<'m> Bindings<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            serde_operators: Default::default(),
        }
    }
}

impl<'m> Env<'m> {
    pub fn bindings_builder<'e>(&'e mut self) -> BindingsBuilder<'e, 'm> {
        BindingsBuilder {
            bindings: &mut self.bindings,
            namespaces: &self.namespaces,
            defs: &self.defs,
            def_types: &self.def_types,
            relations: &self.relations,
        }
    }
}

pub struct BindingsBuilder<'e, 'm> {
    bindings: &'e mut Bindings<'m>,
    namespaces: &'e Namespaces,
    defs: &'e Defs,
    def_types: &'e DefTypes<'m>,
    relations: &'e Relations,
}

impl<'e, 'm> BindingsBuilder<'e, 'm> {
    pub fn new_binding(&mut self, package_id: PackageId) -> DomainBinding<'m> {
        let namespace = self
            .namespaces
            .namespaces
            .get(&package_id)
            .expect("package id does not exist, cannot create binding");

        let serde_operators = namespace
            .iter()
            .filter_map(
                |(typename, type_def_id)| match self.get_serde_operator(*type_def_id) {
                    Some(operator) => Some((typename.clone(), operator)),
                    None => None,
                },
            )
            .collect();

        DomainBinding { serde_operators }
    }

    fn get_serde_operator(&mut self, type_def_id: DefId) -> Option<SerdeOperator<'m>> {
        if let Some(operator) = self.bindings.serde_operators.get(&type_def_id) {
            return *operator;
        }

        let operator = self.create_serde_operator(type_def_id);
        self.bindings.serde_operators.insert(type_def_id, operator);
        operator
    }

    fn create_serde_operator(&mut self, type_def_id: DefId) -> Option<SerdeOperator<'m>> {
        match self.get_def_type(type_def_id) {
            Type::Number => Some(SerdeOperator(self.bump().alloc(SerdeOperatorKind::Number))),
            Type::String => Some(SerdeOperator(self.bump().alloc(SerdeOperatorKind::String))),
            Type::Domain(def_id) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::Type(ident)) => ident.clone(),
                    _ => "Unknown type".into(),
                };
                self.create_domain_type_serde_operator(typename, properties)
            }
            _ => None,
        }
    }

    fn create_domain_type_serde_operator(
        &mut self,
        typename: String,
        properties: Option<&Properties>,
    ) -> Option<SerdeOperator<'m>> {
        match properties.map(|prop| &prop.subject) {
            Some(SubjectProperties::Unit) | None => Some(SerdeOperator(self.bump().alloc(
                SerdeOperatorKind::MapType(MapType {
                    typename,
                    properties: Default::default(),
                }),
            ))),
            Some(SubjectProperties::Anonymous(property_id)) => {
                let Ok((_, relationship, _)) = self.get_property_meta(*property_id) else {
                    panic!("Problem getting property meta");
                };

                let operator = self
                    .get_serde_operator(relationship.object)
                    .expect("No inner serializer");

                Some(SerdeOperator(self.bump().alloc(
                    SerdeOperatorKind::ValueType(ValueType {
                        typename,
                        property: SerdeProperty {
                            property_id: *property_id,
                            operator,
                        },
                    }),
                )))
            }
            Some(SubjectProperties::Named(properties)) => {
                let serde_properties = properties.iter().map(|property_id| {
                    let Ok((_, relationship, relation)) = self.get_property_meta(*property_id) else {
                        panic!("Problem getting property meta");
                    };

                    let object_key = relation.object_prop().expect("Property has no name").clone();
                    let operator = self
                        .get_serde_operator(relationship.object)
                        .expect("No inner serializer");

                    (
                        object_key,
                        SerdeProperty {
                            property_id: *property_id,
                            operator,
                        }
                    )
                }).collect::<HashMap<_, _>>();

                Some(SerdeOperator(self.bump().alloc(
                    SerdeOperatorKind::MapType(MapType {
                        typename,
                        properties: serde_properties,
                    }),
                )))
            }
        }
    }

    fn bump(&self) -> &'m bumpalo::Bump {
        &self.bindings.mem.bump
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
