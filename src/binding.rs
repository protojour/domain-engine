use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    def::{DefId, DefKind, Defs},
    env::Env,
    env_queries::{GetDefType, GetPropertyMeta},
    mem::Mem,
    namespace::{Namespaces, Space},
    relation::{Properties, Relations, SubjectProperties},
    serde::{MapType, SerdeOperator, SerdeOperatorKind, SerdeProperty, ValueType},
    types::{DefTypes, Type},
    PackageId,
};

/// A binding to a specific domain,
/// so it may be interacted with from the external world
pub struct DomainBinding<'m> {
    serde_operator_kinds: HashMap<String, &'m SerdeOperatorKind<'m>>,
}

impl<'m> DomainBinding<'m> {
    pub fn get_serde_operator<'e>(
        &self,
        env: &'e Env<'m>,
        type_name: &str,
    ) -> Option<SerdeOperator<'e, 'm>> {
        self.serde_operator_kinds
            .get(type_name)
            .map(|kind| SerdeOperator {
                kind,
                bindings: &env.bindings,
            })
    }
}

#[derive(Debug)]
pub struct Bindings<'m> {
    mem: &'m Mem,
    pub(crate) serde_operator_kinds: HashMap<DefId, Option<&'m SerdeOperatorKind<'m>>>,
}

impl<'m> Bindings<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            serde_operator_kinds: Default::default(),
        }
    }
}

impl<'m> Env<'m> {
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
    bindings: &'e mut Bindings<'m>,
    namespaces: &'e Namespaces,
    defs: &'e Defs,
    def_types: &'e DefTypes<'m>,
    relations: &'e Relations,
}

impl<'e, 'm> BindingsBuilder<'e, 'm> {
    pub fn new_binding(&'e mut self, package_id: PackageId) -> DomainBinding<'m> {
        let namespace = self
            .namespaces
            .namespaces
            .get(&package_id)
            .expect("package id does not exist, cannot create binding");

        let serde_operator_kinds = namespace
            .space(Space::Type)
            .iter()
            .filter_map(|(typename, type_def_id)| {
                self.get_or_create_serde_operator_kind(*type_def_id)
                    .map(|kind| (typename.clone(), kind))
            })
            .collect();

        DomainBinding {
            serde_operator_kinds,
        }
    }

    fn get_or_create_serde_operator_kind(
        &mut self,
        type_def_id: DefId,
    ) -> Option<&'m SerdeOperatorKind<'m>> {
        if self.type_stack.contains(&type_def_id) {
            return Some(self.bump().alloc(SerdeOperatorKind::Recursive(type_def_id)));
        }

        if let Some(kind) = self.bindings.serde_operator_kinds.get(&type_def_id) {
            return *kind;
        }

        self.type_stack.insert(type_def_id);
        let kind = self.create_serde_operator_kind(type_def_id);
        self.type_stack.remove(&type_def_id);

        self.bindings.serde_operator_kinds.insert(type_def_id, kind);
        kind
    }

    fn create_serde_operator_kind(
        &mut self,
        type_def_id: DefId,
    ) -> Option<&'m SerdeOperatorKind<'m>> {
        match self.get_def_type(type_def_id) {
            Some(Type::Number) => Some(self.bump().alloc(SerdeOperatorKind::Number)),
            Some(Type::String) => Some(self.bump().alloc(SerdeOperatorKind::String)),
            Some(Type::Domain(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::Type(ident)) => ident.clone(),
                    _ => "Unknown type".into(),
                };
                self.create_domain_type_serde_operator_kind(typename, properties)
            }
            _ => None,
        }
    }

    fn create_domain_type_serde_operator_kind(
        &mut self,
        typename: String,
        properties: Option<&Properties>,
    ) -> Option<&'m SerdeOperatorKind<'m>> {
        match properties.map(|prop| &prop.subject) {
            Some(SubjectProperties::Unit) | None => {
                Some(self.bump().alloc(SerdeOperatorKind::MapType(MapType {
                    typename,
                    properties: Default::default(),
                })))
            }
            Some(SubjectProperties::Anonymous(property_id)) => {
                let Ok((_, relationship, _)) = self.get_property_meta(*property_id) else {
                    panic!("Problem getting property meta");
                };

                let kind = self
                    .get_or_create_serde_operator_kind(relationship.object)
                    .expect("No inner serializer");

                Some(self.bump().alloc(SerdeOperatorKind::ValueType(ValueType {
                    typename,
                    property: SerdeProperty {
                        property_id: *property_id,
                        kind,
                    },
                })))
            }
            Some(SubjectProperties::Named(properties)) => {
                let serde_properties = properties.iter().map(|property_id| {
                    let Ok((_, relationship, relation)) = self.get_property_meta(*property_id) else {
                        panic!("Problem getting property meta");
                    };

                    let object_key = relation.object_prop().expect("Property has no name").clone();
                    let kind =
                        self.get_or_create_serde_operator_kind(relationship.object)
                            .expect("No inner serializer");

                    (
                        object_key,
                        SerdeProperty {
                            property_id: *property_id,
                            kind,
                        }
                    )
                }).collect::<IndexMap<_, _>>();

                Some(self.bump().alloc(SerdeOperatorKind::MapType(MapType {
                    typename,
                    properties: serde_properties,
                })))
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
