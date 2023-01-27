use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{DefId, DefKind, Defs},
    mem::Mem,
    namespace::{Namespaces, Space},
    relation::{Properties, Relations, SubjectProperties},
    serde::{
        MapType, MapType2, SerdeOperator, SerdeOperatorId, SerdeOperatorKind, SerdeOperatorOld,
        SerdeProperty, SerdeProperty2, ValueType, ValueType2,
    },
    types::{DefTypes, Type},
    PackageId,
};

/// A binding to a specific domain,
/// so it may be interacted with from the external world
pub struct DomainBinding<'m> {
    serde_operator_kinds: HashMap<String, &'m SerdeOperatorKind<'m>>,
    pub(crate) serde_operators: HashMap<String, SerdeOperatorId>,
}

impl<'m> DomainBinding<'m> {
    pub fn get_serde_operator_old<'e>(
        &self,
        compiler: &'e Compiler<'m>,
        type_name: &str,
    ) -> Option<SerdeOperatorOld<'e, 'm>> {
        self.serde_operator_kinds
            .get(type_name)
            .map(|kind| SerdeOperatorOld {
                kind,
                bindings: &compiler.bindings,
            })
    }

    /*
    pub fn get_serde_executor<'e>(&'e self, type_name: &str) -> Option<SerdeExecutor<'e>> {
        self.serde_operators
            .get(type_name)
            .map(|operator| SerdeExecutor {
                current: operator,
                all_operators: &self.
                bindings: &compiler.bindings,
            })
    }
    */
}

#[derive(Debug)]
pub struct Bindings<'m> {
    mem: &'m Mem,
    pub(crate) serde_operator_kinds: HashMap<DefId, Option<&'m SerdeOperatorKind<'m>>>,
    pub(crate) serde_operators: Vec<SerdeOperator>,
    pub(crate) serde_operator_def_cache: HashMap<DefId, SerdeOperatorId>,
}

impl<'m> Bindings<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            mem,
            serde_operator_kinds: Default::default(),
            serde_operators: Default::default(),
            serde_operator_def_cache: Default::default(),
        }
    }
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
    bindings: &'e mut Bindings<'m>,
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
            Some(SubjectProperties::Unit) | None => SerdeOperator::MapType(MapType2 {
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

                SerdeOperator::ValueType(ValueType2 {
                    typename,
                    property: SerdeProperty2 {
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
                        SerdeProperty2 {
                            property_id: *property_id,
                            operator_id,
                        }
                    )
                }).collect::<IndexMap<_, _>>();

                SerdeOperator::MapType(MapType2 {
                    typename,
                    properties: serde_properties,
                })
            }
        }
    }
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

        let serde_operators = namespace
            .space(Space::Type)
            .iter()
            .filter_map(|(typename, type_def_id)| {
                self.get_serde_operator_id(*type_def_id)
                    .map(|kind| (typename.clone(), kind))
            })
            .collect();

        DomainBinding {
            serde_operator_kinds,
            serde_operators,
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
