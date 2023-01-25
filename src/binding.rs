use std::collections::HashMap;

use smartstring::alias::String;
use thiserror::Error;

use crate::{
    def::DefId,
    env::Env,
    env_queries::{GetDefType, GetPropertyMeta},
    mem::Mem,
    relation::{Properties, PropertyId, SubjectProperties},
    serde::{SerdeOperator, Serder},
    types::Type,
    value::Value,
    PackageId,
};

/// A binding to a specific domain,
/// so it may be interacted with from the external world
pub struct DomainBinding<'e, 'm> {
    env: &'e Env<'m>,
    namespace: &'e HashMap<String, DefId>,
}

pub struct DomainBinding2<'m> {
    serders: HashMap<String, Serder<'m>>,
}

// pub struct DomainBinding2<'e, 'm> {}

#[derive(Debug, Error)]
pub enum InstantiateError {
    #[error("type not found")]
    TypeNotFound,
    #[error("expected number")]
    ExpectedNumber,
    #[error("expected string")]
    ExpectedString,
    #[error("expected object")]
    ExpectedObject,
    #[error("expected empty object")]
    ExpectedEmptyObject,
    #[error("expected key {0}")]
    ExpectedKey(String),
    #[error("excess object keys {0:?}")]
    ExcessKeys(Vec<String>),
    #[error("type not instantiable")]
    TypeNotInstantiable,
}

type InstantiateResult = Result<Value, InstantiateError>;

impl<'e, 'm> DomainBinding<'e, 'm> {
    /// Mainly for testing purposes
    pub fn instantiate_json(&self, type_name: &str, json: serde_json::Value) -> InstantiateResult {
        let type_def_id = self
            .namespace
            .get(type_name)
            .ok_or(InstantiateError::TypeNotFound)?;

        self.instatiate_slow(*type_def_id, json)
    }

    fn instatiate_slow(&self, type_def_id: DefId, json: serde_json::Value) -> InstantiateResult {
        match self.env.get_def_type(type_def_id) {
            Type::Number => Ok(Value::Number(
                json.as_i64().ok_or(InstantiateError::ExpectedNumber)?,
            )),
            Type::String => Ok(Value::String(
                json.as_str()
                    .ok_or(InstantiateError::ExpectedString)?
                    .into(),
            )),
            Type::Domain(def_id) => {
                let properties = self.env.relations.properties_by_type.get(def_id);
                self.instantiate_domain_type_slow(properties, json)
            }
            _ => Err(InstantiateError::TypeNotInstantiable),
        }
    }

    fn instantiate_domain_type_slow(
        &self,
        properties: Option<&Properties>,
        mut json: serde_json::Value,
    ) -> InstantiateResult {
        match properties.map(|prop| &prop.subject) {
            Some(SubjectProperties::Unit) | None => {
                let obj = json
                    .as_object()
                    .ok_or(InstantiateError::ExpectedEmptyObject)?;
                if !obj.is_empty() {
                    return Err(InstantiateError::ExpectedEmptyObject);
                }

                Ok(Value::Compound([].into()))
            }
            Some(SubjectProperties::Anonymous(property_id)) => {
                let Ok((_, relationship, _)) = self.env.get_property_meta(*property_id) else {
                    panic!("Problem getting property meta");
                };

                self.instatiate_slow(relationship.object, json)
            }
            Some(SubjectProperties::Named(properties)) => {
                let json_obj = json
                    .as_object_mut()
                    .ok_or(InstantiateError::ExpectedObject)?;

                let mut attributes: HashMap<PropertyId, Value> = Default::default();

                for property_id in properties {
                    let Ok((_, relationship, relation)) = self.env.get_property_meta(*property_id) else {
                        panic!("Problem getting property meta");
                    };

                    let object_key = relation.object_prop().expect("Property has no name");
                    let json_property_value = json_obj
                        .remove(object_key.as_str())
                        .ok_or_else(|| InstantiateError::ExpectedKey(object_key.clone()))?;
                    let value = self.instatiate_slow(relationship.object, json_property_value)?;

                    attributes.insert(*property_id, value);
                }

                if json_obj.len() > 0 {
                    return Err(InstantiateError::ExcessKeys(
                        json_obj.into_iter().map(|(key, _)| key.into()).collect(),
                    ));
                }

                Ok(Value::Compound(attributes))
            }
        }
    }

    fn instantiate_property_value(
        &self,
        property_id: PropertyId,
        json: serde_json::Value,
    ) -> InstantiateResult {
        panic!()
    }
}

#[derive(Debug)]
pub struct Bindings<'m> {
    mem: &'m Mem,
}

impl<'m> Bindings<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self { mem }
    }
}

impl<'m> Env<'m> {
    pub fn new_binding<'e>(&'e self, package_id: PackageId) -> DomainBinding<'e, 'm> {
        DomainBinding {
            env: self,
            namespace: self
                .namespaces
                .namespaces
                .get(&package_id)
                .expect("package id does not exist, cannot create binding"),
        }
    }

    pub fn new_binding2(&mut self, package_id: PackageId) -> DomainBinding2<'m> {
        let bindings = &self.bindings;
        let namespace = self
            .namespaces
            .namespaces
            .get(&package_id)
            .expect("package id does not exist, cannot create binding");

        let serders = namespace
            .iter()
            .filter_map(|(typename, type_def_id)| {
                match bindings.create_serder(*type_def_id, self) {
                    Some(serder) => Some((typename.clone(), serder)),
                    None => None,
                }
            })
            .collect();

        DomainBinding2 { serders }
    }
}

struct BindingsBuilder<'m> {
    lol: &'m String,
}

impl<'m> Bindings<'m> {
    fn create_serder(&self, type_def_id: DefId, env: &Env) -> Option<Serder<'m>> {
        match env.get_def_type(type_def_id) {
            Type::Number => Some(Serder(self.mem.bump.alloc(SerdeOperator::Number))),
            Type::String => Some(Serder(self.mem.bump.alloc(SerdeOperator::String))),
            Type::Domain(def_id) => panic!(),
            _ => None,
        }
    }
}
