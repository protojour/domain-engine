use std::{collections::HashMap, sync::Arc};

use indexmap::IndexMap;
use ontol_runtime::{
    env::{Env, TypeInfo},
    serde::{MapType, SerdeOperator, SerdeOperatorId},
    smart_format, DefId, PackageId,
};
use smartstring::alias::String;

use crate::SchemaBuildError;

pub fn adapt_domain(
    env: Arc<Env>,
    package_id: PackageId,
) -> Result<DomainAdapter, SchemaBuildError> {
    let domain = env
        .get_domain(&package_id)
        .ok_or(SchemaBuildError::UnknownPackage)?;

    let mut domain_data = DomainData {
        env: env.clone(),
        package_id,
        types: Default::default(),
        entities: Default::default(),
        root_edges: Default::default(),
    };

    for (typename, type_info) in &domain.types {
        let serde_operator_id = match type_info.serde_operator_id {
            Some(id) => id,
            None => continue,
        };

        match env.get_serde_operator(serde_operator_id) {
            SerdeOperator::MapType(map_type) => {
                register_map_type(
                    &mut domain_data,
                    typename,
                    type_info,
                    serde_operator_id,
                    map_type,
                );
            }
            _ => {}
        };
    }

    Ok(DomainAdapter {
        domain_data: Arc::new(domain_data),
    })
}

fn register_map_type(
    domain_data: &mut DomainData,
    typename: &String,
    type_info: &TypeInfo,
    serde_operator_id: SerdeOperatorId,
    map_type: &MapType,
) {
    let env = domain_data.env.as_ref();

    let mut fields: IndexMap<String, Field> = Default::default();

    for (property_name, property) in &map_type.properties {
        match env.get_serde_operator(property.value_operator_id) {
            SerdeOperator::ValueUnionType(_) => {
                // let object_def_id = value_union_type.union_def_variant.id();
                // let object_type_info = env.find_type_info(object_def_id);
            }
            SerdeOperator::MapType(_) => {
                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality: if property.optional {
                            FieldCardinality::UnitOptional
                        } else {
                            FieldCardinality::UnitMandatory
                        },
                        kind: FieldKind::Node {
                            value: property.value_operator_id,
                            rel: property.rel_params_operator_id,
                        },
                    },
                );
            }
            SerdeOperator::Sequence(_) => {
                todo!()
            }
            _ => {
                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality: if property.optional {
                            FieldCardinality::UnitOptional
                        } else {
                            FieldCardinality::UnitMandatory
                        },
                        kind: FieldKind::Scalar(property.value_operator_id),
                    },
                );
            }
        }
    }

    if let Some(_) = type_info.entity_id {
        // fields.insert("_id".into(), FieldData::Id(entity_id));

        domain_data.entities.insert(
            serde_operator_id,
            EntityData {
                def_id: type_info.def_id,
            },
        );

        domain_data.root_edges.insert(
            serde_operator_id,
            EdgeData {
                edge_type_name: smart_format!("{typename}ConnectionEdge"),
                connection_type_name: smart_format!("{typename}Connection"),
            },
        );
    }

    domain_data.types.insert(
        serde_operator_id,
        TypeData {
            type_name: typename.clone(),
            operator_id: serde_operator_id,
            fields,
        },
    );
}

#[derive(Clone)]
pub struct DomainAdapter {
    pub domain_data: Arc<DomainData>,
}

impl DomainAdapter {
    pub fn iter_entities(&self) -> impl Iterator<Item = EntityRef> {
        self.domain_data.entities.keys().map(|def_id| {
            let entity_data = &self.domain_data.entities.get(def_id).unwrap();
            let type_data = &self.domain_data.types.get(def_id).unwrap();

            EntityRef {
                entity_data,
                type_data,
            }
        })
    }

    pub fn dynamic_type_adapter(&self, operator_id: SerdeOperatorId) -> TypeAdapter<NodeKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            operator_id,
            _kind: std::marker::PhantomData,
        }
    }

    pub fn entity_adapter(&self, entity_ref: &EntityRef) -> TypeAdapter<EntityKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            operator_id: entity_ref.type_data.operator_id,
            _kind: std::marker::PhantomData,
        }
    }

    pub fn root_edge_adapter(&self, entity_ref: &EntityRef) -> EdgeAdapter {
        EdgeAdapter {
            domain_data: self.domain_data.clone(),
            operator_id: entity_ref.type_data.operator_id,
        }
    }
}

pub trait Kind {
    type DataRef<'d>;

    fn get_data_ref(domain_data: &DomainData, operator_id: SerdeOperatorId) -> Self::DataRef<'_>;
}

/// Any data that is not a scalar
#[derive(Clone)]
pub struct NodeKind;

#[derive(Clone)]
pub struct EntityKind;

#[derive(Clone)]
pub struct ScalarKind;

impl Kind for NodeKind {
    type DataRef<'d> = NodeRef<'d>;

    fn get_data_ref(domain_data: &DomainData, operator_id: SerdeOperatorId) -> Self::DataRef<'_> {
        let type_data = domain_data.types.get(&operator_id).unwrap();

        NodeRef {
            entity_data: domain_data.entities.get(&operator_id),
            type_data,
        }
    }
}

impl Kind for EntityKind {
    type DataRef<'d> = EntityRef<'d>;

    fn get_data_ref(domain_data: &DomainData, operator_id: SerdeOperatorId) -> Self::DataRef<'_> {
        let entity_data = domain_data.entities.get(&operator_id).unwrap();
        let type_data = domain_data.types.get(&operator_id).unwrap();

        EntityRef {
            entity_data,
            type_data,
        }
    }
}

impl Kind for ScalarKind {
    type DataRef<'d> = ScalarRef<'d>;

    fn get_data_ref(_domain_data: &DomainData, _operator_id: SerdeOperatorId) -> Self::DataRef<'_> {
        todo!()
    }
}

#[derive(Clone)]
pub struct TypeAdapter<K: Kind> {
    pub domain_data: Arc<DomainData>,
    pub operator_id: SerdeOperatorId,
    _kind: std::marker::PhantomData<K>,
}

impl<K: Kind> TypeAdapter<K> {
    pub fn data(&self) -> <K as Kind>::DataRef<'_> {
        <K as Kind>::get_data_ref(&self.domain_data, self.operator_id)
    }

    pub fn serde_operator(&self) -> &SerdeOperator {
        self.domain_data
            .env
            .get_serde_operator(self.type_data().operator_id)
    }

    pub fn type_data(&self) -> &TypeData {
        self.domain_data.types.get(&self.operator_id).unwrap()
    }
}

#[derive(Clone)]
pub struct EdgeAdapter {
    pub domain_data: Arc<DomainData>,
    pub operator_id: SerdeOperatorId,
}

impl EdgeAdapter {
    pub fn data(&self) -> &EdgeData {
        self.domain_data.root_edges.get(&self.operator_id).unwrap()
    }

    pub fn node_adapter(&self) -> TypeAdapter<NodeKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            operator_id: self.operator_id,
            _kind: std::marker::PhantomData,
        }
    }
}

pub struct DomainData {
    pub env: Arc<Env>,
    pub package_id: PackageId,

    pub types: HashMap<SerdeOperatorId, TypeData>,
    pub entities: HashMap<SerdeOperatorId, EntityData>,
    pub root_edges: HashMap<SerdeOperatorId, EdgeData>,
}

#[derive(Copy, Clone)]
pub struct NodeRef<'d> {
    pub entity_data: Option<&'d EntityData>,
    pub type_data: &'d TypeData,
}

#[derive(Copy, Clone)]
pub struct EntityRef<'d> {
    pub entity_data: &'d EntityData,
    pub type_data: &'d TypeData,
}

#[derive(Copy, Clone)]
pub struct ScalarRef<'d> {
    pub scalar_data: &'d ScalarData,
    pub type_data: &'d TypeData,
}

pub struct EntityData {
    pub def_id: DefId,
}

pub struct TypeData {
    pub type_name: String,
    pub operator_id: SerdeOperatorId,
    pub fields: IndexMap<String, Field>,
}

pub struct Field {
    pub cardinality: FieldCardinality,
    pub kind: FieldKind,
}

pub enum FieldCardinality {
    UnitMandatory,
    UnitOptional,
    ManyMandatory,
    ManyOptional,
}

// TODO: Cardinalities
pub enum FieldKind {
    Scalar(SerdeOperatorId),
    Node {
        value: SerdeOperatorId,
        rel: Option<SerdeOperatorId>,
    },
    EntityRelationship {},
}

#[derive(Clone)]
pub struct ScalarData {
    _serde_operator_id: SerdeOperatorId,
}

pub struct EdgeData {
    pub edge_type_name: String,
    pub connection_type_name: String,
}
