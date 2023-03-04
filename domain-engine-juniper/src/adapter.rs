use std::{collections::HashMap, sync::Arc};

use ontol_runtime::{
    env::Env,
    serde::{SerdeOperator, SerdeOperatorId},
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

    let mut types: HashMap<_, _> = Default::default();
    let mut entities: HashMap<_, _> = Default::default();
    let mut root_edges: HashMap<_, _> = Default::default();

    for (typename, type_info) in &domain.types {
        let serde_operator_id = match type_info.serde_operator_id {
            Some(id) => id,
            None => continue,
        };

        types.insert(
            type_info.def_id,
            TypeData {
                type_name: typename.clone(),
                serde_operator_id,
            },
        );

        if type_info.entity_id.is_some() {
            entities.insert(
                type_info.def_id,
                EntityData {
                    def_id: type_info.def_id,
                },
            );

            root_edges.insert(
                type_info.def_id,
                EdgeData {
                    edge_type_name: smart_format!("{typename}ConnectionEdge"),
                    connection_type_name: smart_format!("{typename}Connection"),
                },
            );
        }
    }

    let domain_data = DomainData {
        env,
        package_id,
        types,
        entities,
        root_edges,
    };

    Ok(DomainAdapter {
        domain_data: Arc::new(domain_data),
    })
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

    pub fn type_adapter(&self, def_id: DefId) -> TypeAdapter<DynamicTypeKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            def_id,
            _kind: std::marker::PhantomData,
        }
    }

    pub fn entity_adapter(&self, entity_ref: &EntityRef) -> TypeAdapter<EntityKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            def_id: entity_ref.entity_data.def_id,
            _kind: std::marker::PhantomData,
        }
    }

    pub fn root_edge_adapter(&self, entity_ref: &EntityRef) -> EdgeAdapter {
        EdgeAdapter {
            domain_data: self.domain_data.clone(),
            node_def_id: entity_ref.entity_data.def_id,
        }
    }
}

pub trait Kind {
    type DataRef<'d>;

    fn get_data_ref(domain_data: &DomainData, def_id: DefId) -> Self::DataRef<'_>;
}

#[derive(Clone)]
pub struct DynamicTypeKind;

#[derive(Clone)]
pub struct EntityKind;

#[derive(Clone)]
pub struct ScalarKind;

impl Kind for DynamicTypeKind {
    type DataRef<'d> = DynamicTypeRef<'d>;

    fn get_data_ref(domain_data: &DomainData, def_id: DefId) -> Self::DataRef<'_> {
        let type_data = domain_data.types.get(&def_id).unwrap();

        if let Some(entity_data) = domain_data.entities.get(&def_id) {
            DynamicTypeRef::Entity(EntityRef {
                entity_data,
                type_data,
            })
        } else {
            DynamicTypeRef::Type(type_data)
        }
    }
}

impl Kind for EntityKind {
    type DataRef<'d> = EntityRef<'d>;

    fn get_data_ref(domain_data: &DomainData, def_id: DefId) -> Self::DataRef<'_> {
        let entity_data = domain_data.entities.get(&def_id).unwrap();
        let type_data = domain_data.types.get(&def_id).unwrap();

        EntityRef {
            entity_data,
            type_data,
        }
    }
}

impl Kind for ScalarKind {
    type DataRef<'d> = ScalarRef<'d>;

    fn get_data_ref(_domain_data: &DomainData, _def_id: DefId) -> Self::DataRef<'_> {
        todo!()
    }
}

#[derive(Clone)]
pub struct TypeAdapter<K: Kind> {
    pub domain_data: Arc<DomainData>,
    pub def_id: DefId,
    _kind: std::marker::PhantomData<K>,
}

impl<K: Kind> TypeAdapter<K> {
    pub fn data(&self) -> <K as Kind>::DataRef<'_> {
        <K as Kind>::get_data_ref(&self.domain_data, self.def_id)
    }

    pub fn serde_operator(&self) -> &SerdeOperator {
        self.domain_data
            .env
            .get_serde_operator(self.type_data().serde_operator_id)
    }

    pub fn type_data(&self) -> &TypeData {
        self.domain_data.types.get(&self.def_id).unwrap()
    }
}

#[derive(Clone)]
pub struct EdgeAdapter {
    pub domain_data: Arc<DomainData>,
    pub node_def_id: DefId,
}

impl EdgeAdapter {
    pub fn data(&self) -> &EdgeData {
        self.domain_data.root_edges.get(&self.node_def_id).unwrap()
    }

    pub fn node_adapter(&self) -> TypeAdapter<DynamicTypeKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            def_id: self.node_def_id,
            _kind: std::marker::PhantomData,
        }
    }
}

pub struct DomainData {
    pub env: Arc<Env>,
    pub package_id: PackageId,

    pub types: HashMap<DefId, TypeData>,
    pub entities: HashMap<DefId, EntityData>,
    pub root_edges: HashMap<DefId, EdgeData>,
}

#[derive(Clone)]
pub enum DynamicTypeRef<'d> {
    Entity(EntityRef<'d>),
    Type(&'d TypeData),
    Scalar(ScalarRef<'d>),
}

#[derive(Clone)]
pub struct EntityRef<'d> {
    pub entity_data: &'d EntityData,
    pub type_data: &'d TypeData,
}

#[derive(Clone)]
pub struct ScalarRef<'d> {
    pub scalar_data: &'d ScalarData,
    pub type_data: &'d TypeData,
}

pub struct EntityData {
    pub def_id: DefId,
}

pub struct TypeData {
    pub type_name: String,
    pub serde_operator_id: SerdeOperatorId,
}

#[derive(Clone)]
pub struct ScalarData {
    _serde_operator_id: SerdeOperatorId,
}

pub struct EdgeData {
    pub edge_type_name: String,
    pub connection_type_name: String,
}
