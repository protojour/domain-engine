use std::sync::Arc;

use ontol_runtime::{
    serde::{SerdeOperator, SerdeOperatorId},
    DefId,
};
use tracing::debug;

use self::data::{DomainData, EdgeData, EntityData, ScalarData, TypeData, UnionData};

pub mod adapt;
pub mod data;

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

    pub fn node_adapter(&self, operator_id: SerdeOperatorId) -> TypeAdapter<NodeKind> {
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
            subject: None,
            node_operator_id: entity_ref.type_data.operator_id,
        }
    }
}

pub trait Kind {
    type DataRef<'d>;

    fn get_data_ref(domain_data: &DomainData, operator_id: SerdeOperatorId) -> Self::DataRef<'_>;
}

/// Fully dynamic type (node, scalar, anything)
#[derive(Clone)]
pub struct DynamicKind;

/// Any data that is not a scalar
#[derive(Clone)]
pub struct NodeKind;

#[derive(Clone)]
pub struct EntityKind;

#[derive(Clone)]
pub struct ScalarKind;

impl Kind for DynamicKind {
    type DataRef<'d> = DynamicRef<'d>;

    fn get_data_ref(domain_data: &DomainData, operator_id: SerdeOperatorId) -> Self::DataRef<'_> {
        debug!("dynamic get_data_ref {operator_id:?}");
        if let Some(union_data) = domain_data.unions.get(&operator_id) {
            DynamicRef::Union(UnionRef { union_data })
        } else {
            let type_data = domain_data
                .types
                .get(&operator_id)
                .expect("BUG: Type data for non-union not found");

            if let Some(entity_data) = domain_data.entities.get(&operator_id) {
                DynamicRef::Entity(EntityRef {
                    entity_data,
                    type_data,
                })
            } else {
                DynamicRef::Node(NodeRef {
                    entity_data: None,
                    type_data,
                })
            }
        }
    }
}

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
    pub _kind: std::marker::PhantomData<K>,
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
        self.domain_data
            .types
            .get(&self.operator_id)
            .expect("Type has not been registered")
    }
}

#[derive(Clone)]
pub struct UnionAdapter {
    pub domain_data: Arc<DomainData>,
    pub operator_id: SerdeOperatorId,
}

impl UnionAdapter {
    pub fn data(&self) -> &UnionData {
        self.domain_data
            .unions
            .get(&self.operator_id)
            .expect("No union data found")
    }
}

#[derive(Clone)]
pub struct EdgeAdapter {
    pub domain_data: Arc<DomainData>,
    pub subject: Option<DefId>,
    pub node_operator_id: SerdeOperatorId,
}

impl EdgeAdapter {
    pub fn data(&self) -> &EdgeData {
        self.domain_data
            .edges
            .get(&(self.subject, self.node_operator_id))
            .expect("No edge data found")
    }

    pub fn node_adapter(&self) -> TypeAdapter<DynamicKind> {
        TypeAdapter {
            domain_data: self.domain_data.clone(),
            operator_id: self.node_operator_id,
            _kind: std::marker::PhantomData,
        }
    }
}

#[derive(Copy, Clone)]
pub enum DynamicRef<'d> {
    Node(NodeRef<'d>),
    Entity(EntityRef<'d>),
    Union(UnionRef<'d>),
    Scalar(ScalarRef<'d>),
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
pub struct UnionRef<'d> {
    pub union_data: &'d UnionData,
}

#[derive(Copy, Clone)]
pub struct ScalarRef<'d> {
    pub scalar_data: &'d ScalarData,
    pub type_data: &'d TypeData,
}
