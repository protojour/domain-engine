use std::sync::Arc;

use ontol_runtime::serde::{SerdeOperator, SerdeOperatorId};

use self::data::{DomainData, EdgeData, EntityData, ScalarData, TypeData};

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
