use std::sync::Arc;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{env::Env, serde::SerdeOperatorId, DefId, PackageId};
use smartstring::alias::String;

pub struct DomainData {
    pub env: Arc<Env>,
    pub package_id: PackageId,

    pub types: FnvHashMap<SerdeOperatorId, TypeData>,
    pub entities: FnvHashMap<SerdeOperatorId, EntityData>,
    pub unions: FnvHashMap<SerdeOperatorId, UnionData>,
    pub edges: FnvHashMap<(Option<DefId>, SerdeOperatorId), EdgeData>,

    pub query_type_name: String,
    pub mutation_type_name: String,

    pub queries: IndexMap<String, SerdeOperatorId>,
    pub mutations: IndexMap<String, MutationData>,
}

pub struct MutationData {
    pub entity_operator_id: SerdeOperatorId,
    pub kind: MutationKind,
}

#[derive(Clone, Copy)]
pub enum MutationKind {
    Create {
        input: SerdeOperatorId,
    },
    Update {
        id: SerdeOperatorId,
        input: SerdeOperatorId,
    },
    Delete {
        id: SerdeOperatorId,
    },
}

pub struct EntityData {
    pub def_id: DefId,
    pub id_operator_id: SerdeOperatorId,
}

pub struct TypeData {
    pub type_name: String,
    pub input_type_name: String,
    pub def_id: DefId,
    pub operator_id: SerdeOperatorId,
    pub fields: IndexMap<String, Field>,
}

pub struct UnionData {
    pub type_name: String,
    pub operator_id: SerdeOperatorId,
    pub variants: Vec<SerdeOperatorId>,
}

#[derive(Clone)]
pub struct ScalarData {
    _serde_operator_id: SerdeOperatorId,
}

pub struct EdgeData {
    pub edge_type_name: String,
    pub connection_type_name: String,
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

pub enum FieldKind {
    Scalar(SerdeOperatorId),
    Edge {
        node: SerdeOperatorId,
        rel: Option<SerdeOperatorId>,
    },
    Node(SerdeOperatorId),
    EntityRelationship {
        subject: DefId,
        node: SerdeOperatorId,
        rel: Option<SerdeOperatorId>,
    },
}
