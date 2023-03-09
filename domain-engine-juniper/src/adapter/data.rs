use std::sync::Arc;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{env::Env, serde::SerdeOperatorId, DefId, PackageId};
use smartstring::alias::String;

pub struct DomainData {
    pub env: Arc<Env>,
    pub package_id: PackageId,

    pub types: FnvHashMap<DefId, TypeData>,
    pub entities: FnvHashMap<DefId, EntityData>,
    pub unions: FnvHashMap<DefId, UnionData>,
    pub edges: FnvHashMap<(Option<DefId>, DefId), EdgeData>,

    pub query_type_name: String,
    pub mutation_type_name: String,

    pub queries: IndexMap<String, DefId>,
    pub mutations: IndexMap<String, MutationData>,
}

pub struct MutationData {
    pub entity: DefId,
    pub entity_operator_id: SerdeOperatorId,
    pub kind: MutationKind,
}

#[derive(Clone, Copy)]
pub enum MutationKind {
    Create { input: DefId },
    Update { id: SerdeOperatorId, input: DefId },
    Delete { id: SerdeOperatorId },
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
    pub def_id: DefId,
    pub operator_id: SerdeOperatorId,
    pub variants: Vec<(DefId, SerdeOperatorId)>,
}

#[derive(Clone)]
pub struct ScalarData {
    _serde_operator_id: SerdeOperatorId,
}

pub struct EdgeData {
    pub edge_type_name: String,
    pub connection_type_name: String,
}

#[derive(Clone)]
pub struct Field {
    pub cardinality: FieldCardinality,
    pub kind: FieldKind,
}

#[derive(Clone)]
pub enum FieldCardinality {
    UnitMandatory,
    UnitOptional,
    ManyMandatory,
    ManyOptional,
}

#[derive(Clone)]
pub enum FieldKind {
    Scalar(SerdeOperatorId),
    Edge {
        subject_id: DefId,
        node_id: DefId,
        rel_id: Option<SerdeOperatorId>,
    },
    Node(DefId),
    EntityRelationship {
        subject_id: Option<DefId>,
        node_id: DefId,
        rel_id: Option<SerdeOperatorId>,
    },
}
