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

    pub types_by_def: FnvHashMap<DefId, TypeData>,
    pub entities_by_def: FnvHashMap<DefId, EntityData>,
    pub unions_by_def: FnvHashMap<DefId, UnionData>,
    pub edges_by_def: FnvHashMap<(Option<DefId>, DefId), EdgeData>,

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
        node: DefId,
        node_operator: SerdeOperatorId,
        rel: Option<SerdeOperatorId>,
    },
    Node(DefId, SerdeOperatorId),
    EntityRelationship {
        subject: DefId,
        node: DefId,
        node_operator: SerdeOperatorId,
        rel: Option<SerdeOperatorId>,
    },
}
