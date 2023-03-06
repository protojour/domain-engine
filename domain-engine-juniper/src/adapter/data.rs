use std::{collections::HashMap, sync::Arc};

use indexmap::IndexMap;
use ontol_runtime::{env::Env, serde::SerdeOperatorId, DefId, PackageId};
use smartstring::alias::String;

pub struct DomainData {
    pub env: Arc<Env>,
    pub package_id: PackageId,

    pub types: HashMap<SerdeOperatorId, TypeData>,
    pub entities: HashMap<SerdeOperatorId, EntityData>,
    pub root_edges: HashMap<SerdeOperatorId, EdgeData>,

    pub query_type_name: String,
    pub mutation_type_name: String,

    pub queries: IndexMap<String, SerdeOperatorId>,
    pub mutations: IndexMap<String, (MutationKind, SerdeOperatorId)>,
}

pub enum MutationKind {
    Create,
    Update,
    Delete,
}

pub struct EntityData {
    pub def_id: DefId,
    pub query_field_name: String,
    pub create_mutation_field_name: String,
    pub update_mutation_field_name: String,
    pub delete_mutation_field_name: String,
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
