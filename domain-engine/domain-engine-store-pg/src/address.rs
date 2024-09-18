use domain_engine_core::{DomainResult, VertexAddr};
use ontol_runtime::{
    value::{OctetSequence, Value},
    OntolDefTag,
};
use serde::{Deserialize, Serialize};

use crate::{
    pg_error::ds_bad_req,
    pg_model::{PgDataKey, PgRegKey},
};

#[derive(Serialize, Deserialize)]
struct Address {
    data_key: PgDataKey,
    def_key: PgRegKey,
}

pub fn make_vertex_addr(def_key: PgRegKey, data_key: PgDataKey) -> VertexAddr {
    let address = Address { data_key, def_key };
    let mut vertex_addr = VertexAddr::default();
    bincode::serialize_into(&mut vertex_addr, &address).unwrap();

    // PG vertex addr should fit exactly in VertexAddr inline size
    assert_eq!(vertex_addr.len(), VertexAddr::inline_size());
    vertex_addr
}

pub fn make_ontol_vertex_address(def_key: PgRegKey, data_key: PgDataKey) -> Value {
    let address = Address { data_key, def_key };

    let mut octet_sequence = OctetSequence(Default::default());
    bincode::serialize_into(&mut octet_sequence.0, &address).unwrap();

    Value::OctetSequence(octet_sequence, OntolDefTag::Vertex.def_id().into())
}

pub fn deserialize_ontol_vertex_address(value: Value) -> DomainResult<(PgRegKey, PgDataKey)> {
    let Value::OctetSequence(seq, _) = value else {
        return Err(ds_bad_req("bad vertex type"));
    };

    let Ok(address) = bincode::deserialize::<Address>(&seq.0) else {
        return Err(ds_bad_req("bad vertex encoding"));
    };

    Ok((address.def_key, address.data_key))
}
