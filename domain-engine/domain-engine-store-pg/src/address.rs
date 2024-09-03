use ontol_runtime::{
    value::{OctetSequence, Value},
    OntolDefTag,
};
use serde::{Deserialize, Serialize};

use crate::pg_model::{PgDataKey, PgRegKey};

#[derive(Serialize, Deserialize)]
struct Address {
    data_key: PgDataKey,
    def_key: PgRegKey,
}

pub fn make_ontol_address(def_key: PgRegKey, data_key: PgDataKey) -> Value {
    let address = Address { data_key, def_key };

    let mut octet_sequence = OctetSequence(Default::default());
    bincode::serialize_into(&mut octet_sequence.0, &address).unwrap();

    Value::OctetSequence(
        octet_sequence,
        OntolDefTag::RelationDataStoreAddress.def_id().into(),
    )
}
