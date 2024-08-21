use ontol_runtime::{
    ontology::Ontology,
    value::{OctetSequence, Value},
};
use serde::{Deserialize, Serialize};

use crate::pg_model::{PgDataKey, PgRegKey};

#[derive(Serialize, Deserialize)]
struct Address {
    data_key: PgDataKey,
    def_key: PgRegKey,
}

pub fn make_ontol_address(def_key: PgRegKey, data_key: PgDataKey, ontology: &Ontology) -> Value {
    let address = Address { data_key, def_key };

    let mut octet_sequence = OctetSequence(Default::default());
    bincode::serialize_into(&mut octet_sequence.0, &address).unwrap();

    Value::OctetSequence(
        octet_sequence,
        ontology.ontol_domain_meta().data_store_address.into(),
    )
}
