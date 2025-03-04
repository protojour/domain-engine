use ::serde::{Deserialize, Serialize};

use crate::{DefId, MapKey, PropId, ontology::ontol::TextConstant};

use super::serde::operator::SerdeOperatorAddr;

/// A http+json domain interface
#[derive(Serialize, Deserialize)]
pub struct HttpJson {
    pub resources: Vec<HttpResource>,
}

#[derive(Serialize, Deserialize)]
pub enum HttpResource {
    Def(HttpDefResource),
    MapGet(HttpMapGetResource),
}

#[derive(Serialize, Deserialize)]
pub struct HttpDefResource {
    pub name: TextConstant,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
    pub get: Option<HttpMapGetResource>,
    pub put: Option<Endpoint>,
    pub post: Option<Endpoint>,
    pub keyed: Vec<HttpKeyedResource>,
}

/// A GET endpoint which uses ONTOL mapping
#[derive(Serialize, Deserialize)]
pub struct HttpMapGetResource {
    pub name: TextConstant,
    pub output_operator_addr: SerdeOperatorAddr,
    pub map_key: MapKey,
}

/// A resource under a specific keyed filter that should yield one single entity.
#[derive(Serialize, Deserialize)]
pub struct HttpKeyedResource {
    pub key_name: TextConstant,
    pub key_operator_addr: SerdeOperatorAddr,
    pub key_prop_id: PropId,
    pub get: Option<Endpoint>,
    pub put: Option<Endpoint>,
    pub crdts: Vec<(PropId, TextConstant)>,
}

#[derive(Serialize, Deserialize)]
pub struct Endpoint {}
