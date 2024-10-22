use ::serde::{Deserialize, Serialize};

use crate::{ontology::ontol::TextConstant, DefId, PropId};

use super::serde::operator::SerdeOperatorAddr;

/// A http+json domain interface
#[derive(Serialize, Deserialize)]
pub struct HttpJson {
    pub resources: Vec<HttpResource>,
}

#[derive(Serialize, Deserialize)]
pub struct HttpResource {
    pub name: TextConstant,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
    pub put: Option<Endpoint>,
    pub post: Option<Endpoint>,
    pub keyed: Vec<HttpKeyedResource>,
}

/// A resource under a specific keyed filter that should yield one single entity.
#[derive(Serialize, Deserialize)]
pub struct HttpKeyedResource {
    pub key_name: TextConstant,
    pub key_operator_addr: SerdeOperatorAddr,
    pub key_prop_id: PropId,
    pub get: Option<Endpoint>,
    pub put: Option<Endpoint>,
}

#[derive(Serialize, Deserialize)]
pub struct Endpoint {}
