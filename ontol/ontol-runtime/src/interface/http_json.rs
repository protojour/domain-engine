use ::serde::{Deserialize, Serialize};

use crate::ontology::ontol::TextConstant;

use super::serde::operator::SerdeOperatorAddr;

/// A http+json domain interface
#[derive(Serialize, Deserialize)]
pub struct HttpJson {
    pub resources: Vec<HttpResource>,
}

#[derive(Serialize, Deserialize)]
pub struct HttpResource {
    pub name: TextConstant,
    pub operator_addr: SerdeOperatorAddr,
    pub put: Option<Endpoint>,
}

#[derive(Serialize, Deserialize)]
pub struct Endpoint {}
