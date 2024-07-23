use ::serde::{Deserialize, Serialize};

/// A http+json domain interface
#[derive(Serialize, Deserialize)]
pub struct HttpJson {
    pub resources: Vec<HttpResource>,
}

#[derive(Serialize, Deserialize)]
pub struct HttpResource {
    pub put: Option<Endpoint>,
}

#[derive(Serialize, Deserialize)]
pub struct Endpoint {}
