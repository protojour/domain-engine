use ontol_runtime::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct VertexSearchParams {
    pub query: String,
    pub limit: usize,
}

#[derive(Serialize, Deserialize)]
pub struct VertexSearchResults {
    pub results: Vec<VertexSearchResults>,
}

#[derive(Serialize, Deserialize)]
pub struct VertexSearchResult {
    pub value: Value,
    pub score: f32,
}
