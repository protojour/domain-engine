use ontol_runtime::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct VertexSearchParams {
    pub query: Option<String>,
    pub limit: usize,
}

#[derive(Serialize, Deserialize)]
pub struct VertexSearchResults {
    pub results: Vec<VertexSearchResult>,
}

#[derive(Serialize, Deserialize)]
pub struct VertexSearchResult {
    pub vertex: Value,
    pub score: f32,
}
