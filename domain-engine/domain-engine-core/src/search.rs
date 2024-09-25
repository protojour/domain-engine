use ontol_runtime::value::Value;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Serialize, Deserialize)]
pub struct VertexSearchParams {
    pub query: Option<String>,
    pub filters: SearchFilters,
    pub limit: usize,
}

#[derive(Default, Serialize, Deserialize)]
pub struct SearchFilters {
    pub domains_or_defs: Option<Vec<SearchDomainOrDefFilter>>,
}

impl SearchFilters {
    pub fn filter_is_empty(self) -> Option<Self> {
        if self.domains_or_defs.is_none() {
            None
        } else {
            Some(self)
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct SearchDomainOrDefFilter {
    pub domain_id: Ulid,
    pub def_tag: Option<u16>,
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
