use std::future::IntoFuture;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    search::{VertexSearchParams, VertexSearchResults},
    DomainError, DomainResult, VertexAddr,
};
use ontol_runtime::DefId;
use tantivy::{
    collector::TopDocs,
    query::{QueryParser, QueryParserError},
    schema::OwnedValue,
    TantivyDocument, TantivyError,
};
use tokio::task::JoinError;
use tracing::{error, info};

use crate::TantivyDataStoreLayer;

#[derive(displaydoc::Display, Debug)]
enum SearchInputError {
    /// parse error: {0}
    Parse(QueryParserError),
}

#[derive(displaydoc::Display, Debug)]
enum SearchError {
    /// join error
    Join(JoinError),
    /// search: {0}
    Search(TantivyError),
    /// document fetch: {0}
    DocumentFetch(TantivyError),
    /// doc is missing vertex address
    DocMissingVertexAddress,
    /// doc is missing domain def id
    DocMissingDomainDefId,
    /// doc has invalid domain def id
    DocInvalidDomainDefId,
}

impl From<SearchInputError> for DomainError {
    fn from(value: SearchInputError) -> Self {
        info!("{value}");
        DomainErrorKind::Search(format!("invalid input: {value}")).into_error()
    }
}

impl From<SearchError> for DomainError {
    fn from(value: SearchError) -> Self {
        error!("{value}");
        DomainErrorKind::Search("internal".to_string()).into_error()
    }
}

/// Search document mapped back to a vertex
struct VertexHit {
    vertex_addr: VertexAddr,
    def_id: DefId,
    score: f32,
}

impl TantivyDataStoreLayer {
    pub async fn vertex_search(
        self,
        params: VertexSearchParams,
    ) -> DomainResult<VertexSearchResults> {
        let vertex_hits = tokio::task::spawn_blocking(move || self.blocking_vertex_search(params))
            .into_future()
            .await
            .map_err(SearchError::Join)??;

        todo!()
    }

    fn blocking_vertex_search(self, params: VertexSearchParams) -> DomainResult<Vec<VertexHit>> {
        let query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
        let query = query_parser
            .parse_query(&params.query)
            .map_err(SearchInputError::Parse)?;

        let searcher = self.index_reader.searcher();
        let hits = searcher
            .search(&query, &TopDocs::with_limit(params.limit))
            .map_err(SearchError::Search)?;

        let mut vertex_hits = Vec::with_capacity(hits.len());

        for (score, doc_address) in hits {
            let doc = searcher
                .doc::<TantivyDocument>(doc_address)
                .map_err(SearchError::DocumentFetch)?;

            if let Some(vertex_hit) = self.parse_vertex_hit(&doc, score)? {
                vertex_hits.push(vertex_hit);
            }
        }

        Ok(vertex_hits)
    }

    fn parse_vertex_hit(
        &self,
        doc: &TantivyDocument,
        score: f32,
    ) -> DomainResult<Option<VertexHit>> {
        let OwnedValue::Bytes(vertex_address) = doc
            .get_first(self.schema.vertex_addr)
            .ok_or(SearchError::DocMissingVertexAddress)?
        else {
            panic!("vertex address must be bytes");
        };

        let OwnedValue::Facet(domain_def_id_facet) = doc
            .get_first(self.schema.domain_def_id)
            .ok_or(SearchError::DocMissingDomainDefId)?
        else {
            panic!("domain def id must be a facet");
        };

        let mut path_iter = domain_def_id_facet.to_path().into_iter();

        let domain_ulid = path_iter.next().ok_or(SearchError::DocInvalidDomainDefId)?;
        let def_tag = path_iter.next().ok_or(SearchError::DocInvalidDomainDefId)?;

        let Some(domain) = self.ontology.domain_by_id(
            domain_ulid
                .parse()
                .map_err(|_| SearchError::DocInvalidDomainDefId)?,
        ) else {
            // FIXME: domain could be removed from ontology
            // Cleaning up removed domains could be done initially by a facet search?
            return Ok(None);
        };

        let def_tag: u16 = def_tag
            .parse()
            .map_err(|_| SearchError::DocInvalidDomainDefId)?;

        Ok(Some(VertexHit {
            vertex_addr: vertex_address.iter().copied().collect(),
            def_id: DefId(domain.def_id().domain_index(), def_tag),
            score,
        }))
    }
}
