use std::future::IntoFuture;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    search::{VertexSearchParams, VertexSearchResults},
    DomainError, DomainResult, Session, VertexAddr,
};
use ontol_runtime::DefId;
use tantivy::{
    collector::TopDocs,
    query::{AllQuery, QueryParser, QueryParserError},
    DateTime, DocAddress, Order, Searcher, TantivyError,
};
use tokio::task::JoinError;
use tracing::{debug, error, info};

use crate::TantivyDataStoreLayer;

#[derive(Debug)]
pub struct VertexHit {
    pub ordinal: usize,
    pub vertex_addr: VertexAddr,
    pub def_id: DefId,
    pub score: f32,
}

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
    /// fast field failed
    FastFieldFailed,
    /// fast field not present
    FastFieldNotPresent,
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

impl TantivyDataStoreLayer {
    pub async fn vertex_search(
        &self,
        params: VertexSearchParams,
        session: Session,
    ) -> DomainResult<VertexSearchResults> {
        let zelf = self.clone();
        let vertex_hits = tokio::task::spawn_blocking(move || zelf.blocking_vertex_search(params))
            .into_future()
            .await
            .map_err(SearchError::Join)??;

        self.fetch_vertex_results(vertex_hits, session).await
    }

    fn blocking_vertex_search(&self, params: VertexSearchParams) -> DomainResult<Vec<VertexHit>> {
        let searcher = self.index_reader.searcher();

        if let Some(query) = params.query.as_deref() {
            let query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
            let query = query_parser
                .parse_query(query)
                .map_err(SearchInputError::Parse)?;

            debug!("search query {query:?}");

            let hits = searcher
                .search(&query, &TopDocs::with_limit(params.limit))
                .map_err(SearchError::Search)?;
            self.read_vertex_hits(hits, &searcher)
        } else {
            // a query matching everything ordered by update_time decreasing

            let hits = searcher
                .search(
                    &AllQuery,
                    &TopDocs::with_limit(params.limit)
                        .order_by_fast_field::<DateTime>("update_time", Order::Desc),
                )
                .map_err(SearchError::Search)?;
            self.read_vertex_hits(hits, &searcher)
        }
    }

    fn read_vertex_hits<S: ScoreSource>(
        &self,
        hits: Vec<(S, DocAddress)>,
        searcher: &Searcher,
    ) -> DomainResult<Vec<VertexHit>> {
        debug!("got {} hits", hits.len());

        let mut vertex_hits = Vec::with_capacity(hits.len());

        for (score_source, doc_address) in hits {
            if let Some(vertex_hit) = self.parse_vertex_hit(
                doc_address,
                score_source.to_score(),
                vertex_hits.len(),
                searcher,
            )? {
                vertex_hits.push(vertex_hit);
            }
        }

        Ok(vertex_hits)
    }

    fn parse_vertex_hit(
        &self,
        doc_address: DocAddress,
        score: f32,
        ordinal: usize,
        searcher: &Searcher,
    ) -> DomainResult<Option<VertexHit>> {
        let segment_reader = searcher.segment_reader(doc_address.segment_ord);

        let vertex_addr_ff = segment_reader
            .fast_fields()
            .bytes("vertex_addr")
            .map_err(|_| SearchError::FastFieldFailed)?
            .ok_or(SearchError::FastFieldNotPresent)?;
        let domain_def_id_ff = segment_reader
            .fast_fields()
            .str("domain_def_id")
            .map_err(|_| SearchError::FastFieldFailed)?
            .ok_or(SearchError::FastFieldNotPresent)?;

        let mut vertex_addr = Vec::with_capacity(VertexAddr::inline_size());
        vertex_addr_ff
            .ord_to_bytes(doc_address.doc_id.into(), &mut vertex_addr)
            .map_err(|_| SearchError::DocMissingVertexAddress)?;

        let def_id = {
            let mut domain_def_id = String::new();
            domain_def_id_ff
                .ord_to_str(doc_address.doc_id.into(), &mut domain_def_id)
                .map_err(|_| SearchError::DocMissingDomainDefId)?;

            let mut path_iter = domain_def_id.split('\0');

            let domain_ulid = path_iter.next().ok_or(SearchError::DocInvalidDomainDefId)?;
            let def_tag = path_iter.next().ok_or(SearchError::DocInvalidDomainDefId)?;

            let Some(domain) = self.ontology().domain_by_id(
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

            DefId(domain.def_id().domain_index(), def_tag)
        };

        Ok(Some(VertexHit {
            ordinal,
            vertex_addr: vertex_addr.into_iter().collect(),
            def_id,
            score,
        }))
    }
}

trait ScoreSource {
    fn to_score(&self) -> f32;
}

impl ScoreSource for f32 {
    fn to_score(&self) -> f32 {
        *self
    }
}

impl ScoreSource for tantivy::DateTime {
    fn to_score(&self) -> f32 {
        1.0
    }
}
