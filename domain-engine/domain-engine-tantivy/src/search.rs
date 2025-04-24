use std::future::IntoFuture;

use domain_engine_core::{
    DomainError, DomainResult, Session, VertexAddr,
    domain_error::DomainErrorKind,
    search::{
        FacetCount, SearchDomainOrDef, SearchFilters, VertexSearchFacets, VertexSearchParams,
        VertexSearchResults,
    },
};
use ontol_runtime::{DefId, DomainId};
use tantivy::{
    DateTime, DocAddress, Order, Searcher, TantivyError, Term,
    collector::{FacetCollector, FacetCounts, TopDocs},
    fastfield::FacetReader,
    query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, QueryParserError, TermQuery},
    schema::{Facet, IndexRecordOption},
};
use tokio::task::JoinError;
use tracing::{debug, error, info};

use crate::{TantivyDataStoreLayer, schema::fieldname};

/// Raw output from tantivy search
pub struct RawSearchResults {
    pub raw_hits: Vec<RawVertexHit>,
    pub facets: VertexSearchFacets,
}

#[derive(Debug)]
pub struct RawVertexHit {
    pub ordinal: usize,
    pub vertex_addr: VertexAddr,
    pub def_id: DefId,
    pub score: f32,
}

enum ParsedFacet {
    Domain(DomainId),
    Def(DomainId, u16),
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
    /// doc has invalid vertex address
    DocInvalidVertexAddress,
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
        let raw_results = tokio::task::spawn_blocking(move || zelf.blocking_vertex_search(params))
            .into_future()
            .await
            .map_err(SearchError::Join)??;

        self.fetch_vertex_results(raw_results, session).await
    }

    fn blocking_vertex_search(&self, params: VertexSearchParams) -> DomainResult<RawSearchResults> {
        let searcher = self.index_reader.searcher();

        if let Some(query) = params.query.as_deref() {
            let query_parser = QueryParser::for_index(&self.index, vec![self.schema.text]);
            let query = query_parser
                .parse_query(query)
                .map_err(SearchInputError::Parse)?;

            let query = self.apply_search_filters(Box::new(query), params.filters);

            info!("search query {query:?}");

            let mut facet_collector = FacetCollector::for_field(fieldname::FACET);
            facet_collector.add_facet("/def");
            facet_collector.add_facet("/domain");

            let (document_hits, facet_counts) = searcher
                .search(
                    &query,
                    &(TopDocs::with_limit(params.limit), facet_collector),
                )
                .map_err(SearchError::Search)?;
            self.read_raw_results(document_hits, facet_counts, &searcher)
        } else {
            // a query matching everything ordered by update_time decreasing.
            // Facet counts are not collected for this, because that makes less sense
            // when ordering by UPDATE_TIME.

            let document_hits = searcher
                .search(
                    &self.apply_search_filters(Box::new(AllQuery), params.filters),
                    &TopDocs::with_limit(params.limit)
                        .order_by_fast_field::<DateTime>(fieldname::UPDATE_TIME, Order::Desc),
                )
                .map_err(SearchError::Search)?;
            self.read_raw_results(document_hits, FacetCounts::default(), &searcher)
        }
    }

    fn apply_search_filters(
        &self,
        base_query: Box<dyn Query>,
        filters: SearchFilters,
    ) -> Box<dyn Query> {
        let Some(filters) = filters.filter_is_empty() else {
            return base_query;
        };

        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = vec![];
        subqueries.push((Occur::Must, base_query));

        if let Some(domain_or_def_filters) = filters.domains_or_defs {
            for domain_or_def_filter in domain_or_def_filters {
                let mut facet_path: Vec<String> = vec![];

                if let Some(def_tag) = domain_or_def_filter.def_tag {
                    facet_path.push("def".to_string());
                    facet_path.push(format!("{}:{def_tag}", domain_or_def_filter.domain_id));
                } else {
                    facet_path.push("domain".to_string());
                    facet_path.push(format!("{}", domain_or_def_filter.domain_id));
                }

                subqueries.push((
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_facet(self.schema.facet, &Facet::from_path(facet_path)),
                        IndexRecordOption::Basic,
                    )),
                ));
            }
        }

        Box::new(BooleanQuery::new(subqueries))
    }

    fn read_raw_results<S: ScoreSource>(
        &self,
        document_hits: Vec<(S, DocAddress)>,
        facet_counts: FacetCounts,
        searcher: &Searcher,
    ) -> DomainResult<RawSearchResults> {
        debug!("got {} hits", document_hits.len());

        let mut raw_hits = Vec::with_capacity(document_hits.len());

        for (score_source, doc_address) in document_hits {
            if let Some(raw_hit) = self.read_raw_vertex_hit(
                doc_address,
                score_source.to_score(),
                raw_hits.len(),
                searcher,
            )? {
                raw_hits.push(raw_hit);
            }
        }

        let mut facets = VertexSearchFacets::default();

        for (facet, count) in facet_counts.get("/") {
            match parse_facet(facet) {
                Ok(ParsedFacet::Def(domain_id, def_tag)) => {
                    facets.defs.push(FacetCount {
                        facet: SearchDomainOrDef {
                            domain_id,
                            def_tag: Some(def_tag),
                        },
                        count,
                    });
                }
                Ok(ParsedFacet::Domain(domain_id)) => {
                    facets.domains.push(FacetCount {
                        facet: SearchDomainOrDef {
                            domain_id,
                            def_tag: None,
                        },
                        count,
                    });
                }
                Err(err) => {
                    error!("unable to parse facet: {err}");
                }
            }
        }

        Ok(RawSearchResults { raw_hits, facets })
    }

    fn read_raw_vertex_hit(
        &self,
        doc_address: DocAddress,
        score: f32,
        ordinal: usize,
        searcher: &Searcher,
    ) -> DomainResult<Option<RawVertexHit>> {
        let segment_reader = searcher.segment_reader(doc_address.segment_ord);

        let vertex_addr_reader = segment_reader
            .fast_fields()
            .bytes(fieldname::VERTEX_ADDR)
            .map_err(|_| SearchError::FastFieldFailed)?
            .ok_or(SearchError::FastFieldNotPresent)?;

        let facet_reader = segment_reader
            .facet_reader(fieldname::FACET)
            .map_err(|_| SearchError::FastFieldFailed)?;

        let mut vertex_addr = Vec::with_capacity(VertexAddr::inline_size());
        vertex_addr_reader
            .ord_to_bytes(
                vertex_addr_reader
                    .term_ords(doc_address.doc_id)
                    .next()
                    .ok_or(SearchError::DocMissingVertexAddress)?,
                &mut vertex_addr,
            )
            .map_err(|_| SearchError::DocInvalidVertexAddress)?;

        let Some(def_id) = self.read_def_id(doc_address.doc_id, &facet_reader)? else {
            return Ok(None);
        };

        Ok(Some(RawVertexHit {
            ordinal,
            vertex_addr: vertex_addr.into_iter().collect(),
            def_id,
            score,
        }))
    }

    fn read_def_id(&self, doc_id: u32, facet_reader: &FacetReader) -> DomainResult<Option<DefId>> {
        let mut facet = Facet::default();

        for facet_ord in facet_reader.facet_ords(doc_id) {
            facet_reader
                .facet_from_ord(facet_ord, &mut facet)
                .map_err(|_| SearchError::DocInvalidDomainDefId)?;

            let mut path_iter = facet.encoded_str().split('\0');

            if path_iter.next().ok_or(SearchError::DocInvalidDomainDefId)? != "def" {
                continue;
            }

            let def_str = path_iter.next().ok_or(SearchError::DocInvalidDomainDefId)?;
            let mut split = def_str.split(':');

            let domain_ulid = split.next().ok_or(SearchError::DocInvalidDomainDefId)?;
            let def_tag = split.next().ok_or(SearchError::DocInvalidDomainDefId)?;

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

            return Ok(Some(DefId(domain.def_id().domain_index(), def_tag)));
        }

        Err(SearchError::DocMissingDomainDefId.into())
    }
}

fn parse_facet(facet: &Facet) -> Result<ParsedFacet, &'static str> {
    let mut path = facet.encoded_str().split('\0');

    match path.next().ok_or("missing prefix")? {
        "def" => {
            let mut split = path.next().ok_or("missing def str")?.split(':');
            let domain_id = split
                .next()
                .ok_or("missing domain prefix")?
                .parse()
                .map_err(|_| "invalid domain id")?;
            let def_tag = split
                .next()
                .ok_or("missing def tag")?
                .parse()
                .map_err(|_| "invalid def tag")?;

            Ok(ParsedFacet::Def(domain_id, def_tag))
        }
        "domain" => {
            let domain_id = path
                .next()
                .ok_or("missing domain id")?
                .parse()
                .map_err(|_| "invalid domain id")?;

            Ok(ParsedFacet::Domain(domain_id))
        }
        _prefix => Err("invalid facet prefix"),
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
