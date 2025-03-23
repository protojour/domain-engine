use std::collections::{BTreeMap, HashMap};

use domain_engine_core::{
    DomainError, DomainResult, Session, VertexAddr,
    domain_error::DomainErrorKind,
    domain_select::domain_select_no_edges,
    search::{VertexSearchResult, VertexSearchResults},
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use ontol_runtime::{
    DefId, OntolDefTag, OntolDefTagExt,
    attr::Attr,
    query::{
        condition::{Clause, CondTerm, SetOperator},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect},
    },
    value::{OctetSequence, Value},
};
use thin_vec::{ThinVec, thin_vec};
use tracing::warn;

use crate::{TantivyDataStoreLayer, search::RawSearchResults};

#[derive(displaydoc::Display, Debug)]
enum FetchError {
    /// no vertex address
    NoVertexAddress,
}

impl From<FetchError> for DomainError {
    fn from(value: FetchError) -> Self {
        warn!("fetch error: {value}");
        DomainErrorKind::Search("fetch error".to_string()).into_error()
    }
}

impl TantivyDataStoreLayer {
    pub async fn fetch_vertex_results(
        &self,
        raw_results: RawSearchResults,
        session: Session,
    ) -> DomainResult<VertexSearchResults> {
        let vertex_hits = raw_results.raw_hits;
        let mut ordinal_by_addr: HashMap<VertexAddr, usize> =
            HashMap::with_capacity(vertex_hits.len());
        let mut addr_by_def_id: BTreeMap<DefId, Vec<VertexAddr>> = Default::default();

        for hit in &vertex_hits {
            ordinal_by_addr.insert(hit.vertex_addr.clone(), hit.ordinal);
            addr_by_def_id
                .entry(hit.def_id)
                .or_default()
                .push(hit.vertex_addr.clone());
        }

        if ordinal_by_addr.len() < vertex_hits.len() {
            warn!(
                "overlapping addresses ({}/{})",
                ordinal_by_addr.len(),
                vertex_hits.len()
            );
        }

        let mut vertex_results: Vec<Option<VertexSearchResult>> =
            Vec::with_capacity(vertex_hits.len());
        vertex_results.resize_with(vertex_hits.len(), || None);

        for vertex in self.fetch_vertices_union(addr_by_def_id, session).await? {
            let Some(Attr::Unit(Value::OctetSequence(vertex_addr, _))) =
                vertex.get_attribute(OntolDefTag::RelationDataStoreAddress.prop_id_0())
            else {
                return Err(FetchError::NoVertexAddress.into());
            };

            let Some(ordinal) = ordinal_by_addr.get(vertex_addr.0.as_slice()) else {
                warn!(
                    "{vertex_addr:?} ({:?}) was not in original hit list, ignoring",
                    vertex.type_def_id()
                );
                continue;
            };

            let old = vertex_results[*ordinal].replace(VertexSearchResult {
                vertex,
                score: vertex_hits[*ordinal].score,
            });

            if old.is_some() {
                warn!("overwrote vertex ordinal {ordinal}");
            }
        }

        let vertex_results: Vec<_> = vertex_results.into_iter().flatten().collect();

        if vertex_results.len() < vertex_hits.len() {
            warn!(
                "vertex results len={}, hits len={}",
                vertex_results.len(),
                vertex_hits.len()
            );
        }

        Ok(VertexSearchResults {
            results: vertex_results,
            facets: raw_results.facets,
        })
    }

    async fn fetch_vertices_union(
        &self,
        addr_by_def_id: BTreeMap<DefId, Vec<VertexAddr>>,
        session: Session,
    ) -> DomainResult<Vec<Value>> {
        let mut ds_futures: FuturesUnordered<_> = addr_by_def_id
            .into_iter()
            .map(|(def_id, addresses)| self.fetch_vertices(def_id, addresses, session.clone()))
            .collect();

        let mut ret = vec![];

        while let Some(result) = ds_futures.next().await {
            match result {
                Ok(vec) => {
                    for vertex in vec {
                        ret.push(vertex);
                    }
                }
                Err(err) => {
                    match err.kind() {
                        DomainErrorKind::Unauthorized => {
                            // silently filtered out
                        }
                        _ => {
                            return Err(err);
                        }
                    }
                }
            }
        }

        Ok(ret)
    }

    async fn fetch_vertices(
        &self,
        def_id: DefId,
        addresses: Vec<VertexAddr>,
        session: Session,
    ) -> DomainResult<ThinVec<Value>> {
        let addresses_len = addresses.len();
        let filter = {
            let mut filter = Filter::default_for_datastore();
            let condition = filter.condition_mut();
            let root_var = condition.mk_cond_var();
            condition.add_clause(root_var, Clause::Root);
            let set_var = condition.mk_cond_var();
            condition.add_clause(
                root_var,
                Clause::MatchProp(
                    OntolDefTag::RelationDataStoreAddress.prop_id_0(),
                    SetOperator::ElementIn,
                    set_var,
                ),
            );
            for address in addresses {
                condition.add_clause(
                    set_var,
                    Clause::Member(
                        CondTerm::Wildcard,
                        CondTerm::Value(Value::OctetSequence(
                            OctetSequence(address.into_iter().collect()),
                            OntolDefTag::Vertex.def_id().into(),
                        )),
                    ),
                );
            }
            filter
        };

        let Select::Struct(mut struct_select) =
            domain_select_no_edges(def_id, self.ontology().as_ref())
        else {
            panic!("domain select of vertex must be Struct");
        };

        struct_select.properties.insert(
            OntolDefTag::RelationDataStoreAddress.prop_id_0(),
            Select::Unit,
        );
        struct_select
            .properties
            .insert(OntolDefTag::UpdateTime.prop_id_0(), Select::Unit);

        let messages = [Ok(ReqMessage::Query(
            0,
            EntitySelect {
                source: StructOrUnionSelect::Struct(struct_select),
                filter,
                limit: None,
                after_cursor: None,
                include_total_len: false,
            },
        ))];

        let sequences: Vec<_> = self
            .config
            .datastore
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter(messages).boxed(),
                session,
            )
            .await?
            .accumulate_sequences()
            .collect()
            .await;

        let Some(seq_result) = sequences.into_iter().next() else {
            return Ok(thin_vec![]);
        };
        let elements = seq_result?.into_elements();

        if elements.len() < addresses_len {
            warn!(
                "there were {addresses_len} addresses but only {} vertices for {def_id:?}",
                elements.len()
            );
        }

        Ok(elements)
    }
}
