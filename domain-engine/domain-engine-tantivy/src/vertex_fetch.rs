use std::collections::{BTreeMap, HashMap};

use domain_engine_core::{
    domain_error::DomainErrorKind,
    domain_select::domain_select_no_edges,
    search::{VertexSearchResult, VertexSearchResults},
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
    DomainError, DomainResult, Session, VertexAddr,
};
use futures_util::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    query::{
        condition::{Clause, CondTerm, SetOperator},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect},
    },
    value::{OctetSequence, Value},
    DefId, OntolDefTag,
};
use thin_vec::{thin_vec, ThinVec};
use tracing::warn;

use crate::{search::VertexHit, TantivyDataStoreLayer};

#[derive(displaydoc::Display, Debug)]
enum FetchError {
    /// no vertex address
    NoVertexAddress,
}

impl From<FetchError> for DomainError {
    fn from(value: FetchError) -> Self {
        warn!("fetch error: {value}");
        DomainErrorKind::Search(format!("fetch error")).into_error()
    }
}

impl TantivyDataStoreLayer {
    pub async fn fetch_vertex_results(
        &self,
        vertex_hits: Vec<VertexHit>,
    ) -> DomainResult<VertexSearchResults> {
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

        let mut search_results: Vec<VertexSearchResult> = Vec::with_capacity(vertex_hits.len());

        for vertex in self.fetch_vertices_union(addr_by_def_id).await? {
            let Some(Attr::Unit(Value::OctetSequence(vertex_addr, _))) =
                vertex.get_attribute(OntolDefTag::Vertex.prop_id_0())
            else {
                return Err(FetchError::NoVertexAddress.into());
            };

            let Some(ordinal) = ordinal_by_addr.get(vertex_addr.0.as_slice()) else {
                warn!("{vertex_addr:?} was not in original hit list, ignoring");
                continue;
            };

            search_results.resize_with(ordinal + 1, || VertexSearchResult {
                vertex: Value::unit(),
                score: -1.0,
            });
            search_results[*ordinal] = VertexSearchResult {
                vertex,
                score: vertex_hits[*ordinal].score,
            };
        }

        Ok(VertexSearchResults {
            results: search_results,
        })
    }

    async fn fetch_vertices_union(
        &self,
        addr_by_def_id: BTreeMap<DefId, Vec<VertexAddr>>,
    ) -> DomainResult<Vec<Value>> {
        let ds_futures: FuturesUnordered<_> = addr_by_def_id
            .into_iter()
            .map(|(def_id, addresses)| self.fetch_vertices(def_id, addresses))
            .collect();

        let vec_of_vecs: Vec<_> = ds_futures.try_collect().await?;

        let mut ret = vec![];
        for vec in vec_of_vecs {
            for value in vec {
                ret.push(value);
            }
        }

        Ok(ret)
    }

    async fn fetch_vertices(
        &self,
        def_id: DefId,
        addresses: Vec<VertexAddr>,
    ) -> DomainResult<ThinVec<Value>> {
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

        let Select::Struct(mut struct_select) = domain_select_no_edges(def_id, &self.ontology)
        else {
            panic!("domain select of vertex must be Struct");
        };

        struct_select
            .properties
            .insert(OntolDefTag::Vertex.prop_id_0(), Select::Unit);
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
            .data_store
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter(messages).boxed(),
                Session::default(),
            )
            .await?
            .accumulate_sequences()
            .collect()
            .await;

        let Some(seq_result) = sequences.into_iter().next() else {
            return Ok(thin_vec![]);
        };

        Ok(seq_result?.into_elements())
    }
}
