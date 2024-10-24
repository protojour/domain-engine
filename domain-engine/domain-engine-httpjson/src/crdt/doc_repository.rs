use std::sync::Arc;

use automerge::Automerge;
use domain_engine_core::{
    domain_error::DomainErrorKind,
    transact::{ReqMessage, RespMessage, TransactionMode},
    DomainEngine, DomainError, DomainResult, Session, VertexAddr,
};
use futures_util::{StreamExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    query::{
        condition::{Clause, CondTerm, SetOperator},
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    value::Value,
    DefId, OntolDefTag,
};
use tracing::error;

use super::DocAddr;

/// A repository facade around DomainEngine for better encapsulation of CRDT-related transactions
#[derive(Clone)]
pub struct DocRepository {
    domain_engine: Arc<DomainEngine>,
}

impl From<Arc<DomainEngine>> for DocRepository {
    fn from(value: Arc<DomainEngine>) -> Self {
        Self {
            domain_engine: value,
        }
    }
}

impl DocRepository {
    pub fn domain_engine(&self) -> &Arc<DomainEngine> {
        &self.domain_engine
    }

    /// Load one full CRDT/automerge document from data store
    pub async fn load(
        &self,
        doc_addr: DocAddr,
        session: Session,
    ) -> DomainResult<Option<Automerge>> {
        let mut messages = self
            .domain_engine
            .get_data_store()?
            .api()
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter([Ok(ReqMessage::CrdtGet(
                    doc_addr.0.iter().copied().collect(),
                    doc_addr.1,
                ))])
                .boxed(),
                session,
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .into_iter();

        let Some(RespMessage::SequenceStart(_)) = messages.next() else {
            return Ok(None);
        };
        let Some(RespMessage::Element(value, _)) = messages.next() else {
            return Ok(None);
        };

        match value {
            Value::OctetSequence(payload, _) => {
                let automerge = Automerge::load(&payload.0).map_err(|err| {
                    error!("automerge load: {err:?}");
                    DomainError::data_store("corrupted CRDT")
                })?;

                Ok(Some(automerge.with_actor(
                    self.domain_engine.system().automerge_system_actor().into(),
                )))
            }
            _value => Err(DomainError::data_store("incorrect CRDT return value")),
        }
    }

    /// Save an incremental diff
    pub async fn save_incremental(
        &self,
        doc_addr: DocAddr,
        payload: Vec<u8>,
        session: Session,
    ) -> DomainResult<()> {
        self.domain_engine
            .get_data_store()?
            .api()
            .transact(
                TransactionMode::ReadWrite,
                futures_util::stream::iter([Ok(ReqMessage::CrdtSaveIncremental(
                    doc_addr.0.iter().copied().collect(),
                    doc_addr.1,
                    vec![],
                    payload,
                ))])
                .boxed(),
                session,
            )
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    pub async fn fetch_vertex_addr(
        &self,
        def_id: DefId,
        entity_id: Value,
        session: Session,
    ) -> DomainResult<Option<VertexAddr>> {
        let def = self.domain_engine.ontology().def(def_id);
        let Some(entity) = def.entity() else {
            return Err(DomainErrorKind::NotAnEntity(def_id).into_error());
        };

        let mut filter = Filter::default_for_datastore();
        {
            let condition = filter.condition_mut();
            let prop_var = condition.mk_cond_var();
            let set_var = condition.mk_cond_var();

            condition.add_clause(
                prop_var,
                Clause::MatchProp(entity.id_prop, SetOperator::ElementIn, set_var),
            );
            condition.add_clause(
                set_var,
                Clause::Member(CondTerm::Wildcard, CondTerm::Value(entity_id)),
            );
        }

        let response_messages: Vec<_> = self
            .domain_engine
            .get_data_store()?
            .api()
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter([Ok(ReqMessage::Query(
                    0,
                    EntitySelect {
                        source: StructOrUnionSelect::Struct(StructSelect {
                            def_id,
                            properties: From::from([
                                (
                                    OntolDefTag::RelationDataStoreAddress.prop_id_0(),
                                    Select::Unit,
                                ),
                                (entity.id_prop, Select::Unit),
                            ]),
                        }),
                        filter,
                        limit: None,
                        after_cursor: None,
                        include_total_len: false,
                    },
                ))])
                .boxed(),
                session,
            )
            .await?
            .try_collect()
            .await?;

        let mut msg_iter = response_messages.into_iter();

        let Some(RespMessage::SequenceStart(_)) = msg_iter.next() else {
            return Ok(None);
        };
        let Some(RespMessage::Element(vertex, _)) = msg_iter.next() else {
            return Ok(None);
        };

        let Value::Struct(mut attrs, _) = vertex else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let Attr::Unit(Value::OctetSequence(addr, _)) = attrs
            .remove(&OntolDefTag::RelationDataStoreAddress.prop_id_0())
            .unwrap()
        else {
            return Err(DomainError::data_store("vertex address was missing"));
        };

        Ok(Some(addr.0.into_iter().collect()))
    }
}
