use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use anyhow::anyhow;
use domain_engine_core::{
    data_store::DataStoreAPI,
    object_generator::ObjectGenerator,
    transact::{DataOperation, ReqMessage, RespMessage},
    DomainError, DomainResult, Session,
};
use futures_util::{stream::BoxStream, StreamExt};
use ontol_runtime::{
    attr::{Attr, AttrRef},
    interface::serde::processor::{
        ProcessorMode, ProcessorProfileApi, SerdeProcessor, SpecialProperty,
    },
    query::select::{EntitySelect, Select},
    sequence::{Sequence, SubSequence},
    value::Value,
    DefId,
};
use serde_json::Serializer;
use tracing::debug;

use crate::{arango_client::ArangoDatabaseHandle, query::AttrMut};

use super::{
    query::apply_select,
    {AqlQuery, ArangoCursorResponse, ArangoDatabase},
};

/// Serialize Value and optional relations to serde_json::Value
pub fn serialize(value: &Value, processor: &SerdeProcessor) -> serde_json::Value {
    let mut buf: Vec<u8> = vec![];
    processor
        .serialize_attr(AttrRef::Unit(value), &mut Serializer::new(&mut buf))
        .unwrap_or_else(|_| panic!("failed to serialize {value:?}"));
    serde_json::from_slice(&buf).unwrap_or_else(|_| panic!("failed to convert {buf:?} to Value"))
}

#[derive(Clone, Copy, Debug)]
pub enum WriteMode {
    Insert,
    Update,
    Upsert,
    Delete,
}

#[derive(Clone, Copy, Debug)]
pub enum EdgeWriteMode {
    Insert,
    Overwrite,
    OverwriteInverse,
    UpdateExisting,
    Delete,
    UpsertSelfIdentifying,
}

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Cursor {
    pub offset: usize,
    pub full_count: Option<usize>,
}

impl ProcessorProfileApi for ArangoDatabase {
    fn lookup_special_property(&self, key: &str) -> Option<SpecialProperty> {
        match key {
            "_id" => Some(SpecialProperty::TypeAnnotation),
            "_key" => Some(SpecialProperty::IdOverride),
            "_rev" => Some(SpecialProperty::Ignored),
            "_from" => Some(SpecialProperty::Ignored),
            "_to" => Some(SpecialProperty::Ignored),
            _ => None,
        }
    }

    fn find_special_property_name(&self, prop: SpecialProperty) -> Option<&str> {
        match prop {
            SpecialProperty::IdOverride => Some("_key"),
            SpecialProperty::TypeAnnotation => Some("_id"),
            SpecialProperty::Ignored => None,
        }
    }

    fn annotate_type(&self, input: &serde_value::Value) -> Option<DefId> {
        match input {
            serde_value::Value::String(id) => match id.split_once('/') {
                Some((collection, _)) => self.collection_lookup.get(collection).cloned(),
                None => None,
            },
            _ => None,
        }
    }
}

impl ArangoDatabase {
    // TODO: Implement actual transaction
    async fn transact_inner(
        self: Arc<Self>,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        enum State {
            Insert(Select),
            Update(Select),
            Upsert(Select),
            Delete(DefId),
        }

        let mut state: Option<State> = None;

        Ok(async_stream::try_stream! {
            for await req in messages {
                match req? {
                    ReqMessage::Query(op_seq, select) => {
                        state = None;
                        let sequence = self.query(select).await?;

                        yield RespMessage::SequenceStart(op_seq, sequence.sub().map(|sub_seq| Box::new(sub_seq.clone())));

                        for item in sequence.into_elements() {
                            yield RespMessage::Element(item, DataOperation::Queried);
                        }
                    }
                    ReqMessage::Insert(op_seq, select) => {
                        state = Some(State::Insert(select));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Update(op_seq, select) => {
                        state = Some(State::Update(select));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Upsert(op_seq, select) => {
                        state = Some(State::Upsert(select));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Delete(op_seq, def_id) => {
                        state = Some(State::Delete(def_id));
                        yield RespMessage::SequenceStart(op_seq, None);
                    }
                    ReqMessage::Argument(value) => {
                        match state.as_ref() {
                            Some(State::Insert(select)) => {
                                let (value, op) = self.write(
                                    value,
                                    select,
                                    WriteMode::Insert,
                                    ProcessorMode::Create,
                                    DataOperation::Inserted,
                                )
                                .await?;

                                yield RespMessage::Element(value, op);
                            }
                            Some(State::Update(select)) => {
                                let (value, op) = self.write(
                                    value,
                                    select,
                                    WriteMode::Update,
                                    ProcessorMode::Update,
                                    DataOperation::Updated,
                                )
                                .await?;

                                yield RespMessage::Element(value, op);
                            }
                            Some(State::Upsert(select)) => {
                                let (value, op) = self.write(
                                    value,
                                    select,
                                    WriteMode::Upsert,
                                    ProcessorMode::Create,
                                    // FIXME: The write response is dynamically either Inserted/Updated for Upsert
                                    DataOperation::Inserted
                                )
                                .await?;

                                yield RespMessage::Element(value, op);
                            }
                            Some(State::Delete(def_id)) => {
                                let value = self.delete(value, *def_id).await?;
                                yield RespMessage::Element(value, DataOperation::Deleted);
                            }
                            None => {
                                Err(DomainError::DataStore(anyhow!("invalid transaction state")))?
                            }
                        }
                    }
                }
            }
        }
        .boxed())
    }

    async fn query(&self, select: EntitySelect) -> DomainResult<Sequence<Value>> {
        let query = AqlQuery::build_query(&Select::Entity(select.clone()), &self.ontology, self)?;
        let profile = self.profile();
        let processor = self
            .ontology
            .new_serde_processor(query.operator_addr, ProcessorMode::Raw)
            .with_profile(&profile);

        let select_cursor: Option<Cursor> = select
            .after_cursor
            .as_deref()
            .map(bincode::deserialize)
            .transpose()
            .map_err(|_| DomainError::DataStore(anyhow!("Invalid cursor format")))?;
        let include_total = match select_cursor {
            Some(select_cursor) => match select_cursor.full_count {
                Some(_) => false,
                None => select.include_total_len,
            },
            None => select.include_total_len,
        };

        debug!(
            "AQL:\n{}\nbindVars: {:#?}",
            query.query.to_string(),
            query.bind_vars
        );
        let cursor = self.aql(query, true, include_total, None, processor).await;

        match cursor {
            Ok(mut cursor) => {
                for attr in &mut cursor.result {
                    apply_select(
                        AttrMut::from_attr(attr),
                        &Select::Entity(select.clone()),
                        &self.ontology,
                    )?;
                }

                let full_count = match select_cursor {
                    Some(select_cursor) => match select_cursor.full_count {
                        Some(full_count) => full_count,
                        None => match include_total {
                            true => cursor.extra.unwrap().stats.full_count.unwrap(),
                            false => cursor.count.unwrap(),
                        },
                    },
                    None => match include_total {
                        true => cursor.extra.unwrap().stats.full_count.unwrap(),
                        false => cursor.count.unwrap(),
                    },
                };
                let has_next = match select_cursor {
                    Some(cursor) => (cursor.offset + select.limit) < full_count,
                    None => select.limit < full_count,
                };
                let end_cursor = match select_cursor {
                    Some(cursor) => Some(Cursor {
                        offset: cursor.offset + select.limit,
                        full_count: Some(full_count),
                    }),
                    None => Some(Cursor {
                        offset: select.limit,
                        full_count: Some(full_count),
                    }),
                };

                let mut sequence = Sequence::with_capacity(cursor.result.len());

                for attr in cursor.result {
                    match attr {
                        Attr::Unit(u) => {
                            sequence.push(u);
                        }
                        _ => todo!("not a unit"),
                    }
                }

                Ok(sequence.with_sub(SubSequence {
                    end_cursor: end_cursor
                        .map(|cursor| bincode::serialize(&cursor).unwrap().into()),
                    has_next,
                    total_len: match select.include_total_len {
                        true => Some(full_count),
                        false => None,
                    },
                }))
            }
            Err(err) => Err(DomainError::DataStore(err)),
        }
    }

    async fn write(
        &self,
        mut entity: Value,
        select: &Select,
        write_mode: WriteMode,
        processor_mode: ProcessorMode,
        data_operation: DataOperation,
    ) -> DomainResult<(Value, DataOperation)> {
        let seed: PhantomData<serde_json::Value> = PhantomData;

        ObjectGenerator::new(processor_mode, &self.ontology, self.system.as_ref())
            .generate_objects(&mut entity);

        let pre_query = AqlQuery::prequery_from_entity(&entity, &self.ontology, self)?;

        if pre_query.query.operations.is_some() {
            debug!(
                "AQL:\n{}\nbindVars: {:#?}",
                pre_query.query.to_string(),
                pre_query.bind_vars
            );
            let cursor = self.aql(pre_query, false, false, None, seed).await;
            match cursor {
                Ok(cursor) => {
                    for data in cursor.result {
                        for (key, value) in data.as_object().unwrap() {
                            if value.is_null() {
                                return Err(DomainError::UnresolvedForeignKey(format!(
                                    r#""{}""#,
                                    key
                                )));
                            }
                        }
                    }
                    debug!("All prequeried entities exist");
                }
                Err(err) => return Err(DomainError::DataStore(err)),
            };
        } else {
            debug!("No prequery required");
        }

        let queries =
            AqlQuery::build_write(write_mode, entity, &select.clone(), &self.ontology, self)?;

        let mut cursor = ArangoCursorResponse::default();
        for query in queries {
            let profile = self.profile();
            let processor = self
                .ontology
                .new_serde_processor(query.operator_addr, ProcessorMode::Raw)
                .with_profile(&profile);
            debug!(
                "AQL:\n{}\nbindVars: {:#?}",
                query.query.to_string(),
                query.bind_vars
            );
            cursor = self
                .aql(query, false, false, None, processor)
                .await
                .map_err(|err| {
                    if err.to_string().starts_with("404") {
                        return DomainError::EntityNotFound;
                    }
                    DomainError::DataStore(err)
                })?;
        }

        let mut attr = cursor.result.first().unwrap().clone();
        apply_select(AttrMut::from_attr(&mut attr), select, &self.ontology)?;

        Ok((attr.into_unit().expect("not a unit"), data_operation))
    }

    async fn delete(&self, value: Value, def_id: DefId) -> DomainResult<Value> {
        let def = self.ontology.def(def_id);
        let rel_id = def
            .entity()
            .expect("type should be an entity")
            .id_relationship_id;
        let mut struct_map = BTreeMap::new();
        struct_map.insert(rel_id, Attr::Unit(value));
        let entity = Value::new_struct(struct_map, def_id.into());
        let select = Select::EntityId;

        let queries =
            AqlQuery::build_write(WriteMode::Delete, entity, &select, &self.ontology, self)?;

        let mut cursor = Ok(ArangoCursorResponse::default());
        for query in queries {
            let profile = self.profile();
            let processor = self
                .ontology
                .new_serde_processor(query.operator_addr, ProcessorMode::Raw)
                .with_profile(&profile);
            debug!(
                "AQL:\n{}\nbindVars: {:#?}",
                query.query.to_string(),
                query.bind_vars
            );
            cursor = self.aql(query, false, false, None, processor).await;
        }

        let bool_value = match cursor {
            Ok(_) => true,
            Err(err) => {
                if err.to_string().starts_with("404") {
                    false
                } else {
                    return Err(DomainError::DataStore(err));
                }
            }
        };

        Ok(self.ontology.bool_value(bool_value))
    }
}

#[async_trait::async_trait]
impl DataStoreAPI for ArangoDatabaseHandle {
    // async fn execute(&self, request: Request, _session: Session) -> DomainResult<Response> {
    //     match request {
    //         Request::Query(entity_select) => Ok(Response::Query(self.query(entity_select).await?)),
    //         Request::BatchWrite(requests) => {
    //             Ok(Response::BatchWrite(self.batch_write(requests).await?))
    //         }
    //     }
    // }

    async fn transact(
        &self,
        messages: BoxStream<'static, DomainResult<ReqMessage>>,
        _session: Session,
    ) -> DomainResult<BoxStream<'static, DomainResult<RespMessage>>> {
        self.database.clone().transact_inner(messages).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::ArangoClient;

    use super::*;
    use domain_engine_test_utils::unimock::Unimock;
    use indoc::indoc;
    use ontol_test_utils::{
        examples::ARTIST_AND_INSTRUMENT, expect_eq, serde_helper::serde_raw, TestCompile,
        TestPackages,
    };
    use reqwest::Client;
    use reqwest_middleware::ClientWithMiddleware;
    use serde_json::json;

    pub fn default_client() -> ClientWithMiddleware {
        ClientWithMiddleware::new(Client::new(), vec![])
    }

    fn artist_and_instrument() -> TestPackages {
        TestPackages::with_static_sources([ARTIST_AND_INSTRUMENT])
    }

    #[test]
    fn test_aql_query_build_query() {
        let test = artist_and_instrument().compile();
        let mut database = ArangoClient::new("", default_client()).db(
            "",
            test.ontology_owned(),
            Arc::new(Unimock::new(())),
        );
        database.populate_collections(test.root_package()).unwrap();

        let [artist] = test.bind(["artist"]);
        let select: Select = artist.struct_select([("plays", Select::Leaf)]).into();

        let aql = AqlQuery::build_query(&select, test.ontology(), &database).unwrap();

        expect_eq!(
            expected = indoc! {r#"
            WITH `artist`, `plays`, `instrument`
            FOR obj IN `artist`
                LET `obj_plays` = (
                    FOR `sub_obj`, `sub_obj_edge` IN OUTBOUND obj `plays`
                        RETURN MERGE({ _key: `sub_obj`._key }, { _edge: { how_much: `sub_obj_edge`.how_much } })
                )
                RETURN MERGE(obj, { plays: `obj_plays` })
            "#},
            actual = aql.query.to_string()
        );
    }

    #[test]
    fn test_aql_query_build_insert() {
        let test = artist_and_instrument().compile();
        let mut database = ArangoClient::new("", default_client()).db(
            "",
            test.ontology_owned(),
            Arc::new(Unimock::new(())),
        );
        database.populate_collections(test.root_package()).unwrap();

        let [artist] = test.bind(["artist"]);

        let mode = WriteMode::Insert;
        let entity = serde_raw(&artist)
            .to_value(json!({"ID": "artist/4ddd8824-2827-4f81-966e-f4147cee1edd", "name": "Igor Stravinskij"}))
            .unwrap();
        let select = Select::EntityId;

        let aql = AqlQuery::build_write(mode, entity, &select, test.ontology(), &database).unwrap();

        expect_eq!(
            expected = indoc! {r#"
            WITH `artist`
            LET `artist` = (
                INSERT {"_key":"4ddd8824-2827-4f81-966e-f4147cee1edd","name":"Igor Stravinskij"} INTO `artist`
                RETURN NEW
            )
            RETURN `artist`[0]
            "#},
            actual = aql[0].query.to_string(),
        );
    }

    #[test]
    fn test_aql_query_build_update() {
        let test = artist_and_instrument().compile();
        let mut database = ArangoClient::new("", default_client()).db(
            "",
            test.ontology_owned(),
            Arc::new(Unimock::new(())),
        );
        database.populate_collections(test.root_package()).unwrap();

        let [artist] = test.bind(["artist"]);

        let mode = WriteMode::Update;
        let entity = serde_raw(&artist)
            .to_value(json!({"ID": "artist/4ddd8824-2827-4f81-966e-f4147cee1edd", "name": "Igor Stravinskij"}))
            .unwrap();
        let select = Select::EntityId;

        let aql = AqlQuery::build_write(mode, entity, &select, test.ontology(), &database).unwrap();

        expect_eq!(
            expected = indoc! {r#"
            WITH `artist`
            LET `artist` = (
                UPDATE {"_key":"4ddd8824-2827-4f81-966e-f4147cee1edd","name":"Igor Stravinskij"} IN `artist`
                RETURN NEW
            )
            RETURN `artist`[0]
            "#},
            actual = aql[0].query.to_string(),
        );
    }

    #[test]
    fn test_aql_query_build_delete() {
        let test = artist_and_instrument().compile();
        let mut database = ArangoClient::new("", default_client()).db(
            "",
            test.ontology_owned(),
            Arc::new(Unimock::new(())),
        );
        database.populate_collections(test.root_package()).unwrap();

        let [artist] = test.bind(["artist"]);

        let mode = WriteMode::Delete;
        let entity = serde_raw(&artist)
            .to_value(json!({"ID": "artist/4ddd8824-2827-4f81-966e-f4147cee1edd", "name": "Igor Stravinskij"}))
            .unwrap();
        let select = Select::EntityId;

        let aql = AqlQuery::build_write(mode, entity, &select, test.ontology(), &database).unwrap();

        expect_eq!(
            expected = indoc! {r#"
            WITH `artist`
            LET `artist` = (
                REMOVE {"_key":"4ddd8824-2827-4f81-966e-f4147cee1edd"} IN `artist`
                RETURN OLD
            )
            RETURN `artist`[0]
            "#},
            actual = aql[0].query.to_string(),
        );
    }
}
