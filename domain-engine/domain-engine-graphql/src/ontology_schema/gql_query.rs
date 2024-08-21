use std::collections::BTreeMap;
use std::str::FromStr;

use crate::cursor_util::GraphQLCursor;
use crate::field_error;
use crate::juniper::{self, graphql_object, FieldResult};

use domain_engine_core::transact::AccumulateSequences;
use domain_engine_core::transact::ReqMessage;
use domain_engine_core::transact::TransactionMode;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use ontol_runtime::query::filter::Filter;
use ontol_runtime::query::select::EntitySelect;
use ontol_runtime::query::select::StructOrUnionSelect;
use ontol_runtime::query::select::StructSelect;
use ontol_runtime::DefId;
use serde::de::value::StringDeserializer;
use serde::Deserialize;
use ulid::Ulid;

use super::gql_def;
use super::gql_dictionary;
use super::gql_dictionary::DefDictionaryEntry;
use super::gql_domain;
use super::gql_vertex;
use super::OntologyCtx;

#[derive(Default)]
pub struct Query;

#[graphql_object]
#[graphql(context = OntologyCtx)]
impl Query {
    fn api_version() -> &'static str {
        "0.1"
    }

    fn domain(
        id: Option<juniper::ID>,
        name: Option<String>,
        ctx: &OntologyCtx,
    ) -> FieldResult<gql_domain::Domain> {
        let domain = if let Some(id) = id {
            let id = Ulid::from_string(&id)?;
            ctx.domains().find(|(_, d)| d.domain_id().ulid == id)
        } else if let Some(name) = name {
            ctx.domains()
                .find(|(_, d)| ctx.get_text_constant(d.unique_name()).to_string() == name)
        } else {
            None
        };

        if let Some((id, _)) = domain {
            Ok(gql_domain::Domain { pkg_id: id })
        } else {
            Err(field_error("Domain not found"))
        }
    }

    fn domains(ctx: &OntologyCtx) -> FieldResult<Vec<gql_domain::Domain>> {
        let mut domains = vec![];
        for (package_id, _ontology_domain) in ctx.domains() {
            let domain = gql_domain::Domain { pkg_id: package_id };
            domains.push(domain);
        }
        Ok(domains)
    }

    fn def(def_id: String, ctx: &OntologyCtx) -> FieldResult<gql_def::Def> {
        if let Ok(def_id) = DefId::from_str(&def_id) {
            if let Some(def) = ctx.get_def(def_id) {
                return Ok(gql_def::Def { id: def.id });
            }
        }
        Err(field_error("TypeInfo not found"))
    }

    fn def_dictionary(ctx: &OntologyCtx) -> Vec<DefDictionaryEntry> {
        let mut dict: BTreeMap<String, Vec<gql_def::Def>> = Default::default();

        for (_, domain) in ctx.domains() {
            for def in domain.defs() {
                if let Some(name) = def.name() {
                    dict.entry(ctx[name].to_string())
                        .or_default()
                        .push(gql_def::Def { id: def.id });
                }
            }
        }

        dict.into_iter()
            .map(|(name, defs)| gql_dictionary::DefDictionaryEntry {
                name,
                definitions: defs,
            })
            .collect()
    }

    async fn vertices(
        def_id: String,
        first: i32,
        after: Option<String>,
        ctx: &OntologyCtx,
    ) -> FieldResult<gql_vertex::VertexConnection> {
        let data_store = ctx.data_store().map_err(field_error)?;
        let def_id = DefId::from_str(&def_id).map_err(|_| field_error("invalid def id format"))?;
        let _def = ctx
            .get_def(def_id)
            .ok_or_else(|| field_error("invalid def id"))?;

        let messages = [Ok(ReqMessage::Query(
            0,
            EntitySelect {
                source: StructOrUnionSelect::Struct(StructSelect {
                    def_id,
                    properties: Default::default(),
                }),
                filter: Filter::default(),
                limit: first.try_into().map_err(field_error)?,
                after_cursor: match after {
                    Some(after) => Some(
                        GraphQLCursor::deserialize(StringDeserializer::<serde_json::Error>::new(
                            after,
                        ))?
                        .0,
                    ),
                    None => None,
                },
                include_total_len: false,
            },
        ))];

        let sequences: Vec<_> = data_store
            .api()
            .transact(
                TransactionMode::ReadOnly,
                futures_util::stream::iter(messages).boxed(),
                ctx.session().clone(),
            )
            .await?
            .accumulate_sequences()
            .try_collect()
            .await?;

        let sequence = sequences
            .into_iter()
            .next()
            .ok_or_else(|| field_error("nothing returned"))?;

        gql_vertex::VertexConnection::from_sequence(sequence)
    }
}
