use std::collections::BTreeMap;
use std::str::FromStr;

use crate::cursor_util::GraphQLCursor;
use crate::field_error;
use crate::gql_scalar::GqlScalar;
use crate::juniper::{self, graphql_object, FieldResult};

use domain_engine_core::domain_select::domain_select_no_edges;
use domain_engine_core::transact::AccumulateSequences;
use domain_engine_core::transact::ReqMessage;
use domain_engine_core::transact::TransactionMode;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use ontol_runtime::ontology::domain::{FieldPath, VertexOrder};
use ontol_runtime::query::filter::Filter;
use ontol_runtime::query::order::Direction;
use ontol_runtime::query::select::StructOrUnionSelect;
use ontol_runtime::query::select::StructSelect;
use ontol_runtime::query::select::{EntitySelect, Select};
use ontol_runtime::PropId;
use serde::de::value::StringDeserializer;
use serde::Deserialize;
use ulid::Ulid;

use super::gql_dictionary;
use super::gql_dictionary::DefDictionaryEntry;
use super::gql_domain;
use super::gql_value::ValueScalarCfg;
use super::gql_vertex::{self, VertexOrderDirection};
use super::OntologyCtx;
use super::{gql_def, gql_id};

#[derive(Default)]
pub struct Query;

#[graphql_object]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
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
            Ok(gql_domain::Domain { domain_index: id })
        } else {
            Err(field_error("Domain not found"))
        }
    }

    fn domains(ctx: &OntologyCtx) -> FieldResult<Vec<gql_domain::Domain>> {
        let mut domains = vec![];
        for (domain_index, _ontology_domain) in ctx.domains() {
            let domain = gql_domain::Domain { domain_index };
            domains.push(domain);
        }
        Ok(domains)
    }

    fn def(def_id: gql_id::DefId, ctx: &OntologyCtx) -> FieldResult<gql_def::Def> {
        let def_id = def_id.as_runtime_def_id(ctx)?;
        if let Some(def) = ctx.get_def(def_id) {
            return Ok(gql_def::Def { id: def.id });
        }
        Err(field_error("Def not found"))
    }

    fn def_dictionary(ctx: &OntologyCtx) -> Vec<DefDictionaryEntry> {
        let mut dict: BTreeMap<String, Vec<gql_def::Def>> = Default::default();

        for (_, domain) in ctx.domains() {
            for def in domain.defs() {
                if let Some(name) = def.ident() {
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
        def_id: gql_id::DefId,
        first: i32,
        after: Option<String>,
        with_address: Option<bool>,
        with_def_id: Option<bool>,
        order: Option<Vec<gql_vertex::VertexOrder>>,
        ctx: &OntologyCtx,
    ) -> FieldResult<gql_vertex::VertexConnection> {
        let data_store = ctx.data_store().map_err(field_error)?;
        let def_id = def_id.as_runtime_def_id(ctx)?;

        let _def = ctx
            .get_def(def_id)
            .ok_or_else(|| field_error("invalid def id"))?;
        let cfg = ValueScalarCfg {
            with_address: with_address.unwrap_or(true),
            with_def_id: with_def_id.unwrap_or(true),
        };

        let mut filter = Filter::default_for_datastore();

        if let Some(order) = order {
            let mut vertex_order_chain = vec![];
            for vertex_order in order {
                let mut field_path_tuple = vec![];

                for vertex_order in vertex_order.field_paths {
                    let mut field_path = vec![];

                    for prop in vertex_order {
                        let prop_id = PropId::from_str(&prop)
                            .map_err(|_| "invalid property id in vertex order")?;

                        field_path.push(prop_id);
                    }

                    field_path_tuple.push(FieldPath(field_path.into_boxed_slice()));
                }

                vertex_order_chain.push(VertexOrder {
                    tuple: field_path_tuple.into_boxed_slice(),
                    direction: match vertex_order.direction {
                        Some(VertexOrderDirection::Ascending) | None => Direction::Ascending,
                        Some(VertexOrderDirection::Descending) => Direction::Descending,
                    },
                })
            }

            filter.set_vertex_order(vertex_order_chain);
        }

        let struct_select = match domain_select_no_edges(def_id, ctx) {
            Select::Struct(struct_select) => struct_select,
            _ => StructSelect {
                def_id,
                properties: Default::default(),
            },
        };

        let messages = [Ok(ReqMessage::Query(
            0,
            EntitySelect {
                source: StructOrUnionSelect::Struct(struct_select),
                filter,
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

        gql_vertex::VertexConnection::from_sequence(sequence, cfg, ctx)
    }
}
