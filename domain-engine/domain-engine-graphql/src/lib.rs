#![forbid(unsafe_code)]

use std::{fmt::Display, sync::Arc};

use ::juniper::{graphql_value, FieldError};
use context::{SchemaCtx, SchemaType, ServiceCtx};
use domain_engine_core::{
    domain_error::DomainErrorKind,
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
    DomainResult,
};
use futures_util::{StreamExt, TryStreamExt};
use gql_scalar::GqlScalar;
use look_ahead_utils::ArgsWrapper;
use ontol_runtime::{
    attr::{AttrMatrixRef, AttrRef},
    interface::{
        graphql::{
            data::{EntityMutationField, FieldData, FieldKind, Optionality, TypeModifier, TypeRef},
            schema::TypingPurpose,
        },
        DomainInterface,
    },
    ontology::Ontology,
    sequence::Sequence,
    value::ValueDebug,
    DomainIndex,
};
use templates::matrix_type::MatrixType;
use thiserror::Error;
use tracing::{debug, trace};

use crate::{
    look_ahead_utils::EntityMutationKind,
    select_analyzer::{AnalyzedQuery, SelectAnalyzer},
    templates::{attribute_type::AttributeType, resolve_schema_type_field},
};

pub mod context;
pub mod cursor_util;
pub mod gql_scalar;
pub mod ontology_schema;

mod look_ahead_utils;
mod macros;
mod registry_ctx;
mod select_analyzer;
mod templates;
mod value_serializer;

pub mod juniper {
    pub use ::juniper::*;
}

pub type Schema = juniper::RootNode<
    'static,
    templates::query_type::QueryType,
    templates::mutation_type::MutationType,
    juniper::EmptySubscription<ServiceCtx>,
    GqlScalar,
>;

#[derive(Debug, Error)]
pub enum CreateSchemaError {
    #[error("GraphQL interface not found for the given domain")]
    GraphqlInterfaceNotFound,
}

pub trait CreateGraphqlSchema {}

pub fn create_graphql_schema(
    ontology: Arc<Ontology>,
    domain_index: DomainIndex,
) -> Result<Schema, CreateSchemaError> {
    let ontol_interface_schema = ontology
        .domain_interfaces(domain_index)
        .iter()
        .filter_map(|interface| match interface {
            DomainInterface::GraphQL(schema) => Some(schema),
            _ => None,
        })
        .next()
        .ok_or(CreateSchemaError::GraphqlInterfaceNotFound)?;

    // Don't spam the log system if the schema is very large
    let should_debug = ontol_interface_schema.types.len() < 50;

    let schema_ctx = Arc::new(SchemaCtx {
        schema: ontol_interface_schema.clone(),
        ontology,
    });

    let mut juniper_schema = Schema::new_with_info(
        templates::query_type::QueryType,
        templates::mutation_type::MutationType,
        juniper::EmptySubscription::new(),
        schema_ctx.query_schema_type(),
        schema_ctx.mutation_schema_type(),
        (),
    );
    juniper_schema.argument_validation_disabled = true;

    if should_debug {
        debug!("Created schema \n{}", juniper_schema.as_sdl());
    } else {
        debug!("Created schema too large to debug");
    }

    Ok(juniper_schema)
}

/// Execute a GraphQL query on the Domain Engine
async fn query(
    def: &SchemaType,
    field_name: &str,
    executor: &juniper::Executor<'_, '_, ServiceCtx, GqlScalar>,
) -> juniper::ExecutionResult<GqlScalar> {
    let schema_ctx = &def.schema_ctx;
    let query_field = def.type_data().fields().unwrap().get(field_name).unwrap();
    let service_ctx = executor.context();

    match SelectAnalyzer::new(schema_ctx, service_ctx)
        .analyze_look_ahead(executor.look_ahead(), query_field)?
    {
        AnalyzedQuery::Map {
            map_key,
            input,
            mut selects,
        } => {
            debug!("exec_map with selects: {selects:?}");

            let output = service_ctx
                .domain_engine
                .exec_map(map_key, input, &mut selects, service_ctx.session.clone())
                .await?;

            trace!("query result: {}", ValueDebug(&output));

            if output.is_unit()
                && matches!(
                    query_field,
                    FieldData {
                        kind: FieldKind::MapFind { .. },
                        field_type: TypeRef {
                            modifier: TypeModifier::Unit(Optionality::Mandatory),
                            ..
                        }
                    }
                )
            {
                return Err(DomainErrorKind::EntityNotFound.into());
            }

            resolve_schema_type_field(
                AttributeType {
                    attr: AttrRef::Unit(&output),
                },
                schema_ctx
                    .find_schema_type_by_unit(query_field.field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            )
        }
        AnalyzedQuery::Version => Ok(juniper::Value::Scalar(GqlScalar::String("0.0.0".into()))),
    }
}

/// Execute a GraphQL mutation on the Domain Engine
async fn mutation(
    def: &SchemaType,
    field_name: &str,
    executor: &juniper::Executor<'_, '_, ServiceCtx, GqlScalar>,
) -> juniper::ExecutionResult<GqlScalar> {
    let schema_ctx = &def.schema_ctx;
    let service_ctx = executor.context();
    let ctx = (schema_ctx.as_ref(), service_ctx);

    let look_ahead = executor.look_ahead();
    let args_wrapper = ArgsWrapper::new(look_ahead);
    let select_analyzer = SelectAnalyzer::new(schema_ctx, service_ctx);

    let field_data = def.type_data().fields().unwrap().get(field_name).unwrap();

    match &field_data.kind {
        FieldKind::EntityMutation(field) => {
            let EntityMutationField {
                def_id,
                create_arg,
                update_arg,
                delete_arg,
                field_unit_type_addr,
            } = field.as_ref();

            let entity_mutations = args_wrapper.deserialize_entity_mutation_args(
                create_arg.as_ref(),
                update_arg.as_ref(),
                delete_arg.as_ref(),
                ctx,
            )?;
            let select = select_analyzer.analyze_select(look_ahead, field_data)?;

            let mut req_messages: Vec<DomainResult<ReqMessage>> = vec![];
            let mut op_seq = 0;

            for entity_mutation in entity_mutations {
                let values: Vec<_> = entity_mutation
                    .inputs
                    .into_elements()
                    .into_iter()
                    // .map(|attr| attr.val)
                    .collect();

                match entity_mutation.kind {
                    EntityMutationKind::Create => {
                        req_messages.push(Ok(ReqMessage::Insert(op_seq, select.clone())));
                        op_seq += 1;

                        for value in values {
                            req_messages.push(Ok(ReqMessage::Argument(value)));
                        }
                    }
                    EntityMutationKind::Update => {
                        req_messages.push(Ok(ReqMessage::Update(op_seq, select.clone())));
                        op_seq += 1;

                        for value in values {
                            req_messages.push(Ok(ReqMessage::Argument(value)));
                        }
                    }
                    EntityMutationKind::Delete => {
                        req_messages.push(Ok(ReqMessage::Delete(op_seq, *def_id)));
                        op_seq += 1;

                        for value in values {
                            req_messages.push(Ok(ReqMessage::Argument(value)));
                        }
                    }
                }
            }

            let response_sequences: Vec<_> = service_ctx
                .domain_engine
                .transact(
                    TransactionMode::ReadWriteAtomic,
                    futures_util::stream::iter(req_messages).boxed(),
                    service_ctx.session.clone(),
                )
                .await?
                .accumulate_sequences()
                .try_collect()
                .await?;

            let mut output_sequence = Sequence::default();

            for response_sequence in response_sequences {
                for value in response_sequence.into_elements() {
                    output_sequence.push(value);
                }
            }

            resolve_schema_type_field(
                MatrixType {
                    matrix: AttrMatrixRef::from_ref(&output_sequence),
                },
                schema_ctx.get_schema_type(*field_unit_type_addr, TypingPurpose::Selection),
                executor,
            )
        }
        _ => panic!(),
    }
}

fn field_error<S>(msg: impl Display) -> FieldError<S> {
    FieldError::new(msg, graphql_value!(None))
}
