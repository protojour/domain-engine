#![forbid(unsafe_code)]

use std::sync::Arc;

use context::{SchemaCtx, SchemaType, ServiceCtx};
use domain_engine_core::{
    data_store::{BatchWriteRequest, BatchWriteResponse},
    DomainError,
};
use gql_scalar::GqlScalar;
use look_ahead_utils::ArgsWrapper;
use ontol_runtime::{
    interface::{
        graphql::{
            data::{FieldData, FieldKind, Optionality, TypeModifier, TypeRef},
            schema::TypingPurpose,
        },
        DomainInterface,
    },
    ontology::Ontology,
    sequence::Sequence,
    value::{Value, ValueDebug},
    PackageId,
};
use templates::sequence_type::SequenceType;
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
    #[error("GraphQL interface not found for given package")]
    GraphqlInterfaceNotFound,
}

pub fn create_graphql_schema(
    package_id: PackageId,
    ontology: Arc<Ontology>,
) -> Result<Schema, CreateSchemaError> {
    let ontol_interface_schema = ontology
        .domain_interfaces(package_id)
        .iter()
        .map(|interface| match interface {
            DomainInterface::GraphQL(schema) => schema,
        })
        .next()
        .ok_or(CreateSchemaError::GraphqlInterfaceNotFound)?;

    // Don't spam the log system if the schema is very large
    let should_debug = ontol_interface_schema.types.len() < 100;

    let schema_ctx = Arc::new(SchemaCtx {
        schema: ontol_interface_schema.clone(),
        ontology,
    });

    let juniper_schema = Schema::new_with_info(
        templates::query_type::QueryType,
        templates::mutation_type::MutationType,
        juniper::EmptySubscription::new(),
        schema_ctx.query_schema_type(),
        schema_ctx.mutation_schema_type(),
        (),
    );

    if should_debug {
        debug!("Created schema \n{}", juniper_schema.as_sdl());
    }

    Ok(juniper_schema)
}

/// Execute a GraphQL query on the Domain Engine
async fn query(
    type_info: &SchemaType,
    field_name: &str,
    executor: &juniper::Executor<'_, '_, ServiceCtx, GqlScalar>,
) -> juniper::ExecutionResult<GqlScalar> {
    let schema_ctx = &type_info.schema_ctx;
    let query_field = type_info
        .type_data()
        .fields()
        .unwrap()
        .get(field_name)
        .unwrap();
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
                return Err(DomainError::EntityNotFound.into());
            }

            resolve_schema_type_field(
                AttributeType {
                    attr: &output.into(),
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
    type_info: &SchemaType,
    field_name: &str,
    executor: &juniper::Executor<'_, '_, ServiceCtx, GqlScalar>,
) -> juniper::ExecutionResult<GqlScalar> {
    let schema_ctx = &type_info.schema_ctx;
    let service_ctx = executor.context();
    let ctx = (schema_ctx.as_ref(), service_ctx);

    let look_ahead = executor.look_ahead();
    let args_wrapper = ArgsWrapper::new(look_ahead);
    let select_analyzer = SelectAnalyzer::new(schema_ctx, service_ctx);

    let field_data = type_info
        .type_data()
        .fields()
        .unwrap()
        .get(field_name)
        .unwrap();

    match &field_data.kind {
        FieldKind::EntityMutation {
            def_id,
            create_arg,
            update_arg,
            delete_arg,
            field_unit_type_addr,
        } => {
            let entity_mutations = args_wrapper.deserialize_entity_mutation_args(
                create_arg.as_ref(),
                update_arg.as_ref(),
                delete_arg.as_ref(),
                ctx,
            )?;
            let select = select_analyzer.analyze_select(look_ahead, field_data)?;
            let mut batch_write_requests = Vec::with_capacity(entity_mutations.len());

            for entity_mutation in entity_mutations {
                let values = entity_mutation
                    .inputs
                    .attrs
                    .into_iter()
                    .map(|attr| attr.val)
                    .collect();

                batch_write_requests.push(match entity_mutation.kind {
                    EntityMutationKind::Create => BatchWriteRequest::Insert(values, select.clone()),
                    EntityMutationKind::Update => BatchWriteRequest::Update(values, select.clone()),
                    EntityMutationKind::Delete => BatchWriteRequest::Delete(values, *def_id),
                });
            }

            let batch_write_responses = service_ctx
                .domain_engine
                .execute_writes(batch_write_requests, service_ctx.session.clone())
                .await?;

            let mut output_sequence = Sequence::default();

            for batch_write_response in batch_write_responses {
                match batch_write_response {
                    BatchWriteResponse::Inserted(values) | BatchWriteResponse::Updated(values) => {
                        output_sequence
                            .attrs
                            .extend(values.into_iter().map(Into::into));
                    }
                    BatchWriteResponse::Deleted(bools) => {
                        let bool_type = schema_ctx.ontology.ontol_domain_meta().bool;

                        output_sequence.attrs.extend(
                            bools
                                .into_iter()
                                .map(|bool| Value::I64(if bool { 1 } else { 0 }, bool_type).into()),
                        )
                    }
                }
            }

            resolve_schema_type_field(
                SequenceType {
                    seq: &output_sequence,
                },
                schema_ctx.get_schema_type(*field_unit_type_addr, TypingPurpose::Selection),
                executor,
            )
        }
        _ => panic!(),
    }
}
