#![forbid(unsafe_code)]

use std::sync::Arc;

use context::{SchemaCtx, SchemaType, ServiceCtx};
use domain_engine_core::DomainError;
use gql_scalar::GqlScalar;
use juniper::LookAheadMethods;
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
    select::{Select, StructOrUnionSelect},
    sequence::Sequence,
    value::ValueDebug,
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

    debug!("Created schema \n{}", juniper_schema.as_schema_language());

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

    let output = {
        let service_ctx = executor.context();
        let AnalyzedQuery {
            map_key,
            input,
            mut selects,
        } = SelectAnalyzer::new(schema_ctx, service_ctx)
            .analyze_look_ahead(&executor.look_ahead(), query_field)?;

        service_ctx
            .domain_engine
            .exec_map(map_key, input, &mut selects)
            .await?
    };

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
    let args_wrapper = ArgsWrapper::new(look_ahead.arguments());
    let query_analyzer = SelectAnalyzer::new(schema_ctx, service_ctx);

    let field_data = type_info
        .type_data()
        .fields()
        .unwrap()
        .get(field_name)
        .unwrap();

    match &field_data.kind {
        FieldKind::EntityMutation {
            create_arg,
            update_arg,
            delete_arg,
            field_unit_type_addr,
        } => {
            let entity_mutations = args_wrapper
                .deserialize_entity_mutation_args(create_arg, update_arg, delete_arg, ctx)?;
            let struct_query = query_analyzer.analyze_struct_select(&look_ahead, field_data)?;
            let query = match struct_query {
                StructOrUnionSelect::Struct(struct_query) => Select::Struct(struct_query),
                StructOrUnionSelect::Union(def_id, structs) => Select::StructUnion(def_id, structs),
            };

            let mut output_sequence = Sequence::default();

            // FIXME: This could get a nice dose of transaction management
            for entity_mutation in entity_mutations {
                match entity_mutation.kind {
                    EntityMutationKind::Create => {
                        for input_attr in entity_mutation.inputs.attrs {
                            let value = service_ctx
                                .domain_engine
                                // FIXME: Avoid query clone
                                .store_new_entity(input_attr.value, query.clone())
                                .await?;
                            output_sequence.attrs.push(value.into());
                        }
                    }
                    EntityMutationKind::Update => {
                        todo!()
                    }
                    EntityMutationKind::Delete => {
                        todo!()
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
        FieldKind::CreateMutation { input } => {
            let input_attribute =
                args_wrapper.deserialize_domain_field_arg_as_attribute(input, ctx)?;
            let struct_query = query_analyzer.analyze_struct_select(&look_ahead, field_data)?;
            let query = match struct_query {
                StructOrUnionSelect::Struct(struct_query) => Select::Struct(struct_query),
                StructOrUnionSelect::Union(def_id, structs) => Select::StructUnion(def_id, structs),
            };

            trace!(
                "CREATE {} -> {query:#?}",
                ValueDebug(&input_attribute.value)
            );

            let value = service_ctx
                .domain_engine
                .store_new_entity(input_attribute.value, query)
                .await?;

            resolve_schema_type_field(
                AttributeType {
                    attr: &value.into(),
                },
                schema_ctx
                    .find_schema_type_by_unit(field_data.field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            )
        }
        FieldKind::UpdateMutation { id, input } => {
            let id_attribute = args_wrapper.deserialize_domain_field_arg_as_attribute(id, ctx)?;
            let input_attribute =
                args_wrapper.deserialize_domain_field_arg_as_attribute(input, ctx)?;

            let query = query_analyzer.analyze_struct_select(&look_ahead, field_data);

            trace!(
                "UPDATE {} -> {} -> {query:#?}",
                ValueDebug(&id_attribute.value),
                ValueDebug(&input_attribute.value)
            );
            Ok(juniper::Value::Null)
        }
        FieldKind::DeleteMutation { id } => {
            let id_value = args_wrapper.deserialize_domain_field_arg_as_attribute(id, ctx)?;

            trace!("DELETE {id_value:?}");

            Ok(juniper::Value::Null)
        }
        _ => panic!(),
    }
}
