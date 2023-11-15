use std::fmt::{Debug, Display};

use domain_engine_core::{
    data_store::{DataStoreAPIMock, DefaultDataStoreFactory},
    DomainEngine,
};
use domain_engine_juniper::{
    context::ServiceCtx, create_graphql_schema, gql_scalar::GqlScalar, Schema,
};
use ontol_runtime::sequence::Sequence;
use ontol_test_utils::{OntolTest, SourceName, TestCompile};
use unimock::*;

pub trait TestCompileSchema {
    fn compile_schemas<const N: usize>(
        self,
        source_names: [SourceName; N],
    ) -> (OntolTest, [Schema; N]);
}

impl<T: TestCompile> TestCompileSchema for T {
    fn compile_schemas<const N: usize>(
        self,
        source_names: [SourceName; N],
    ) -> (OntolTest, [Schema; N]) {
        let mut ontol_test = self.compile();
        // Don't want JSON schema noise in GraphQL tests:
        ontol_test.compile_json_schema = false;

        let schemas: [Schema; N] = source_names.map(|source_name| {
            create_graphql_schema(
                ontol_test.get_package_id(source_name.0),
                ontol_test.ontology.clone(),
            )
            .unwrap()
        });

        (ontol_test, schemas)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TestError {
    GraphQL(juniper::GraphQLError),
    Execution(Vec<juniper::ExecutionError<GqlScalar>>),
}

impl Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GraphQL(err) => write!(f, "GraphQL: {err}"),
            Self::Execution(errors) => {
                write!(f, "Execution: ")?;
                let mut iter = errors.iter().peekable();
                while let Some(execution_error) = iter.next() {
                    let message = execution_error.error().message();
                    let pos = execution_error.location();

                    write!(
                        f,
                        "{message} (field at line {} column {})",
                        pos.line(),
                        pos.column()
                    )?;
                    if iter.peek().is_some() {
                        write!(f, ", ")?;
                    }
                }
                Ok(())
            }
        }
    }
}

pub fn mock_data_store_query_entities_empty() -> impl unimock::Clause {
    DataStoreAPIMock::query
        .next_call(matching!(_, _))
        .returns(Ok(Sequence::new([])))
}

pub async fn gql_ctx_mock_data_store(
    ontol_test: &OntolTest,
    data_store_package: SourceName,
    setup: impl unimock::Clause,
) -> ServiceCtx {
    DomainEngine::test_builder(ontol_test.ontology.clone())
        .mock_data_store(ontol_test.get_package_id(data_store_package.0), setup)
        .build(DefaultDataStoreFactory)
        .await
        .unwrap()
        .into()
}

#[async_trait::async_trait]
pub trait Exec {
    async fn exec(
        self,
        variables: impl Into<juniper::Variables<GqlScalar>> + Send,
        schema: &Schema,
        context: &ServiceCtx,
    ) -> Result<juniper::Value<GqlScalar>, TestError>;
}

#[async_trait::async_trait]
impl Exec for &'static str {
    async fn exec(
        self,
        variables: impl Into<juniper::Variables<GqlScalar>> + Send,
        schema: &Schema,
        context: &ServiceCtx,
    ) -> Result<juniper::Value<GqlScalar>, TestError> {
        let variables = variables.into();
        match juniper::execute(self, None, schema, &variables, context).await {
            Ok((value, execution_errors)) => {
                if !execution_errors.is_empty() {
                    Err(TestError::Execution(execution_errors))
                } else {
                    Ok(value)
                }
            }
            Err(error) => Err(TestError::GraphQL(error)),
        }
    }
}

pub trait GraphqlTestResultExt {
    fn unwrap_first_graphql_error_msg(self) -> String;
    fn unwrap_first_exec_error_msg(self) -> String;
}

impl<T: Debug> GraphqlTestResultExt for Result<T, TestError> {
    #[track_caller]
    fn unwrap_first_graphql_error_msg(self) -> String {
        match self.unwrap_err() {
            TestError::GraphQL(error) => error.to_string(),
            TestError::Execution(_) => {
                panic!("Error was an Execution error");
            }
        }
    }

    #[track_caller]
    fn unwrap_first_exec_error_msg(self) -> String {
        match self.unwrap_err() {
            TestError::Execution(errors) => {
                let error = errors.into_iter().next().expect("No errors");
                error.error().message().to_string()
            }
            TestError::GraphQL(_) => {
                panic!("Error was a GraphQL error");
            }
        }
    }
}
