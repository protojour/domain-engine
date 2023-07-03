use std::{fmt::Display, sync::Arc};

use domain_engine_core::{Config, EngineAPIMock};
use domain_engine_juniper::{create_graphql_schema, gql_scalar::GqlScalar, GqlContext, Schema};
use ontol_test_utils::{SourceName, TestCompile, TestEnv};
use unimock::*;

mod test_graphql_basic;
mod test_graphql_demo;
mod test_graphql_in_memory;
mod test_graphql_input;

trait TestCompileSchema {
    fn compile_schemas<const N: usize>(
        self,
        source_names: [SourceName; N],
    ) -> (TestEnv, [Schema; N]);
}

impl<T: TestCompile> TestCompileSchema for T {
    fn compile_schemas<const N: usize>(
        self,
        source_names: [SourceName; N],
    ) -> (TestEnv, [Schema; N]) {
        let mut test_env = self.compile_ok(|_| {});
        // Don't want JSON schema noise in GraphQL tests:
        test_env.compile_json_schema = false;

        let schemas: [Schema; N] = source_names.map(|source_name| {
            create_graphql_schema(test_env.get_package_id(source_name.0), test_env.env.clone())
                .unwrap()
        });

        (test_env, schemas)
    }
}

#[derive(Debug, Eq, PartialEq)]
enum TestError {
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

pub fn mock_default_config() -> impl unimock::Clause {
    EngineAPIMock::get_config
        .each_call(matching!())
        .returns(Config::default())
}

pub fn mock_query_entities_empty() -> impl unimock::Clause {
    EngineAPIMock::query_entities
        .next_call(matching!(_))
        .returns(Ok(vec![]))
}

pub fn mock_gql_context(setup: impl unimock::Clause) -> GqlContext {
    GqlContext {
        engine_api: Arc::new(Unimock::new(setup)),
    }
}

#[async_trait::async_trait]
trait Exec {
    async fn exec(
        self,
        schema: &Schema,
        context: &GqlContext,
    ) -> Result<juniper::Value<GqlScalar>, TestError>;
}

#[async_trait::async_trait]
impl Exec for &'static str {
    async fn exec(
        self,
        schema: &Schema,
        context: &GqlContext,
    ) -> Result<juniper::Value<GqlScalar>, TestError> {
        match juniper::execute(self, None, schema, &juniper::Variables::new(), context).await {
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

fn main() {}
