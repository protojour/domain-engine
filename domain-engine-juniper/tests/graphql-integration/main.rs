use std::{fmt::Display, sync::Arc};

use domain_engine_core::{Config, EngineAPIMock};
use domain_engine_juniper::{create_graphql_schema, gql_scalar::GqlScalar, GqlContext, Schema};
use ontol_runtime::env::Env;
use ontol_test_utils::{TestCompile, TEST_PKG};
use unimock::*;

mod test_graphql_basic;
mod test_graphql_input;

trait TestCompileSchema {
    fn compile_schema(self) -> (Arc<Env>, Schema);
}

impl<T: TestCompile> TestCompileSchema for T {
    fn compile_schema(self) -> (Arc<Env>, Schema) {
        let env = self.compile_ok(|_| {});
        (env.clone(), create_graphql_schema(TEST_PKG, env).unwrap())
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
        .next_call(matching!(_, _))
        .returns(Ok(vec![]))
}

#[async_trait::async_trait]
trait MockExec {
    async fn mock_exec(
        self,
        schema: &Schema,
        mock_clause: impl unimock::Clause + Send,
    ) -> Result<juniper::Value<GqlScalar>, TestError>;
}

#[async_trait::async_trait]
impl MockExec for &'static str {
    async fn mock_exec(
        self,
        schema: &Schema,
        mock_clause: impl unimock::Clause + Send,
    ) -> Result<juniper::Value<GqlScalar>, TestError> {
        let unimock = Unimock::new(mock_clause);

        match juniper::execute(
            self,
            None,
            schema,
            &juniper::Variables::new(),
            &GqlContext {
                domain_api: Arc::new(unimock),
            },
        )
        .await
        {
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
