use std::{fmt::Display, sync::Arc};

use domain_engine_juniper::{create_graphql_schema, gql_scalar::GqlScalar, GqlContext, Schema};
use ontol_runtime::env::Env;
use ontol_test_utils::{TestCompile, TEST_PKG};
use unimock::Unimock;

mod test_graphql_basic;
mod test_graphql_input;

trait TestCompileSchema {
    fn schema_builder(self) -> SchemaBuilder;
}

impl<T: TestCompile> TestCompileSchema for T {
    fn schema_builder(self) -> SchemaBuilder {
        let env = self.compile_ok(|_| {});
        SchemaBuilder {
            env,
            api_mock: Unimock::new(()),
        }
    }
}

pub struct SchemaBuilder {
    env: Arc<Env>,
    api_mock: Unimock,
}

impl SchemaBuilder {
    fn api_mock<C: unimock::Clause>(self, f: impl Fn(&Env) -> C) -> Self {
        let api_mock = Unimock::new(f(&self.env));
        Self {
            env: self.env,
            api_mock,
        }
    }

    fn build(self) -> TestSchema {
        TestSchema {
            schema: create_graphql_schema(
                TEST_PKG,
                self.env,
                Arc::new(self.api_mock),
                Arc::new(domain_engine_core::Config::default()),
            )
            .unwrap(),
        }
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

struct TestSchema {
    schema: Schema,
}

#[async_trait::async_trait]
trait Exec {
    async fn exec(self, schema: &TestSchema) -> Result<juniper::Value<GqlScalar>, TestError>;
}

#[async_trait::async_trait]
impl Exec for &'static str {
    async fn exec(self, test_schema: &TestSchema) -> Result<juniper::Value<GqlScalar>, TestError> {
        match juniper::execute(
            self,
            None,
            &test_schema.schema,
            &juniper::Variables::new(),
            &GqlContext,
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
