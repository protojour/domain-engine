use std::{fmt::Display, sync::Arc};

use domain_engine_juniper::{create_graphql_schema, gql_scalar::GqlScalar, GqlContext, Schema};
use ontol_runtime::env::Env;
use ontol_test_utils::{TestCompile, TEST_PKG};
use unimock::Unimock;

mod test_graphql_basic;
mod test_graphql_input;

trait TestCompileSchema {
    fn builder(self) -> ContextBuilder;
}

impl<T: TestCompile> TestCompileSchema for T {
    fn builder(self) -> ContextBuilder {
        let env = self.compile_ok(|_| {});
        ContextBuilder {
            env,
            api_mock: Unimock::new(()),
        }
    }
}

pub struct ContextBuilder {
    env: Arc<Env>,
    api_mock: Unimock,
}

impl ContextBuilder {
    fn api_mock<C: unimock::Clause>(self, f: impl Fn(&Env) -> C) -> Self {
        Self {
            api_mock: Unimock::new(f(&self.env)),
            env: self.env,
        }
    }

    fn build(self) -> TestContext {
        TestContext {
            schema: create_graphql_schema(TEST_PKG, self.env).unwrap(),
            context: GqlContext {
                config: Arc::new(domain_engine_core::Config::default()),
                domain_api: Arc::new(self.api_mock),
            },
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

struct TestContext {
    schema: Schema,
    context: GqlContext,
}

#[async_trait::async_trait]
trait Exec {
    async fn exec(self, ctx: &TestContext) -> Result<juniper::Value<GqlScalar>, TestError>;
}

#[async_trait::async_trait]
impl Exec for &'static str {
    async fn exec(self, ctx: &TestContext) -> Result<juniper::Value<GqlScalar>, TestError> {
        match juniper::execute(
            self,
            None,
            &ctx.schema,
            &juniper::Variables::new(),
            &ctx.context,
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
