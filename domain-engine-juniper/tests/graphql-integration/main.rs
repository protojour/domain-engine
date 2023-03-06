use domain_engine_juniper::{create_graphql_schema, gql_scalar::GqlScalar, GqlContext, Schema};
use juniper::graphql_value;
use ontol_test_utils::{TestCompile, TEST_PKG};
use test_log::test;

trait TestCompileSchema {
    fn compile_schema(self) -> TestSchema;
}

impl<T: TestCompile> TestCompileSchema for T {
    fn compile_schema(self) -> TestSchema {
        let env = self.compile_ok(|_| {});
        TestSchema {
            schema: create_graphql_schema(env, TEST_PKG).unwrap(),
        }
    }
}

#[derive(Debug)]
enum TestError<'a> {
    GraphQL(juniper::GraphQLError<'a>),
    Execution(Vec<juniper::ExecutionError<GqlScalar>>),
}

struct TestSchema {
    schema: Schema,
}

#[async_trait::async_trait]
trait Exec {
    async fn exec<'a>(
        self,
        schema: &'a TestSchema,
    ) -> Result<juniper::Value<GqlScalar>, TestError<'a>>;
}

#[async_trait::async_trait]
impl Exec for &'static str {
    async fn exec<'a>(
        self,
        test_schema: &'a TestSchema,
    ) -> Result<juniper::Value<GqlScalar>, TestError<'a>> {
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

#[test]
fn test_create_empty_schema() {
    "".compile_ok(|env| {
        create_graphql_schema(env, TEST_PKG).unwrap();
    });
}

#[test(tokio::test)]
async fn test_basic_schema() {
    let schema = "
    type foo {
        rel [id] string
        rel ['prop'] int
    }
    "
    .compile_schema();

    assert_eq!(
        "{
            fooList {
                edges {
                    node {
                        prop
                    }
                }
            }    
        }"
        .exec(&schema)
        .await
        .unwrap(),
        graphql_value!({
            "fooList": None,
        }),
    );

    assert_eq!(
        "mutation {
            createfoo(
                input: {
                    prop: 42
                }
            ) {
                prop
            }
        }"
        .exec(&schema)
        .await
        .unwrap(),
        graphql_value!(None),
    );
}

fn main() {}
