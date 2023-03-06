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

impl TestSchema {
    async fn exec<'a>(&'a self, doc: &'a str) -> Result<juniper::Value<GqlScalar>, TestError<'a>> {
        match juniper::execute(
            doc,
            None,
            &self.schema,
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

    let doc = "{
        fooList {
            edges {
                node {
                    prop
                }
            }
        }
    }";

    assert_eq!(
        graphql_value!({
            "fooList": None,
        }),
        schema.exec(doc).await.unwrap()
    );
}

fn main() {}
