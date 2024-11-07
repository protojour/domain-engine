use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display},
    sync::Arc,
};

use domain_engine_core::{data_store::DataStore, DomainEngine, Session};
use domain_engine_graphql::{
    domain::{context::ServiceCtx, create_graphql_schema, DomainSchema},
    gql_scalar::GqlScalar,
    juniper,
};
use domain_engine_store_inmemory::InMemoryDataStoreFactory;
use juniper::ScalarValue;
use ontol_test_utils::{
    default_file_url, default_short_name, OntolTest, TestCompile, TestPackages,
};
use ordered_float::NotNan;
use tracing::info;
use unimock::*;

use crate::mock_datastore::LinearDataStoreAdapter;

pub trait TestCompileSchema {
    fn compile_schemas<const N: usize>(
        self,
        source_names: [&str; N],
    ) -> (OntolTest, [DomainSchema; N]);
}

impl<T: TestCompile> TestCompileSchema for T {
    #[track_caller]
    fn compile_schemas<const N: usize>(
        self,
        source_names: [&str; N],
    ) -> (OntolTest, [DomainSchema; N]) {
        compile_schemas_inner(self.compile(), source_names)
    }
}

pub trait TestCompileSingletonSchema {
    fn compile_single_schema(self) -> (OntolTest, DomainSchema);
}

impl TestCompileSingletonSchema for &'static str {
    #[track_caller]
    fn compile_single_schema(self) -> (OntolTest, DomainSchema) {
        let (ontol_test, [schema]) = compile_schemas_inner(
            TestPackages::with_static_sources([(default_file_url(), self)]).compile(),
            [default_short_name()],
        );
        (ontol_test, schema)
    }
}

impl TestCompileSingletonSchema for Arc<String> {
    #[track_caller]
    fn compile_single_schema(self) -> (OntolTest, DomainSchema) {
        let (ontol_test, [schema]) = compile_schemas_inner(
            TestPackages::with_sources([(default_file_url(), self)]).compile(),
            [default_short_name()],
        );
        (ontol_test, schema)
    }
}

fn compile_schemas_inner<const N: usize>(
    mut ontol_test: OntolTest,
    short_names: [&str; N],
) -> (OntolTest, [DomainSchema; N]) {
    // Don't want JSON schema noise in GraphQL tests:
    ontol_test.set_compile_json_schema(false);

    let schemas: [DomainSchema; N] = short_names.map(|short_name| {
        create_graphql_schema(
            ontol_test.ontology_owned(),
            ontol_test.get_domain_index(short_name),
        )
        .unwrap()
    });

    (ontol_test, schemas)
}

#[derive(Debug, Eq, PartialEq)]
pub enum TestError<S> {
    GraphQL(juniper::GraphQLError),
    Execution(Vec<juniper::ExecutionError<S>>),
}

impl Display for TestError<GqlScalar> {
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

pub fn gql_ctx_mock_data_store(
    ontol_test: &OntolTest,
    data_store_packages: &[&str],
    setup: impl unimock::Clause,
) -> ServiceCtx {
    let unimock = Unimock::new(setup);
    let domain_engine = DomainEngine::builder(ontol_test.ontology_owned())
        .data_store(DataStore::new(
            data_store_packages
                .iter()
                .map(|src_name| ontol_test.get_domain_index(src_name.as_ref()))
                .collect(),
            Arc::new(LinearDataStoreAdapter::new(unimock.clone())),
        ))
        .system(Box::new(unimock))
        .build_sync(InMemoryDataStoreFactory, Session::default())
        .unwrap();

    ServiceCtx::from(Arc::new(domain_engine))
}

#[async_trait::async_trait]
pub trait Exec {
    async fn exec<QT, MT, ST, S>(
        self,
        variables: impl Into<juniper::Variables<S>> + Send,
        schema: &juniper::RootNode<QT, MT, ST, S>,
        context: &QT::Context,
    ) -> Result<juniper::Value<S>, TestError<S>>
    where
        QT: juniper::GraphQLTypeAsync<S, Context: Sync, TypeInfo: Sync>,
        MT: juniper::GraphQLTypeAsync<S, Context = QT::Context, TypeInfo: Sync>,
        ST: juniper::GraphQLType<S, Context = QT::Context, TypeInfo: Sync> + Sync,
        S: juniper::ScalarValue + Send + Sync;
}

#[async_trait::async_trait]
impl Exec for &str {
    async fn exec<QT, MT, ST, S>(
        self,
        variables: impl Into<juniper::Variables<S>> + Send,
        schema: &juniper::RootNode<QT, MT, ST, S>,
        context: &QT::Context,
    ) -> Result<juniper::Value<S>, TestError<S>>
    where
        QT: juniper::GraphQLTypeAsync<S, Context: Sync, TypeInfo: Sync>,
        MT: juniper::GraphQLTypeAsync<S, Context = QT::Context, TypeInfo: Sync>,
        ST: juniper::GraphQLType<S, Context = QT::Context, TypeInfo: Sync> + Sync,
        S: juniper::ScalarValue + Send + Sync,
    {
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

impl<T: Debug, S> GraphqlTestResultExt for Result<T, TestError<S>> {
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

pub trait GraphqlValueResultExt<S> {
    fn unordered(self) -> Result<UnorderedValue, TestError<S>>;
}

impl<S> GraphqlValueResultExt<S> for Result<juniper::Value<GqlScalar>, TestError<S>> {
    fn unordered(self) -> Result<UnorderedValue, TestError<S>> {
        self.map(|value| value.unordered())
    }
}

pub trait ValueExt {
    fn field(&self, name: &str) -> &juniper::Value<GqlScalar>;
    fn opt_field(&self, name: &str) -> Option<&juniper::Value<GqlScalar>>;
    fn element(&self, index: usize) -> &juniper::Value<GqlScalar>;
    fn scalar(&self) -> &GqlScalar;
    fn unordered(&self) -> UnorderedValue;
}

impl ValueExt for juniper::Value<GqlScalar> {
    fn field(&self, name: &str) -> &juniper::Value<GqlScalar> {
        self.opt_field(name)
            .unwrap_or_else(|| panic!("field `{name}` was not present"))
    }

    fn opt_field(&self, name: &str) -> Option<&juniper::Value<GqlScalar>> {
        self.as_object_value()
            .expect("not an object")
            .get_field_value(name)
    }

    fn element(&self, index: usize) -> &juniper::Value<GqlScalar> {
        &self.as_list_value().expect("not a list")[index]
    }

    fn scalar(&self) -> &GqlScalar {
        self.as_scalar_value().unwrap()
    }

    fn unordered(&self) -> UnorderedValue {
        match self {
            juniper::Value::Null => UnorderedValue::Null,
            juniper::Value::Scalar(GqlScalar::Boolean(bool)) => UnorderedValue::Boolean(*bool),
            juniper::Value::Scalar(GqlScalar::I32(int)) => UnorderedValue::Int((*int).into()),
            juniper::Value::Scalar(GqlScalar::I64(int)) => UnorderedValue::Int(*int),
            juniper::Value::Scalar(GqlScalar::F64(float)) => {
                UnorderedValue::Float(NotNan::new(*float).unwrap())
            }
            juniper::Value::Scalar(GqlScalar::String(string)) => {
                UnorderedValue::String(string.to_string())
            }
            juniper::Value::List(elements) => {
                UnorderedValue::Set(elements.iter().map(Self::unordered).collect())
            }
            juniper::Value::Object(object) => UnorderedValue::Object(
                object
                    .iter()
                    .map(|(key, val)| (key.clone(), val.unordered()))
                    .collect(),
            ),
        }
    }
}

/// A macro for creating an UnorderedValue
#[macro_export]
macro_rules! graphql_value_unordered {
    ($($input:tt)*) => {
        $crate::graphql_test_utils::ValueExt::unordered(&juniper::graphql_value!($($input)*))
    }
}

/// Version of juniper::Value that does not care about ordering of arrays
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum UnorderedValue {
    Null,
    Boolean(bool),
    Int(i64),
    Float(NotNan<f64>),
    String(String),
    Set(BTreeSet<UnorderedValue>),
    Object(BTreeMap<String, UnorderedValue>),
}

pub struct GraphQLPageDebug {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
    pub total_count: Option<i32>,
}

impl GraphQLPageDebug {
    pub fn parse_connection(
        response: &juniper::Value<GqlScalar>,
        connection_name: &str,
    ) -> Option<Self> {
        let connection = response.field(connection_name);
        let total_count = connection.opt_field("totalCount");
        let page_info = connection.field("pageInfo");

        info!("page_info: {page_info:?}");

        let has_next_page = page_info.field("hasNextPage").as_scalar()?.as_bool()?;
        let end_cursor = page_info.opt_field("endCursor")?.as_scalar()?.as_string();

        Some(Self {
            total_count: total_count
                .and_then(|count| count.as_scalar())
                .and_then(|scalar| scalar.as_int()),
            has_next_page,
            end_cursor,
        })
    }
}
