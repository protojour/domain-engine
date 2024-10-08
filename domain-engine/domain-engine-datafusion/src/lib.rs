use std::{any::Any, fmt::Debug, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{self, CatalogProvider, SchemaProvider, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, ExecutionMode, ExecutionPlan, Partitioning,
        PlanProperties,
    },
    prelude::Expr,
};
use domain_engine_core::{
    transact::{ReqMessage, TransactionMode},
    DomainEngine, DomainError, Session,
};
use futures_util::StreamExt;
use ontol_runtime::{
    ontology::domain::{Def, Domain, Entity},
    DefId, DomainIndex,
};
use table::{mk_datafusion_schema, DatafusionFilter, RecordBatchBuilder};

mod table;

type DfResult<T> = Result<T, DataFusionError>;

pub struct OntologyCatalogProvider {
    engine: Arc<DomainEngine>,
}

impl From<Arc<DomainEngine>> for OntologyCatalogProvider {
    fn from(value: Arc<DomainEngine>) -> Self {
        Self { engine: value }
    }
}

impl CatalogProvider for OntologyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.engine
            .ontology()
            .domains()
            .map(|(_, domain)| self.engine.ontology()[domain.unique_name()].to_string())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let (domain_index, _domain) = self
            .engine
            .ontology()
            .domains()
            .find(|(_, domain)| &self.engine.ontology()[domain.unique_name()] == name)?;

        Some(Arc::new(DomainSchemaProvider {
            engine: self.engine.clone(),
            domain_index,
        }))
    }
}

struct DomainSchemaProvider {
    engine: Arc<DomainEngine>,
    domain_index: DomainIndex,
}

impl DomainSchemaProvider {
    fn domain(&self) -> &Domain {
        self.engine
            .ontology()
            .domain_by_index(self.domain_index)
            .unwrap()
    }

    fn entities(&self) -> impl Iterator<Item = (DefId, &Def, &Entity)> {
        self.domain()
            .defs()
            .filter_map(|def| def.entity().map(|entity| (def.id, def, entity)))
    }

    fn entity_by_name(&self, name: &str) -> Option<(DefId, &Def, &Entity)> {
        self.entities()
            .find(|(.., entity)| &self.engine.ontology()[entity.ident] == name)
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DomainSchemaProvider {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        Some("ontology")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        self.entities()
            .map(|(.., entity)| self.engine.ontology()[entity.ident].to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some((def_id, def, _entity)) = self.entity_by_name(name) else {
            return Ok(None);
        };

        let schema = mk_datafusion_schema(def, self.engine.ontology());

        Ok(Some(Arc::new(EntityTableProvider {
            engine: self.engine.clone(),
            schema: Arc::new(schema),
            def_id,
        })))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Err(exec_error("ontology tables are static"))
    }

    fn deregister_table(&self, _name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Err(exec_error("ontology tables are static"))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.entity_by_name(name).is_some()
    }
}

struct EntityTableProvider {
    engine: Arc<DomainEngine>,
    schema: SchemaRef,
    def_id: DefId,
}

#[async_trait::async_trait]
impl TableProvider for EntityTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an [`ExecutionPlan`] for scanning the table with optionally
    /// specified `projection`, `filter` and `limit`, described below.
    ///
    /// The `ExecutionPlan` is responsible scanning the datasource's
    /// partitions in a streaming, parallelized fashion.
    ///
    /// # Projection
    ///
    /// If specified, only a subset of columns should be returned, in the order
    /// specified. The projection is a set of indexes of the fields in
    /// [`Self::schema`].
    ///
    /// DataFusion provides the projection to scan only the columns actually
    /// used in the query to improve performance, an optimization  called
    /// "Projection Pushdown". Some datasources, such as Parquet, can use this
    /// information to go significantly faster when only a subset of columns is
    /// required.
    ///
    /// # Filters
    ///
    /// A list of boolean filter [`Expr`]s to evaluate *during* the scan, in the
    /// manner specified by [`Self::supports_filters_pushdown`]. Only rows for
    /// which *all* of the `Expr`s evaluate to `true` must be returned (aka the
    /// expressions are `AND`ed together).
    ///
    /// To enable filter pushdown you must override
    /// [`Self::supports_filters_pushdown`] as the default implementation does
    /// not and `filters` will be empty.
    ///
    /// DataFusion pushes filtering into the scans whenever possible
    /// ("Filter Pushdown"), and depending on the format and the
    /// implementation of the format, evaluating the predicate during the scan
    /// can increase performance significantly.
    ///
    /// ## Note: Some columns may appear *only* in Filters
    ///
    /// In certain cases, a query may only use a certain column in a Filter that
    /// has been completely pushed down to the scan. In this case, the
    /// projection will not contain all the columns found in the filter
    /// expressions.
    ///
    /// For example, given the query `SELECT t.a FROM t WHERE t.b > 5`,
    ///
    /// ```text
    /// ┌────────────────────┐
    /// │  Projection(t.a)   │
    /// └────────────────────┘
    ///            ▲
    ///            │
    ///            │
    /// ┌────────────────────┐     Filter     ┌────────────────────┐   Projection    ┌────────────────────┐
    /// │  Filter(t.b > 5)   │────Pushdown──▶ │  Projection(t.a)   │ ───Pushdown───▶ │  Projection(t.a)   │
    /// └────────────────────┘                └────────────────────┘                 └────────────────────┘
    ///            ▲                                     ▲                                      ▲
    ///            │                                     │                                      │
    ///            │                                     │                           ┌────────────────────┐
    /// ┌────────────────────┐                ┌────────────────────┐                 │        Scan        │
    /// │        Scan        │                │        Scan        │                 │  filter=(t.b > 5)  │
    /// └────────────────────┘                │  filter=(t.b > 5)  │                 │  projection=(t.a)  │
    ///                                       └────────────────────┘                 └────────────────────┘
    ///
    /// Initial Plan                  If `TableProviderFilterPushDown`           Projection pushdown notes that
    ///                               returns true, filter pushdown              the scan only needs t.a
    ///                               pushes the filter into the scan
    ///                                                                          BUT internally evaluating the
    ///                                                                          predicate still requires t.b
    /// ```
    ///
    /// # Limit
    ///
    /// If `limit` is specified,  must only produce *at least* this many rows,
    /// (though it may return more).  Like Projection Pushdown and Filter
    /// Pushdown, DataFusion pushes `LIMIT`s  as far down in the plan as
    /// possible, called "Limit Pushdown" as some sources can use this
    /// information to improve their performance. Note that if there are any
    /// Inexact filters pushed down, the LIMIT cannot be pushed down. This is
    /// because inexact filters do not guarantee that every filtered row is
    /// removed, so applying the limit could lead to too few rows being available
    /// to return as a final result.
    async fn scan(
        &self,
        state: &dyn catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let session = state
            .config()
            .get_extension::<Session>()
            .ok_or_else(|| exec_error("domain-engine Session is missing"))?;

        let df_filter = DatafusionFilter::compile(
            self.def_id,
            (projection, filters, limit),
            &self.schema,
            self.engine.ontology(),
        );

        Ok(Arc::new(EntityScan {
            engine: self.engine.clone(),
            df_filter,
            session: Session(session.0.clone()),
            properties: PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Unbounded,
            ),
            children: vec![],
        }))
    }

    /// Specify if DataFusion should provide filter expressions to the
    /// TableProvider to apply *during* the scan.
    ///
    /// Some TableProviders can evaluate filters more efficiently than the
    /// `Filter` operator in DataFusion, for example by using an index.
    ///
    /// # Parameters and Return Value
    ///
    /// The return `Vec` must have one element for each element of the `filters`
    /// argument. The value of each element indicates if the TableProvider can
    /// apply the corresponding filter during the scan. The position in the return
    /// value corresponds to the expression in the `filters` parameter.
    ///
    /// If the length of the resulting `Vec` does not match the `filters` input
    /// an error will be thrown.
    ///
    /// Each element in the resulting `Vec` is one of the following:
    /// * [`Exact`] or [`Inexact`]: The TableProvider can apply the filter
    /// during scan
    /// * [`Unsupported`]: The TableProvider cannot apply the filter during scan
    ///
    /// By default, this function returns [`Unsupported`] for all filters,
    /// meaning no filters will be provided to [`Self::scan`].
    ///
    /// [`Unsupported`]: TableProviderFilterPushDown::Unsupported
    /// [`Exact`]: TableProviderFilterPushDown::Exact
    /// [`Inexact`]: TableProviderFilterPushDown::Inexact
    /// # Example
    ///
    /// ```rust
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// # use arrow_schema::SchemaRef;
    /// # use async_trait::async_trait;
    /// # use datafusion_catalog::{TableProvider, Session};
    /// # use datafusion_common::Result;
    /// # use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
    /// # use datafusion_physical_plan::ExecutionPlan;
    /// // Define a struct that implements the TableProvider trait
    /// struct TestDataSource {}
    ///
    /// #[async_trait]
    /// impl TableProvider for TestDataSource {
    /// # fn as_any(&self) -> &dyn Any { todo!() }
    /// # fn schema(&self) -> SchemaRef { todo!() }
    /// # fn table_type(&self) -> TableType { todo!() }
    /// # async fn scan(&self, s: &dyn Session, p: Option<&Vec<usize>>, f: &[Expr], l: Option<usize>) -> Result<Arc<dyn ExecutionPlan>> {
    ///         todo!()
    /// # }
    ///     // Override the supports_filters_pushdown to evaluate which expressions
    ///     // to accept as pushdown predicates.
    ///     fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
    ///         // Process each filter
    ///         let support: Vec<_> = filters.iter().map(|expr| {
    ///           match expr {
    ///             // This example only supports a between expr with a single column named "c1".
    ///             Expr::Between(between_expr) => {
    ///                 between_expr.expr
    ///                 .try_into_col()
    ///                 .map(|column| {
    ///                     if column.name == "c1" {
    ///                         TableProviderFilterPushDown::Exact
    ///                     } else {
    ///                         TableProviderFilterPushDown::Unsupported
    ///                     }
    ///                 })
    ///                 // If there is no column in the expr set the filter to unsupported.
    ///                 .unwrap_or(TableProviderFilterPushDown::Unsupported)
    ///             }
    ///             _ => {
    ///                 // For all other cases return Unsupported.
    ///                 TableProviderFilterPushDown::Unsupported
    ///             }
    ///         }
    ///     }).collect();
    ///     Ok(support)
    ///     }
    /// }
    /// ```
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

struct EntityScan {
    engine: Arc<DomainEngine>,
    df_filter: DatafusionFilter,
    session: Session,
    properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl DisplayAs for EntityScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EntityScan")
    }
}

impl Debug for EntityScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityScan").finish()
    }
}

impl ExecutionPlan for EntityScan {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            engine: self.engine.clone(),
            df_filter: self.df_filter.clone(),
            session: self.session.clone(),
            properties: self.properties.clone(),
            children,
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let engine = self.engine.clone();
        let mut batch_builder = RecordBatchBuilder::new(
            self.schema(),
            self.df_filter.column_selection(),
            self.engine.ontology_owned(),
        );
        let session = self.session.clone();
        let msg_stream =
            futures_util::stream::iter([Ok(ReqMessage::Query(0, self.df_filter.entity_select()))])
                .boxed();

        let record_batch_stream = async_stream::try_stream! {
            let datastore_stream = engine.transact(
                TransactionMode::ReadOnly,
                msg_stream,
                session,
            )
            .await
            .map_err(domain_error)?;

            for await result in datastore_stream {
                if let Some(batch) = batch_builder.handle_msg(result.map_err(domain_error)?) {
                    yield batch;
                }
            }

            if let Some(batch) = batch_builder.produce_batch() {
                yield batch;
            }
        }
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            record_batch_stream,
        )))
    }
}

fn exec_error(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(msg.into())
}

fn domain_error(err: DomainError) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::{SessionConfig, SessionContext};
    use domain_engine_core::{DomainEngine, Session};
    use domain_engine_test_utils::{
        data_store_util, dynamic_data_store::DynamicDataStoreFactory,
        system::mock_current_time_monotonic, unimock,
    };
    use ontol_examples::artist_and_instrument;
    use ontol_macros::datastore_test;
    use ontol_runtime::ontology::Ontology;
    use ontol_test_utils::{serde_helper::serde_create, TestCompile, TestPackages};
    use serde_json::json;

    use crate::OntologyCatalogProvider;

    #[datastore_test(tokio::test)]
    async fn setup(ds: &str) {
        let test = TestPackages::with_static_sources([artist_and_instrument()]).compile();
        let [artist] = test.bind(["artist"]);
        let engine = make_domain_engine(test.ontology_owned(), ds).await;

        let mut config = SessionConfig::new();
        config.set_extension(Arc::new(Session::default()));
        let ctx = SessionContext::new_with_config(config);
        ctx.register_catalog(
            "ontology",
            Arc::new(OntologyCatalogProvider::from(engine.clone())),
        );

        data_store_util::insert_entity_select_entityid(
            &engine,
            serde_create(&artist)
                .to_value(json!({
                    "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                    "name": "Beach Boys",
                }))
                .unwrap(),
        )
        .await
        .unwrap();

        let dataframe = ctx
            .sql("SELECT \"ID\", name FROM ontology.artist_and_instrument.artist")
            .await
            .unwrap();

        let results = dataframe.collect().await.unwrap();

        let pretty_results = datafusion::arrow::util::pretty::pretty_format_batches(&results)
            .unwrap()
            .to_string();

        let expected = vec![
            "+---------------------------------------------+------------+",
            "| ID                                          | name       |",
            "+---------------------------------------------+------------+",
            "| artist/88832e20-8c6e-46b4-af79-27b19b889a58 | Beach Boys |",
            "+---------------------------------------------+------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
    }

    async fn make_domain_engine(ontology: Arc<Ontology>, datastore: &str) -> Arc<DomainEngine> {
        Arc::new(
            DomainEngine::builder(ontology)
                .system(Box::new(unimock::Unimock::new(
                    mock_current_time_monotonic(),
                )))
                .build(DynamicDataStoreFactory::new(datastore), Session::default())
                .await
                .unwrap(),
        )
    }
}
