use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::{mk_arrow_schema, RecordBatchBuilder};
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
use filter::{ConditionBuilder, DatafusionFilter};
use futures_util::StreamExt;
use ontol_runtime::{
    ontology::domain::{Def, Domain, Entity},
    DefId, DomainIndex,
};

mod arrow;
mod filter;

#[cfg(test)]
mod tests_datastore;

#[cfg(test)]
mod tests_filter;

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

        let arrow_schema = mk_arrow_schema(def, self.engine.ontology());

        Ok(Some(Arc::new(EntityTableProvider {
            engine: self.engine.clone(),
            arrow_schema: Arc::new(arrow_schema),
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
    arrow_schema: SchemaRef,
    def_id: DefId,
}

#[async_trait::async_trait]
impl TableProvider for EntityTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    // FIXME: Need to understand the documentaion on _limit pushdown_:
    //
    // from the trait method documentation:
    // > If `limit` is specified,  must only produce *at least* this many rows,
    // > (though it may return more).  Like Projection Pushdown and Filter
    // > Pushdown, DataFusion pushes `LIMIT`s  as far down in the plan as
    // > possible, called "Limit Pushdown" as some sources can use this
    // > information to improve their performance. Note that if there are any
    // > Inexact filters pushed down, the LIMIT cannot be pushed down. This is
    // > because inexact filters do not guarantee that every filtered row is
    // > removed, so applying the limit could lead to too few rows being available
    // > to return as a final result.
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
            self.engine.ontology(),
        );

        Ok(Arc::new(EntityScan {
            params: Arc::new(EntityScanParams {
                engine: self.engine.clone(),
                df_filter,
                session: Session(session.0.clone()),
                properties: PlanProperties::new(
                    EquivalenceProperties::new(self.arrow_schema.clone()),
                    Partitioning::UnknownPartitioning(1),
                    ExecutionMode::Unbounded,
                ),
            }),
            children: vec![],
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let def = self.engine.ontology().def(self.def_id);
        let mut condition_builder = ConditionBuilder::new(def, self.engine.ontology());

        Ok(filters
            .iter()
            .map(|expr| {
                if condition_builder.try_add_expr(expr).is_ok() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

struct EntityScan {
    params: Arc<EntityScanParams>,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

struct EntityScanParams {
    engine: Arc<DomainEngine>,
    df_filter: DatafusionFilter,
    session: Session,
    properties: PlanProperties,
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
        &self.params.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            params: self.params.clone(),
            children,
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let engine = self.params.engine.clone();
        let mut batch_builder = RecordBatchBuilder::new(
            self.params.df_filter.column_selection(),
            self.schema(),
            engine.ontology_owned(),
            context.session_config().batch_size(),
        );
        let session = self.params.session.clone();
        let msg_stream = futures_util::stream::iter([Ok(ReqMessage::Query(
            0,
            self.params.df_filter.entity_select(),
        ))])
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

            if let Some(batch) = batch_builder.yield_batch() {
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
