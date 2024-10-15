use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion_catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion_common::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
    Partitioning, PlanProperties,
};
use domain_engine_arrow::{
    schema::mk_arrow_schema, ArrowConfig, ArrowQuery, ArrowReqMessage, ArrowRespMessage,
};
use domain_engine_core::{DomainEngine, Session};
use filter::{ConditionBuilder, DatafusionFilter};
use futures_util::StreamExt;
use ontol_runtime::{
    ontology::{
        aspects::DefsAspect,
        domain::{Def, Domain, Entity},
    },
    DefId, DomainIndex,
};

pub use domain_engine_arrow::ArrowTransactAPI;
pub use ontol_runtime::ontology::Ontology;

mod filter;

#[cfg(test)]
mod tests_datastore;

#[cfg(test)]
mod tests_filter;

type DfResult<T> = Result<T, DataFusionError>;

pub trait DomainEngineAPI: ArrowTransactAPI {
    fn ontology_defs(&self) -> &DefsAspect;
}

impl DomainEngineAPI for Arc<DomainEngine> {
    fn ontology_defs(&self) -> &DefsAspect {
        self.ontology().as_ref()
    }
}

pub struct OntologyCatalogProvider {
    engine: Arc<dyn DomainEngineAPI + Send + Sync>,
}

impl From<Arc<dyn DomainEngineAPI + Send + Sync>> for OntologyCatalogProvider {
    fn from(value: Arc<dyn DomainEngineAPI + Send + Sync>) -> Self {
        Self { engine: value }
    }
}

impl CatalogProvider for OntologyCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.engine
            .ontology_defs()
            .domains()
            .map(|(_, domain)| self.engine.ontology_defs()[domain.unique_name()].to_string())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let (domain_index, _domain) = self
            .engine
            .ontology_defs()
            .domains()
            .find(|(_, domain)| &self.engine.ontology_defs()[domain.unique_name()] == name)?;

        Some(Arc::new(DomainSchemaProvider {
            engine: self.engine.clone(),
            domain_index,
        }))
    }
}

struct DomainSchemaProvider {
    engine: Arc<dyn DomainEngineAPI + Send + Sync>,
    domain_index: DomainIndex,
}

impl DomainSchemaProvider {
    fn domain(&self) -> &Domain {
        self.engine
            .ontology_defs()
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
            .find(|(.., entity)| &self.engine.ontology_defs()[entity.ident] == name)
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
            .map(|(.., entity)| self.engine.ontology_defs()[entity.ident].to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let Some((def_id, def, _entity)) = self.entity_by_name(name) else {
            return Ok(None);
        };

        let arrow_schema = mk_arrow_schema(def, self.engine.ontology_defs());

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
        Err(error::exec("ontology tables are static"))
    }

    fn deregister_table(&self, _name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        Err(error::exec("ontology tables are static"))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.entity_by_name(name).is_some()
    }
}

struct EntityTableProvider {
    engine: Arc<dyn DomainEngineAPI + Send + Sync>,
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
        state: &dyn datafusion_catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let session = state
            .config()
            .get_extension::<Session>()
            .unwrap_or_else(|| Arc::new(Session::default()));

        let mut arrow_schema = self.arrow_schema.clone();

        if let Some(indices) = &projection {
            arrow_schema = Arc::new(arrow_schema.project(indices)?);
        }

        let df_filter = DatafusionFilter::compile(
            self.def_id,
            (projection, filters, limit),
            self.engine.ontology_defs(),
        );

        Ok(Arc::new(EntityScan {
            params: Arc::new(EntityScanParams {
                engine: self.engine.clone(),
                df_filter,
                session: Session(session.0.clone()),
                properties: PlanProperties::new(
                    EquivalenceProperties::new(arrow_schema),
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
        let def = self.engine.ontology_defs().def(self.def_id);
        let mut condition_builder = ConditionBuilder::new(def, self.engine.ontology_defs());

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
    engine: Arc<dyn DomainEngineAPI + Send + Sync>,
    df_filter: DatafusionFilter,
    session: Session,
    properties: PlanProperties,
}

impl DisplayAs for EntityScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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

    fn properties(&self) -> &PlanProperties {
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
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let record_batch_stream = self
            .params
            .engine
            .arrow_transact(
                ArrowReqMessage::Query(ArrowQuery {
                    entity_select: self.params.df_filter.entity_select(),
                    column_selection: self.params.df_filter.column_selection(),
                    projection: self.params.df_filter.projection(),
                    schema: self.schema(),
                }),
                ArrowConfig {
                    batch_size: context.session_config().batch_size(),
                },
                self.params.session.clone(),
            )
            .filter_map(|resp_message| async move {
                match resp_message {
                    Ok(ArrowRespMessage::RecordBatch(batch)) => Some(Ok(batch)),
                    Err(err) => Some(Err(error::domain_to_datafusion(err))),
                }
            });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            record_batch_stream,
        )))
    }
}

pub mod error {
    use datafusion_common::DataFusionError;
    use domain_engine_core::{domain_error::DomainErrorKind, DomainError};

    pub fn domain_to_datafusion(err: DomainError) -> DataFusionError {
        DataFusionError::External(Box::new(err))
    }

    pub fn datafusion_to_domain(err: DataFusionError) -> DomainError {
        match err {
            DataFusionError::Plan(msg) => DomainErrorKind::BadInputFormat(msg).into_error(),
            DataFusionError::SQL(parser_error, _) => {
                DomainErrorKind::BadInputFormat(format!("{parser_error}")).into_error()
            }
            DataFusionError::External(generic_err) => match generic_err.downcast::<DomainError>() {
                Ok(domain_error) => *domain_error,
                Err(err) => DomainErrorKind::Interface(format!("{err:?}")).into_error(),
            },
            other => DomainErrorKind::Interface(format!("{other:?}")).into_error(),
        }
    }

    pub fn exec(msg: impl Into<String>) -> DataFusionError {
        DataFusionError::Execution(msg.into())
    }
}
