use std::sync::Arc;

use ontol_runtime::{
    config::DataStoreConfig,
    env::Env,
    query::{EntityQuery, Query, StructOrUnionQuery},
    value::{Attribute, Value},
    MapKey, PackageId,
};
use tracing::debug;

use crate::{
    data_store::DataStore,
    domain_error::DomainResult,
    in_memory_store::api::InMemoryDb,
    resolve_path::{ProbeOptions, ResolverGraph},
    Config, DomainError,
};

pub struct DomainEngine {
    env: Arc<Env>,
    config: Arc<Config>,
    resolver_graph: ResolverGraph,

    #[allow(unused)]
    data_store: Option<DataStore>,
}

impl DomainEngine {
    pub fn builder(env: Arc<Env>) -> Builder {
        Builder {
            env,
            config: Config::default(),
            data_store: None,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn env(&self) -> &Env {
        &self.env
    }

    fn get_data_store(&self) -> DomainResult<&DataStore> {
        self.data_store.as_ref().ok_or(DomainError::NoDataStore)
    }

    pub async fn query_entities(&self, mut query: EntityQuery) -> DomainResult<Vec<Attribute>> {
        let data_store = self.get_data_store()?;

        let (resolve_path, mut def_id) = match &query.source {
            StructOrUnionQuery::Struct(struct_query) => self
                .resolver_graph
                .probe_path(
                    &self.env,
                    struct_query.def_id,
                    data_store.package_id(),
                    ProbeOptions {
                        must_be_entity: true,
                        inverted: true,
                    },
                )
                .map(|path| (path, struct_query.def_id)),
            StructOrUnionQuery::Union(..) => todo!("Resolve a union"),
        }
        .ok_or(DomainError::NoResolvePathToDataStore)?;

        let outer_def_id = def_id;

        // Transform query
        for next_def_id in &resolve_path.path {
            match &mut query.source {
                StructOrUnionQuery::Struct(struct_query) => {
                    struct_query.def_id = *next_def_id;
                }
                _ => todo!(),
            }

            def_id = *next_def_id;
        }

        debug!(
            "Resolve path: {resolve_path:?} to: {:?}",
            self.env.get_type_info(def_id)
        );

        let mut result = data_store.api().query(self, query).await?;

        if !resolve_path.path.is_empty() {
            // Transform result

            for next_def_id in
                std::iter::once(&outer_def_id).chain(resolve_path.path.iter().rev().skip(1))
            {
                let proc = self
                    .env()
                    .get_mapper_proc(
                        MapKey { def_id, seq: false },
                        MapKey {
                            def_id: *next_def_id,
                            seq: false,
                        },
                    )
                    .expect("No mapping procedure for query output");

                result = result
                    .into_iter()
                    .map(|attribute| {
                        let mapped_value = self.env().new_vm().eval(proc, [attribute.value]);
                        mapped_value.into()
                    })
                    .collect();

                def_id = *next_def_id;
            }
        }

        Ok(result)
    }

    pub async fn store_new_entity(&self, entity: Value, query: Query) -> DomainResult<Value> {
        // TODO: Domain translation by finding optimal mapping path
        self.get_data_store()?
            .api()
            .store_new_entity(self, entity, query)
            .await
    }
}

pub struct Builder {
    env: Arc<Env>,
    config: Config,
    data_store: Option<DataStore>,
}

impl Builder {
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    pub fn data_store(mut self, data_store: DataStore) -> Self {
        self.data_store = Some(data_store);
        self
    }

    pub fn mock_data_store(mut self, package_id: PackageId, setup: impl unimock::Clause) -> Self {
        self.data_store = Some(DataStore::new(
            package_id,
            Box::new(unimock::Unimock::new(setup)),
        ));
        self
    }

    pub fn build(self) -> DomainEngine {
        let data_store = self.data_store.or_else(|| {
            let mut data_store: Option<DataStore> = None;

            for (package_id, _) in self.env.domains() {
                if let Some(config) = self.env.get_package_config(*package_id) {
                    if let Some(DataStoreConfig::InMemory) = config.data_store {
                        data_store = Some(DataStore::new(
                            *package_id,
                            Box::new(InMemoryDb::new(&self.env, *package_id)),
                        ));
                    }
                }
            }

            data_store
        });

        let resolver_graph = ResolverGraph::new(&self.env);

        DomainEngine {
            env: self.env,
            config: Arc::new(self.config),
            resolver_graph,
            data_store,
        }
    }
}
