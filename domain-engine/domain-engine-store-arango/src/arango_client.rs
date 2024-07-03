use anyhow::{anyhow, Context};
use deunicode::AsciiChars;
use domain_engine_core::system::ArcSystemApi;
use ontol_runtime::{
    interface::serde::{
        operator::SerdeOperatorAddr,
        processor::{ProcessorProfile, ProcessorProfileFlags, ScalarFormat},
    },
    ontology::{
        domain::{DataRelationshipKind, Def},
        ontol::TextConstant,
        Ontology,
    },
    DefId, EdgeId, PackageId,
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde::{
    de::{DeserializeOwned, DeserializeSeed},
    Deserialize,
};
use serde_json::{json, Value};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::Arc,
};
use tracing::{debug, info, warn};

use super::aql::Query;

// TODO: typed parameters and integrated format coercion
// e.g. DbName, CollectionName...

/// An ArangoDB client
pub struct ArangoClient {
    /// URL for ArangoDB host
    pub host: String,
    /// Reqwest client
    pub http: ClientWithMiddleware,
}

/// An ArangoDB client for a specific database
pub struct ArangoDatabase {
    /// ArangoDB client
    pub client: ArangoClient,
    /// Database name
    pub db_name: String,
    /// Collections by DefId
    pub collections: HashMap<DefId, String>,
    /// Edge collections by Edge id
    pub edge_collections: HashMap<EdgeId, EdgeCollection>,
    /// Reverse lookup DefId for ProcessorProfileApi
    pub collection_lookup: HashMap<String, DefId>,
    pub ontology: Arc<Ontology>,
    pub system: ArcSystemApi,
}

pub struct EdgeCollection {
    pub name: String,

    pub rel_params: Option<DefId>,
}

/// An ArangoDB AQL query
#[derive(Clone, Debug)]
pub struct AqlQuery {
    /// AQL query AST data
    pub query: Query,
    /// Bind vars
    pub bind_vars: HashMap<String, Value>,
    /// Response deserialization operator address
    pub operator_addr: SerdeOperatorAddr,
}

/// HTTP response enum for most ArangoDB queries
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ArangoResponse<T> {
    Result(ArangoResultResponse<T>),
    Error(ArangoErrorResponse),
}

/// HTTP response enum for AQL query endpoint
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ArangoAQLResponse<T> {
    Result(ArangoCursorResponse<T>),
    Error(ArangoErrorResponse),
}

/// Standard HTTP response structure for most ArangoDB queries
#[derive(Debug, Deserialize)]
struct ArangoResultResponse<T> {
    /// Result(s) can be array or object
    result: T,
}

/// Standard HTTP response structure for most ArangoDB errors
#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
struct ArangoErrorResponse {
    /// HTTP status code
    code: u16,
    /// Server error code
    error_num: u16,
    /// A descriptive error message
    error_message: String,
}

/// Standard HTTP response structure for AQL query endpoint
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct ArangoCursorResponse<T> {
    /// Array of result documents
    pub result: T,
    /// Total number of results
    pub count: Option<usize>,
    /// ID of a temporary cursor for batched results
    pub id: Option<String>,
    /// `true` if the cursor has more results
    pub has_more: bool,
    /// Object with extra details, see ArangoDB docs
    pub extra: Option<Extra>,
}

/// Extra details structure
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Extra {
    /// Statistics
    pub stats: Stats,
}

/// Stats
#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
#[allow(dead_code)]
pub struct Stats {
    /// Number of documents in the result before the last top-level LIMIT
    pub full_count: Option<usize>,
}

/// Bulk response data
#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct BulkData<T> {
    pub _id: String,
    pub _key: String,
    pub _rev: String,
    /// Old value, if old = true
    #[serde(default)]
    pub old: Option<T>,
    /// New value, if new = true
    #[serde(default)]
    pub new: Option<T>,
}

/// Collection data
#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
pub struct Collection {
    /// Numeric ID of collection
    pub id: String,
    /// Name of collection
    pub name: String,
    /// Status of collection
    pub status: CollectionStatus,
    /// Type of collection
    #[serde(rename(deserialize = "type"))]
    pub kind: CollectionKind,
    /// `true` if this is a system collection
    pub is_system: bool,
    /// Globally unique ID of collection
    pub globally_unique_id: String,
}

/// Collection status
#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum CollectionStatus {
    Loaded = 2,
    Deleted = 3,
}

/// Type of collection
#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum CollectionKind {
    Collection = 2,
    EdgeCollection = 3,
}

/// Overwrite mode for bulk operations
pub enum OverwriteMode {
    /// If a document with the specified `_key` value exists already,
    /// nothing is done and no write operation is carried out. Not
    /// compatible with `old`.
    Ignore,
    /// If a document with the specified `_key` value exists already,
    /// it is overwritten with the specified document value.
    Replace,
    /// If a document with the specified `_key` value exists already,
    /// it is patched with the specified document value.
    Update,
    /// If a document with the specified `_key` value exists already,
    /// return a unique constraint violation error so that the insert
    /// operation fails.
    Conflict,
}

impl ArangoClient {
    /// Initialize a new ArangoClient
    pub fn new(host: &str, client: ClientWithMiddleware) -> Self {
        // TODO: support multiple hosts
        ArangoClient {
            host: host.to_string(),
            http: client,
        }
    }

    /// Get an instance of a named ArangoDatabase
    pub fn db(self, name: &str, ontology: Arc<Ontology>, system: ArcSystemApi) -> ArangoDatabase {
        ArangoDatabase {
            client: self,
            db_name: name.to_string(),
            collections: HashMap::new(),
            edge_collections: HashMap::new(),
            collection_lookup: HashMap::new(),
            ontology,
            system,
        }
    }

    /// Ping ArangoDB by checking the _system database
    pub async fn ping(&self) -> anyhow::Result<()> {
        let url = format!("{}/_db/_system/_api/collection", self.host);
        self.http.get(url).send().await?;
        Ok(())
    }

    /// Initialize database for given name
    pub async fn init(&self, name: &str) -> anyhow::Result<()> {
        if !self.has_database(name).await.context("has_database")? {
            self.create_database(name)
                .await
                .context("create_database")?;
        }
        Ok(())
    }

    /// Check if database by given name exists
    pub async fn has_database(&self, name: &str) -> anyhow::Result<bool> {
        let url = format!("{}/_db/_system/_api/database", self.host);
        let resp = self
            .http
            .get(url)
            .send()
            .await?
            .json::<ArangoResponse<Vec<String>>>()
            .await?;
        match resp {
            ArangoResponse::Result(result) => Ok(result.result.contains(&name.to_string())),
            ArangoResponse::Error(err) => Err(err.into()),
        }
    }

    /// Create a database with given name
    pub async fn create_database(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/_db/_system/_api/database", self.host);
        let body = json!({ "name": name });
        info!("Creating database {name}.");
        self.http.post(url).body(body.to_string()).send().await?;
        Ok(())
    }

    /// Drop the database with given name
    pub async fn drop_database(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/_db/_system/_api/database/{name}", self.host);
        warn!("DROPPING database {name}!");
        self.http.delete(url).send().await?;
        Ok(())
    }
}

/// Get a canonical ArangoDB collection name from ONTOL def
fn get_collection_name(def: &Def, ontology: &Ontology) -> String {
    let text_constant = match def.store_key {
        Some(store_key) => store_key,
        None => def.name().expect("type should have a name"),
    };
    let mut name = ontology[text_constant]
        .to_ascii_lossy()
        .replace("[?]", "_")
        .to_string();
    name.truncate(256);
    name
}

impl ArangoDatabase {
    /// Initialize collections and database from ontology
    pub async fn init(&mut self, package_id: PackageId, provision_db: bool) -> anyhow::Result<()> {
        if self.collections.is_empty() {
            self.populate_collections(package_id)?;
        }

        if !provision_db {
            return Ok(());
        }

        // ensure database exists
        self.client
            .init(&self.db_name)
            .await
            .context("client init")?;

        for collection in self.collections.values() {
            if !self.has_collection(collection.as_str()).await? {
                self.create_collection(collection.as_str(), CollectionKind::Collection)
                    .await?;
            }
        }

        for edge_collection in self.edge_collections.values() {
            if !self.has_collection(edge_collection.name.as_str()).await? {
                self.create_collection(
                    edge_collection.name.as_str(),
                    CollectionKind::EdgeCollection,
                )
                .await?;
            }
        }

        for (def_id, collection) in &self.collections {
            self.collection_lookup
                .insert(collection.to_string(), *def_id);
        }
        for (edge_id, collection) in &self.edge_collections {
            self.collection_lookup
                .insert(collection.name.to_string(), edge_id.0);
        }

        Ok(())
    }

    /// Populate collection and edge collection names
    pub fn populate_collections(&mut self, package_id: PackageId) -> anyhow::Result<()> {
        let domain = self
            .ontology
            .find_domain(package_id)
            .expect("package id should match a domain");

        let mut subject_names_by_edge_id: HashMap<EdgeId, TextConstant> = Default::default();

        // collections
        // TODO: handle rare collisions after ASCII/length coercion
        for def in domain.defs() {
            if def.entity().is_none() {
                continue;
            }

            let collection = get_collection_name(def, &self.ontology);
            self.collections.insert(def.id, collection.clone());

            for data_relationship in def.data_relationships.values() {
                if let DataRelationshipKind::Edge(projection) = data_relationship.kind {
                    if projection.subject.0 == 0 {
                        subject_names_by_edge_id.insert(projection.id, data_relationship.name);
                    }
                }
            }
        }

        // edges
        for (edge_id, edge_info) in domain.edges() {
            let name = if let Some(store_key) = edge_info.store_key {
                self.ontology[store_key].to_string()
            } else {
                let name = subject_names_by_edge_id
                    .get(edge_id)
                    .copied()
                    .ok_or_else(|| anyhow!("edge subject name not found for {edge_id:?}"))?;
                let mut name = self.ontology[name]
                    .to_ascii_lossy()
                    .replace("[?]", "_")
                    .to_string();

                for other_edge_collection in self.edge_collections.values_mut() {
                    if name == other_edge_collection.name {
                        // return Err(anyhow!("duplicate edge name: {name}"));

                        if let Some(type_disambiguation) =
                            edge_info.cardinals[0].target.iter().next()
                        {
                            let prefix =
                            self.collections.get(&type_disambiguation).ok_or_else(|| {
                                anyhow!("cannot disambiguate for `{name}` {edge_id:?}: {type_disambiguation:?}")
                            })?;

                            name = format!("{prefix}_{name}");
                        }
                    }
                }

                name
            };

            debug!("register edge collection {name}: edge_id={edge_id:?}");

            let rel_params = if edge_info.cardinals.len() > 2 {
                // FIXME: This could be a union
                edge_info.cardinals[2].target.iter().copied().next()
            } else {
                None
            };

            self.edge_collections
                .insert(*edge_id, EdgeCollection { name, rel_params });
        }

        Ok(())
    }

    pub fn profile(&self) -> ProcessorProfile {
        ProcessorProfile {
            id_format: ScalarFormat::RawText,
            flags: ProcessorProfileFlags::all(),
            api: self,
        }
    }

    /// Check if collection exists
    pub async fn has_collection(&self, name: &str) -> anyhow::Result<bool> {
        let url = format!("{}/_db/{}/_api/collection", self.client.host, self.db_name);
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<ArangoResponse<Vec<Collection>>>()
            .await?;
        match resp {
            ArangoResponse::Result(result) => Ok(result.result.iter().any(|c| c.name == name)),
            ArangoResponse::Error(err) => Err(err.into()),
        }
    }

    /// Create a collection
    pub async fn create_collection(&self, name: &str, kind: CollectionKind) -> anyhow::Result<()> {
        let url = format!("{}/_db/{}/_api/collection", self.client.host, self.db_name);
        let body = json!({
            "name": name,
            "type": kind,
        });
        info!(
            "Creating {}collection {name}...",
            match kind {
                CollectionKind::Collection => "",
                CollectionKind::EdgeCollection => "edge ",
            }
        );
        self.client
            .http
            .post(url)
            .body(body.to_string())
            .send()
            .await?;
        Ok(())
    }

    /// List collections
    pub async fn list_collections(&self) -> anyhow::Result<Vec<Collection>> {
        let url = format!("{}/_db/{}/_api/collection", self.client.host, self.db_name);
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<ArangoResponse<Vec<Collection>>>()
            .await?;
        match resp {
            ArangoResponse::Result(result) => Ok(result.result),
            ArangoResponse::Error(err) => Err(err.into()),
        }
    }

    /// Drop a collection
    pub async fn drop_collection(&self, name: &str) -> anyhow::Result<()> {
        let url = format!(
            "{}/_db/{}/_api/collection/{name}",
            self.client.host, self.db_name
        );
        warn!("DROPPING collection {name}!");
        self.client.http.delete(url).send().await?;
        Ok(())
    }

    /// Truncate a collection
    pub async fn truncate_collection(&self, name: &str) -> anyhow::Result<()> {
        let url = format!(
            "{}/_db/{}/_api/collection/{name}/truncate",
            self.client.host, self.db_name
        );
        warn!("TRUNCATING collection {name}!");
        self.client.http.put(url).send().await?;
        Ok(())
    }

    /// Create an index
    pub async fn create_index(&self, collection: &str, data: &Value) -> anyhow::Result<()> {
        let url = format!(
            "{}/_db/{}/_api/index/{collection}",
            self.client.host, self.db_name
        );
        self.client
            .http
            .post(url)
            .body(data.to_string())
            .send()
            .await?;
        Ok(())
    }

    /// List indexes
    pub async fn list_indexes(&self, collection: &str) -> anyhow::Result<Value> {
        // TODO: deserialize to something useful
        let url = format!(
            "{}/_db/{}/_api/index/{collection}",
            self.client.host, self.db_name
        );
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// Read an index
    pub async fn read_index(&self, collection: &str, id: &str) -> anyhow::Result<Value> {
        // TODO: deserialize to something useful
        let url = format!(
            "{}/_db/{}/_api/index/{collection}/{id}",
            self.client.host, self.db_name
        );
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// Delete an index
    pub async fn delete_index(&self, collection: &str, id: &str) -> anyhow::Result<()> {
        let url = format!(
            "{}/_db/{}/_api/index/{collection}/{id}",
            self.client.host, self.db_name
        );
        self.client.http.delete(url).send().await?;
        Ok(())
    }

    /// Create a view
    pub async fn create_view(&self, data: &Value) -> anyhow::Result<()> {
        let url = format!("{}/_db/{}/_api/view", self.client.host, self.db_name);
        self.client
            .http
            .post(url)
            .body(data.to_string())
            .send()
            .await?;
        Ok(())
    }

    /// List views
    pub async fn list_views(&self) -> anyhow::Result<Vec<Value>> {
        // TODO: deserialize to something useful
        let url = format!("{}/_db/{}/_api/view", self.client.host, self.db_name);
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<ArangoResponse<Value>>()
            .await?
            .into_vec_value()?;
        Ok(resp)
    }

    /// Read properties of a view
    pub async fn read_view(&self, name: &str) -> anyhow::Result<Value> {
        // TODO: deserialize to something useful
        let url = format!(
            "{}/_db/{}/_api/view/{name}/properties",
            self.client.host, self.db_name
        );
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// Update a view
    pub async fn update_view(&self, name: &str, data: &Value) -> anyhow::Result<()> {
        let url = format!(
            "{}/_db/{}/_api/view/{name}/properties",
            self.client.host, self.db_name
        );
        self.client
            .http
            .patch(url)
            .body(data.to_string())
            .send()
            .await?;
        Ok(())
    }

    /// Delete a view
    pub async fn delete_view(&self, name: &str) -> anyhow::Result<()> {
        let url = format!("{}/_db/{}/_api/view/{name}", self.client.host, self.db_name);
        self.client.http.get(url).send().await?;
        Ok(())
    }

    /// Create an analyzer
    pub async fn create_analyzer(&self, data: &Value) -> anyhow::Result<()> {
        let url = format!("{}/_db/{}/_api/analyzer", self.client.host, self.db_name);
        self.client
            .http
            .post(url)
            .body(data.to_string())
            .send()
            .await?
            .json::<ArangoResponse<Value>>()
            .await?;
        Ok(())
    }

    /// List analyzers
    pub async fn list_analyzers(&self) -> anyhow::Result<Vec<Value>> {
        let url = format!("{}/_db/{}/_api/analyzer", self.client.host, self.db_name);
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<ArangoResponse<Value>>()
            .await?
            .into_vec_value()?;
        Ok(resp)
    }

    /// Read definition of an analyzer
    pub async fn read_analyzer(&self, name: &str) -> anyhow::Result<Value> {
        let url = format!(
            "{}/_db/{}/_api/analyzer/{name}",
            self.client.host, self.db_name
        );
        let resp = self
            .client
            .http
            .get(url)
            .send()
            .await?
            .json::<Value>()
            .await?;
        Ok(resp)
    }

    /// Delete an analyzer
    pub async fn delete_analyzer(&self, name: &str) -> anyhow::Result<()> {
        let url = format!(
            "{}/_db/{}/_api/analyzer/{name}",
            self.client.host, self.db_name
        );
        self.client.http.get(url).send().await?;
        Ok(())
    }

    /// Execute an AQL query
    pub async fn aql<D: DeserializeSeed<'static> + Send + Clone>(
        &self,
        query: AqlQuery,
        count: bool,
        total: bool,
        trx_id: Option<&str>,
        seed: D,
    ) -> anyhow::Result<ArangoCursorResponse<Vec<D::Value>>> {
        let url = format!("{}/_db/{}/_api/cursor", self.client.host, self.db_name);
        let body = json!({
            "query": query.query.to_string(),
            "bindVars": query.bind_vars,
            "count": count,
            "options": {
                "fullCount": total,
            }
        });
        let mut req = self.client.http.post(url);
        if let Some(id) = trx_id {
            req = req.header("x-arango-trx-id", id)
        }
        let resp = req
            .body(body.to_string())
            .send()
            .await?
            .json::<ArangoAQLResponse<Vec<Value>>>()
            .await?;

        // debug
        match resp {
            ArangoAQLResponse::Result(ref res) => {
                debug!("http {:#?}", res);
            }
            ArangoAQLResponse::Error(ref err) => {
                debug!("http ERROR {err:#?}");
            }
        };

        let mut results: Vec<D::Value> = vec![];
        match resp {
            ArangoAQLResponse::Result(result) => {
                for res in result.result {
                    results.push(seed.clone().deserialize(res)?);
                }

                Ok(ArangoCursorResponse {
                    result: results,
                    count: result.count,
                    id: result.id,
                    has_more: result.has_more,
                    extra: result.extra,
                })
            }
            ArangoAQLResponse::Error(err) => Err(err.into()),
        }
    }

    /// Bulk create multiple documents
    pub async fn bulk_create<T: DeserializeOwned + Default>(
        &self,
        collection: &str,
        data: &Value,
        sync: bool,
        new: bool,
        mode: OverwriteMode,
        trx_id: Option<&str>,
    ) -> anyhow::Result<Vec<BulkData<T>>> {
        let params = [
            ("waitForSync", sync.to_string()),
            ("returnNew", new.to_string()),
            ("overwriteMode", mode.to_string()),
        ];
        let url = format!(
            "{}/_db/{}/_api/document/{collection}",
            self.client.host, self.db_name
        );
        let url_param = Url::parse_with_params(&url, &params)?;
        let mut req = self.client.http.post(url_param);
        if let Some(id) = trx_id {
            req = req.header("x-arango-trx-id", id)
        }
        let resp = req
            .body(data.to_string())
            .send()
            .await?
            .json::<Vec<BulkData<T>>>()
            .await?;
        Ok(resp)
    }

    /// Bulk update multiple documents
    #[allow(clippy::too_many_arguments)]
    pub async fn bulk_update<T: DeserializeOwned + Default>(
        &self,
        collection: &str,
        data: &Value,
        sync: bool,
        new: bool,
        old: bool,
        keep_null: bool,
        merge: bool,
        trx_id: Option<&str>,
    ) -> anyhow::Result<Vec<BulkData<T>>> {
        let params = [
            ("waitForSync", sync.to_string()),
            ("returnNew", new.to_string()),
            ("returnOld", old.to_string()),
            ("keepNull", keep_null.to_string()),
            ("mergeObjects", merge.to_string()),
        ];
        let url = format!(
            "{}/_db/{}/_api/document/{collection}",
            self.client.host, self.db_name
        );
        let url_param = Url::parse_with_params(&url, &params)?;
        let mut req = self.client.http.patch(url_param);
        if let Some(id) = trx_id {
            req = req.header("x-arango-trx-id", id)
        }
        let resp = req
            .body(data.to_string())
            .send()
            .await?
            .json::<Vec<BulkData<T>>>()
            .await?;
        Ok(resp)
    }

    /// Bulk delete multiple documents
    pub async fn bulk_delete<T: DeserializeOwned + Default>(
        &self,
        collection: &str,
        data: &Value,
        sync: bool,
        old: bool,
        trx_id: Option<&str>,
    ) -> anyhow::Result<Vec<BulkData<T>>> {
        let params = [
            ("waitForSync", sync.to_string()),
            ("returnOld", old.to_string()),
        ];
        let url = format!(
            "{}/_db/{}/_api/document/{collection}",
            self.client.host, self.db_name
        );
        let url_param = Url::parse_with_params(&url, &params)?;
        let mut req = self.client.http.delete(url_param);
        if let Some(id) = trx_id {
            req = req.header("x-arango-trx-id", id)
        }
        let resp = req
            .body(data.to_string())
            .send()
            .await?
            .json::<Vec<BulkData<T>>>()
            .await?;
        Ok(resp)
    }
}

impl ArangoResponse<Value> {
    fn into_vec_value(self) -> anyhow::Result<Vec<Value>> {
        match self {
            ArangoResponse::Result(result) => {
                let results: Vec<Value> = serde_json::from_value(result.result)?;
                Ok(results)
            }
            ArangoResponse::Error(err) => Err(err.into()),
        }
    }
}

impl Display for ArangoErrorResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [{}]: {}",
            self.code, self.error_num, self.error_message
        )
    }
}

impl std::error::Error for ArangoErrorResponse {}

impl Display for OverwriteMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OverwriteMode::Ignore => write!(f, "ignore"),
            OverwriteMode::Replace => write!(f, "replace"),
            OverwriteMode::Update => write!(f, "update"),
            OverwriteMode::Conflict => write!(f, "conflict"),
        }
    }
}
