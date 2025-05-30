use anyhow::{Context, anyhow};
use deunicode::AsciiChars;
use domain_engine_core::system::ArcSystemApi;
use ontol_runtime::{
    DefId, DomainIndex,
    interface::serde::{
        operator::SerdeOperatorAddr,
        processor::{ProcessorProfile, ProcessorProfileFlags, ScalarFormat},
    },
    ontology::{
        Ontology,
        domain::{DataRelationshipKind, Def, DefKind, EdgeInfo},
        ontol::TextConstant,
    },
};
use reqwest::Url;
use reqwest_middleware::ClientWithMiddleware;
use serde::{
    Deserialize,
    de::{DeserializeOwned, DeserializeSeed},
};
use serde_json::{Value, json};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::{
    collections::{BTreeSet, HashMap},
    fmt::{Display, Formatter},
    sync::Arc,
};
use tracing::{debug, info, warn};

use crate::aql::Ident;

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
    pub collections: HashMap<DefId, Ident>,
    /// Edge collections by Edge id
    pub edge_collections: HashMap<DefId, EdgeCollection>,
    /// Reverse lookup DefId for ProcessorProfileApi
    pub collection_lookup: HashMap<String, DefId>,
    pub ontology: Arc<Ontology>,
    pub system: ArcSystemApi,
}

pub struct ArangoDatabaseHandle {
    pub(crate) database: Arc<ArangoDatabase>,
}

impl From<ArangoDatabase> for ArangoDatabaseHandle {
    fn from(value: ArangoDatabase) -> Self {
        Self {
            database: Arc::new(value),
        }
    }
}

pub struct EdgeCollection {
    pub name: Ident,

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
pub struct Extra {
    /// Statistics
    pub stats: Stats,
}

/// Stats
#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "camelCase"))]
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
/// FIXME: There is no namespacing going on in ArangoDB (yet).
fn get_collection_name(def: &Def, ontology: &Ontology) -> Ident {
    let text_constant = match def.store_key {
        Some(store_key) => store_key,
        None => def.ident().expect("type should have an identifier"),
    };
    let mut name = ontology[text_constant].to_ascii_lossy().replace("[?]", "_");
    name.truncate(256);
    Ident::new(name)
}

impl ArangoDatabase {
    /// Initialize collections and database from ontology
    pub async fn init(
        &mut self,
        persisted: &BTreeSet<DomainIndex>,
        provision_db: bool,
    ) -> anyhow::Result<()> {
        if self.collections.is_empty() {
            for domain_index in persisted {
                self.populate_collections(*domain_index)?;
            }
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
            if !self.has_collection(collection.raw_str()).await? {
                self.create_collection(collection.raw_str(), CollectionKind::Collection)
                    .await?;
            }
        }

        for edge_collection in self.edge_collections.values() {
            if !self.has_collection(edge_collection.name.raw_str()).await? {
                self.create_collection(
                    edge_collection.name.raw_str(),
                    CollectionKind::EdgeCollection,
                )
                .await?;
            }
        }

        for (def_id, collection) in &self.collections {
            self.collection_lookup
                .insert(collection.raw_str().to_string(), *def_id);
        }

        // doesn't currently make sense for edge collections
        // for (edge_id, collection) in &self.edge_collections {
        //     self.collection_lookup
        //         .insert(collection.name.to_string(), edge_id.0);
        // }

        Ok(())
    }

    /// Populate collection and edge collection names
    pub fn populate_collections(&mut self, domain_index: DomainIndex) -> anyhow::Result<()> {
        let domain = self
            .ontology
            .domain_by_index(domain_index)
            .expect("domain index should match a domain");

        let mut subject_names_by_edge_id: HashMap<DefId, TextConstant> = Default::default();
        let mut collection_name_collisions: BTreeSet<String> = Default::default();

        // collections
        // TODO: handle rare collisions after ASCII/length coercion
        for def in domain.defs() {
            let def_id = def.id;

            match &def.kind {
                DefKind::Entity(_entity) => {
                    let collection = get_collection_name(def, &self.ontology);
                    if let Some(prev_collection) =
                        self.collections.insert(def.id, collection.clone())
                    {
                        collection_name_collisions.insert(prev_collection.raw_str().to_string());
                    }

                    for data_relationship in def.data_relationships.values() {
                        if let DataRelationshipKind::Edge(projection) = data_relationship.kind {
                            if projection.subject.0 == 0 {
                                subject_names_by_edge_id
                                    .insert(projection.edge_id, data_relationship.name);
                            }
                        }
                    }
                }
                DefKind::Edge(edge_info) => {
                    let name = self.find_edge_collection_name(
                        def.id,
                        edge_info,
                        &subject_names_by_edge_id,
                    )?;
                    debug!("register edge collection {name}: edge_id={def_id:?}");

                    let rel_params = if edge_info.cardinals.len() > 2 {
                        // FIXME: This could be a union
                        edge_info.cardinals[2].target.iter().copied().next()
                    } else {
                        None
                    };

                    self.edge_collections
                        .insert(def_id, EdgeCollection { name, rel_params });
                }
                _ => {}
            }
        }

        // edges
        for (edge_id, edge_info) in domain.edges() {
            let name =
                self.find_edge_collection_name(*edge_id, edge_info, &subject_names_by_edge_id)?;
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

        if !collection_name_collisions.is_empty() {
            return Err(anyhow!(
                "collection name collisions: {collection_name_collisions:?}"
            ));
        }

        Ok(())
    }

    fn find_edge_collection_name(
        &self,
        edge_id: DefId,
        edge_info: &EdgeInfo,
        subject_names_by_edge_id: &HashMap<DefId, TextConstant>,
    ) -> anyhow::Result<Ident> {
        if let Some(store_key) = edge_info.store_key {
            return Ok(Ident::new(self.ontology[store_key].to_string()));
        }

        if let Some(ident) = subject_names_by_edge_id.get(&edge_id).copied() {
            let mut ident = self.ontology[ident]
                .to_ascii_lossy()
                .replace("[?]", "_")
                .to_string();

            for other_edge_collection in self.edge_collections.values() {
                if ident == other_edge_collection.name.raw_str() {
                    // return Err(anyhow!("duplicate edge name: {name}"));

                    if let Some(type_disambiguation) = edge_info.cardinals[0].target.iter().next() {
                        let collection =
                            self.collections.get(type_disambiguation).ok_or_else(|| {
                                anyhow!(
                                    "cannot disambiguate for `{ident}` {edge_id:?}: {type_disambiguation:?}"
                                )
                            })?;

                        ident = format!("{prefix}_{ident}", prefix = collection.raw_str());
                    }
                }
            }

            return Ok(Ident::new(ident));
        }

        // find non-union type in the cardinals
        for cardinal in &edge_info.cardinals {
            if cardinal.target.len() == 1 {
                let def = self.ontology.def(*cardinal.target.iter().next().unwrap());

                if let Some(ident) = def.ident() {
                    let mut concat = self.ontology[ident].to_string();

                    for (_, rel_info, projection) in def.edge_relationships() {
                        if projection.edge_id == edge_id {
                            concat.push('_');
                            concat.push_str(&self.ontology[rel_info.name]);
                        }
                    }

                    return Ok(Ident::new(concat));
                }
            }
        }

        Err(anyhow!("edge collection name not found for {edge_id:?}"))
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
    #[expect(clippy::too_many_arguments)]
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
