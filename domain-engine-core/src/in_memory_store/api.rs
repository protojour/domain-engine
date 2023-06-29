use fnv::FnvHashMap;
use ontol_runtime::{
    env::Env,
    query::EntityQuery,
    serde::operator::{SerdeOperator, SerdeOperatorId, ValueOperator},
    string_types::StringLikeType,
    value::{Attribute, Value},
    DefId, PackageId, RelationshipId,
};
use tokio::sync::RwLock;

use crate::{
    data_store::DataStoreAPI, entity_id_utils::analyze_string_pattern, DomainEngine, DomainResult,
};

use super::store::{EdgeCollection, EntityCollection, InMemoryStore};

#[derive(Debug)]
pub struct InMemoryDb {
    #[allow(unused)]
    package_id: PackageId,
    store: RwLock<InMemoryStore>,
}

#[async_trait::async_trait]
impl DataStoreAPI for InMemoryDb {
    async fn query(
        &self,
        engine: &DomainEngine,
        query: EntityQuery,
    ) -> DomainResult<Vec<Attribute>> {
        Ok(self
            .store
            .read()
            .await
            .query_entities(engine, &query)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn store_entity(&self, engine: &DomainEngine, entity: Value) -> DomainResult<Value> {
        self.store.write().await.write_entity(engine, entity)
    }
}

impl InMemoryDb {
    pub fn new(env: &Env, package_id: PackageId) -> Self {
        let domain = env.find_domain(package_id).unwrap();

        let mut collections: FnvHashMap<DefId, EntityCollection> = Default::default();
        let mut edge_collections: FnvHashMap<RelationshipId, EdgeCollection> = Default::default();

        for type_info in domain.type_infos() {
            if let Some(entity_info) = &type_info.entity_info {
                collections.insert(
                    type_info.def_id,
                    Self::collection_from_id_operator(env, entity_info.id_operator_id),
                );

                for (property_id, _entity_relationship) in &entity_info.entity_relationships {
                    let relationship_id = property_id.relationship_id;
                    edge_collections
                        .entry(relationship_id)
                        .or_insert_with(|| EdgeCollection { edges: vec![] });
                }
            }
        }

        Self {
            package_id,
            store: RwLock::new(InMemoryStore {
                collections,
                edge_collections,
                int_id_counter: 0,
            }),
        }
    }

    fn collection_from_id_operator(env: &Env, id_operator_id: SerdeOperatorId) -> EntityCollection {
        match env.get_serde_operator(id_operator_id) {
            SerdeOperator::String(_) => EntityCollection::String(Default::default()),
            SerdeOperator::Int(_) => EntityCollection::Int(Default::default()),
            SerdeOperator::StringPattern(def_id) => match env.get_string_like_type(*def_id) {
                Some(StringLikeType::Uuid) => EntityCollection::Uuid(Default::default()),
                _ => panic!("string pattern unsuitable for id"),
            },
            SerdeOperator::ValueType(ValueOperator {
                inner_operator_id, ..
            }) => Self::collection_from_id_operator(env, *inner_operator_id),
            SerdeOperator::CapturingStringPattern(def_id) => {
                if let Some(property) =
                    analyze_string_pattern(env.get_string_pattern(*def_id).unwrap())
                {
                    let type_info = env.get_type_info(property.type_def_id);
                    Self::collection_from_id_operator(env, type_info.operator_id.unwrap())
                } else {
                    panic!("String pattern without any properties")
                }
            }
            operator => panic!("unrecognized operator {operator:?}"),
        }
    }
}
