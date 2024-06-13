use crate::juniper;

use ontol_runtime::{
    ontology::domain::{self, EdgeCardinalProjection},
    property, DefId, EdgeId, RelationshipId,
};

use super::{gql_domain, Ctx};

pub struct DataRelationshipInfo {
    pub def_id: DefId,
    pub kind: RelationshipKindEnum,
    pub rel_id: RelationshipId,
    pub edge_projection: Option<EdgeCardinalProjection>,
}

struct DataRelationshipEdgeProjection {
    projection: EdgeCardinalProjection,
}

struct Edge {
    id: EdgeId,
}

struct EdgeCardinal {
    idx: usize,
    inner: domain::EdgeCardinal,
}

#[derive(juniper::GraphQLEnum, PartialEq, Clone, Copy)]
pub enum RelationshipKindEnum {
    Id,
    Tree,
    Edge,
}

#[derive(juniper::GraphQLEnum)]
pub enum DataRelationshipSource {
    Inherent,
    ByUnionProxy,
}

#[derive(juniper::GraphQLEnum)]
enum PropertyCardinality {
    Optional,
    Mandatory,
}

#[derive(juniper::GraphQLObject)]
pub struct Cardinality {
    property_cardinality: PropertyCardinality,
    value_cardinality: ValueCardinality,
}

#[derive(juniper::GraphQLEnum)]
enum ValueCardinality {
    /// The property supports at most one defined value
    Unit,
    /// The property supports a set of values.
    ///
    /// The set is indexed, i.e. remembers the order of entries.
    IndexSet,
    /// The property supports a list of values, duplicates are allowed.
    List,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl DataRelationshipInfo {
    fn relationship_id(&self) -> String {
        self.rel_id.to_string()
    }
    fn source(&self, ctx: &Ctx) -> DataRelationshipSource {
        let type_info = ctx.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.rel_id).unwrap();
        match data_relationship.source {
            ontol_runtime::ontology::domain::DataRelationshipSource::Inherent => {
                DataRelationshipSource::Inherent
            }
            ontol_runtime::ontology::domain::DataRelationshipSource::ByUnionProxy => {
                DataRelationshipSource::ByUnionProxy
            }
        }
    }
    fn target(&self, ctx: &Ctx) -> gql_domain::TypeInfo {
        let type_info = ctx.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.rel_id).unwrap();
        let target_def_id = match data_relationship.target {
            ontol_runtime::ontology::domain::DataRelationshipTarget::Unambiguous(def_id) => def_id,
            ontol_runtime::ontology::domain::DataRelationshipTarget::Union(def_id) => def_id,
        };
        gql_domain::TypeInfo { id: target_def_id }
    }
    fn name(&self, ctx: &Ctx) -> String {
        let type_info = ctx.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.rel_id).unwrap();
        ctx[data_relationship.name].into()
    }
    fn cardinality(&self, ctx: &Ctx) -> Cardinality {
        let type_info = ctx.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.rel_id).unwrap();
        Cardinality::from(data_relationship.cardinality)
    }
    fn kind(&self) -> RelationshipKindEnum {
        self.kind
    }
    fn edge_projection(&self) -> Option<DataRelationshipEdgeProjection> {
        self.edge_projection
            .map(|projection| DataRelationshipEdgeProjection { projection })
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl DataRelationshipEdgeProjection {
    fn subject(&self) -> i32 {
        self.projection.subject.0.into()
    }

    fn object(&self) -> i32 {
        self.projection.object.0.into()
    }

    fn edge(&self) -> Edge {
        Edge {
            id: self.projection.id,
        }
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl Edge {
    fn type_info(&self) -> gql_domain::TypeInfo {
        gql_domain::TypeInfo { id: self.id.0 }
    }

    fn cardinals(&self, ctx: &Ctx) -> Vec<EdgeCardinal> {
        let edge = ctx.find_edge(self.id).unwrap();
        edge.cardinals
            .iter()
            .enumerate()
            .map(|(idx, cardinal)| EdgeCardinal {
                idx,
                inner: cardinal.clone(),
            })
            .collect()
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl EdgeCardinal {
    fn index(&self) -> i32 {
        self.idx.try_into().unwrap()
    }

    fn target(&self) -> gql_domain::TypeInfo {
        let target_def_id = match self.inner.target {
            ontol_runtime::ontology::domain::DataRelationshipTarget::Unambiguous(def_id) => def_id,
            ontol_runtime::ontology::domain::DataRelationshipTarget::Union(def_id) => def_id,
        };
        gql_domain::TypeInfo { id: target_def_id }
    }
}

impl From<property::Cardinality> for Cardinality {
    fn from(value: property::Cardinality) -> Self {
        Self {
            property_cardinality: match value.0 {
                property::PropertyCardinality::Optional => PropertyCardinality::Optional,
                property::PropertyCardinality::Mandatory => PropertyCardinality::Mandatory,
            },
            value_cardinality: match value.1 {
                property::ValueCardinality::Unit => ValueCardinality::Unit,
                property::ValueCardinality::IndexSet => ValueCardinality::IndexSet,
                property::ValueCardinality::List => ValueCardinality::List,
            },
        }
    }
}
