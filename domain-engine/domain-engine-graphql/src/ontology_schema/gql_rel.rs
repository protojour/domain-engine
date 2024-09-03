//! GraphQL types related to ONTOL relations

use crate::juniper;

use ontol_runtime::{
    ontology::domain::{self, EdgeCardinalProjection},
    property, DefId, PropId,
};

use super::{gql_def, OntologyCtx};

pub struct DataRelationshipInfo {
    pub def_id: DefId,
    pub kind: RelationshipKind,
    pub prop_id: PropId,
    pub edge_projection: Option<EdgeCardinalProjection>,
}

struct DataRelationshipEdgeProjection {
    projection: EdgeCardinalProjection,
}

struct Edge {
    id: DefId,
}

struct EdgeCardinal {
    idx: usize,
    inner: domain::EdgeCardinal,
}

#[derive(juniper::GraphQLEnum, PartialEq, Clone, Copy)]
pub enum RelationshipKind {
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
#[graphql(context = OntologyCtx)]
impl DataRelationshipInfo {
    fn prop_id(&self) -> String {
        self.prop_id.to_string()
    }
    fn source(&self, ctx: &OntologyCtx) -> DataRelationshipSource {
        let def = ctx.def(self.def_id);
        let data_relationship = def.data_relationships.get(&self.prop_id).unwrap();
        match data_relationship.source {
            ontol_runtime::ontology::domain::DataRelationshipSource::Inherent => {
                DataRelationshipSource::Inherent
            }
            ontol_runtime::ontology::domain::DataRelationshipSource::ByUnionProxy => {
                DataRelationshipSource::ByUnionProxy
            }
        }
    }
    fn target(&self, ctx: &OntologyCtx) -> gql_def::Def {
        let def = ctx.def(self.def_id);
        let data_relationship = def.data_relationships.get(&self.prop_id).unwrap();
        let target_def_id = match data_relationship.target {
            ontol_runtime::ontology::domain::DataRelationshipTarget::Unambiguous(def_id) => def_id,
            ontol_runtime::ontology::domain::DataRelationshipTarget::Union(def_id) => def_id,
        };
        gql_def::Def { id: target_def_id }
    }
    fn name(&self, ctx: &OntologyCtx) -> String {
        let def = ctx.def(self.def_id);
        let data_relationship = def.data_relationships.get(&self.prop_id).unwrap();
        ctx[data_relationship.name].into()
    }
    fn cardinality(&self, ctx: &OntologyCtx) -> Cardinality {
        let def = ctx.def(self.def_id);
        let data_relationship = def.data_relationships.get(&self.prop_id).unwrap();
        Cardinality::from(data_relationship.cardinality)
    }
    fn kind(&self) -> RelationshipKind {
        self.kind
    }
    fn edge_projection(&self) -> Option<DataRelationshipEdgeProjection> {
        self.edge_projection
            .map(|projection| DataRelationshipEdgeProjection { projection })
    }
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx)]
impl DataRelationshipEdgeProjection {
    fn subject(&self) -> i32 {
        self.projection.subject.0.into()
    }

    fn object(&self) -> i32 {
        self.projection.object.0.into()
    }

    fn edge(&self) -> Edge {
        Edge {
            id: self.projection.edge_id,
        }
    }
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx)]
impl Edge {
    // fn def(&self) -> gql_def::Def {
    //     gql_def::Def { id: self.id.0 }
    // }

    fn cardinals(&self, ctx: &OntologyCtx) -> Vec<EdgeCardinal> {
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
#[graphql(context = OntologyCtx)]
impl EdgeCardinal {
    fn index(&self) -> i32 {
        self.idx.try_into().unwrap()
    }

    fn target(&self) -> Vec<gql_def::Def> {
        self.inner
            .target
            .iter()
            .map(|def_id| gql_def::Def { id: *def_id })
            .collect()
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
