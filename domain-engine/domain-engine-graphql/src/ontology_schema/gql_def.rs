//! GraphQL types related to a single ONTOL domain definition

use ontol_runtime::{
    ontology::{domain, map::MapMeta, ontol::TextConstant},
    DefId, MapKey, RelationshipId,
};

use super::{gql_rel, Ctx};

#[derive(Clone)]
pub struct Def {
    pub id: DefId,
}

pub struct Entity {
    pub id: DefId,
}

#[derive(juniper::GraphQLEnum, PartialEq)]
pub enum DefKind {
    Entity,
    Data,
    Relationship,
    Function,
    Domain,
    Generator,
}

struct MapEdge {
    key: MapKey,
    meta: MapMeta,
}

pub struct NamedMap {
    pub name: TextConstant,
}

struct PropertyFlow {
    source: RelationshipId,
    target: RelationshipId,
    kind: PropertyFlowKind,
}

#[derive(juniper::GraphQLEnum)]
enum PropertyFlowKind {
    ChildOf,
    DependentOn,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl Def {
    fn id(&self) -> String {
        format!("{:?}", self.id)
    }
    fn name(&self, ctx: &Ctx) -> Option<String> {
        ctx.def(self.id).name().map(|name| ctx[name].into())
    }
    fn doc_string(&self, ctx: &Ctx) -> Option<String> {
        ctx.get_docs(self.id)
            .map(|docs_constant| ctx[docs_constant].into())
    }
    fn entity(&self, ctx: &Ctx) -> Option<Entity> {
        ctx.def(self.id).entity().map(|_| Entity { id: self.id })
    }
    fn union_variants(&self, ctx: &Ctx) -> Vec<Def> {
        ctx.union_variants(self.id)
            .iter()
            .copied()
            .map(|id| Def { id })
            .collect()
    }
    pub fn kind(&self, ctx: &Ctx) -> DefKind {
        match ctx.def(self.id).kind {
            domain::DefKind::Entity(_) => DefKind::Entity,
            domain::DefKind::Data(_) => DefKind::Data,
            domain::DefKind::Relationship(_) => DefKind::Relationship,
            domain::DefKind::Function(_) => DefKind::Function,
            domain::DefKind::Domain(_) => DefKind::Domain,
            domain::DefKind::Generator(_) => DefKind::Generator,
        }
    }
    fn data_relationships(&self, ctx: &Ctx) -> Vec<gql_rel::DataRelationshipInfo> {
        ctx.def(self.id)
            .data_relationships
            .iter()
            .map(|(rel_id, dri)| {
                let (kind, edge_projection) = match dri.kind {
                    ontol_runtime::ontology::domain::DataRelationshipKind::Id => {
                        (gql_rel::RelationshipKind::Id, None)
                    }
                    ontol_runtime::ontology::domain::DataRelationshipKind::Tree => {
                        (gql_rel::RelationshipKind::Tree, None)
                    }
                    ontol_runtime::ontology::domain::DataRelationshipKind::Edge(cardinal_id) => {
                        (gql_rel::RelationshipKind::Edge, Some(cardinal_id))
                    }
                };
                gql_rel::DataRelationshipInfo {
                    kind,
                    def_id: self.id,
                    rel_id: *rel_id,
                    edge_projection,
                }
            })
            .collect()
    }
    fn maps_to(&self, ctx: &Ctx) -> Vec<MapEdge> {
        ctx.iter_map_meta()
            .filter(|(map_key, _)| map_key.input.def_id == self.id)
            .map(|(key, meta)| MapEdge {
                key,
                meta: meta.clone(),
            })
            .collect()
    }

    fn maps_from(&self, ctx: &Ctx) -> Vec<MapEdge> {
        ctx.iter_map_meta()
            .filter(|(map_key, _)| map_key.output.def_id == self.id)
            .map(|(key, meta)| MapEdge {
                key,
                meta: meta.clone(),
            })
            .collect()
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl Entity {
    fn is_self_identifying(&self, ctx: &Ctx) -> bool {
        ctx.def(self.id).entity().unwrap().is_self_identifying
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl MapEdge {
    fn output(&self) -> Def {
        Def {
            id: self.key.output.def_id,
        }
    }
    fn input(&self) -> Def {
        Def {
            id: self.key.input.def_id,
        }
    }

    fn property_flows(&self, ctx: &Ctx) -> Vec<PropertyFlow> {
        ctx.get_prop_flow_slice(&self.meta)
            .unwrap_or(&[])
            .iter()
            .filter_map(|flow| match flow.data {
                ontol_runtime::ontology::map::PropertyFlowData::ChildOf(property_id) => {
                    Some(PropertyFlow {
                        source: flow.id,
                        target: property_id,
                        kind: PropertyFlowKind::ChildOf,
                    })
                }
                ontol_runtime::ontology::map::PropertyFlowData::DependentOn(property_id) => {
                    Some(PropertyFlow {
                        source: flow.id,
                        target: property_id,
                        kind: PropertyFlowKind::DependentOn,
                    })
                }
                _ => None,
            })
            .collect()
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl PropertyFlow {
    fn source(&self) -> String {
        self.source.to_string()
    }
    fn target(&self) -> String {
        self.target.to_string()
    }
    fn kind(&self) -> &PropertyFlowKind {
        &self.kind
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl NamedMap {
    fn name(&self, context: &Ctx) -> String {
        context[self.name].to_string()
    }
}
