//! GraphQL types related to a single ONTOL domain definition

use ontol_runtime::{
    ontology::{
        domain::{self, DataRelationshipKind},
        map::MapMeta,
        ontol::TextConstant,
    },
    DefId, DefPropTag, MapKey, PropId,
};

use crate::gql_scalar::GqlScalar;

use super::{gql_domain, gql_id, gql_rel, OntologyCtx};

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
    Edge,
    Relation,
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
    source: PropId,
    target: PropId,
    kind: PropertyFlowKind,
}

#[derive(juniper::GraphQLEnum)]
enum PropertyFlowKind {
    ChildOf,
    DependentOn,
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
impl Def {
    /// The identifier of this def.
    /// The ID is unique only within this particular ontology.
    fn id(&self, ctx: &OntologyCtx) -> gql_id::DefId {
        let domain = ctx.domain_by_index(self.id.domain_index()).unwrap();
        gql_id::DefId {
            domain_id: domain.domain_id().ulid,
            def_tag: self.id.1,
        }
    }

    /// The tag of this def, unique within its domain.
    fn tag(&self) -> i32 {
        self.id.1.into()
    }

    /// The domain of this Def
    fn domain(&self) -> gql_domain::Domain {
        gql_domain::Domain {
            domain_index: self.id.domain_index(),
        }
    }

    /// The ONTOL identifier of this def
    fn ident(&self, ctx: &OntologyCtx) -> Option<String> {
        ctx.def(self.id).ident().map(|name| ctx[name].into())
    }

    /// The documentation of this def
    fn doc(&self, ctx: &OntologyCtx) -> Option<String> {
        ctx.get_def_docs(self.id).map(|docs| docs.to_string())
    }

    /// The prop-id if this Def is used directly as a property
    fn prop_id(&self) -> String {
        PropId(self.id, DefPropTag(0)).to_string()
    }

    /// The optional entity represented by this def
    fn entity(&self, ctx: &OntologyCtx) -> Option<Entity> {
        ctx.def(self.id).entity().map(|_| Entity { id: self.id })
    }

    fn union_variants(&self, ctx: &OntologyCtx) -> Vec<Def> {
        ctx.union_variants(self.id)
            .iter()
            .copied()
            .map(|id| Def { id })
            .collect()
    }
    pub fn kind(&self, ctx: &OntologyCtx) -> DefKind {
        match ctx.def(self.id).kind {
            domain::DefKind::Entity(_) => DefKind::Entity,
            domain::DefKind::Data(_) => DefKind::Data,
            domain::DefKind::Edge(_) => DefKind::Edge,
            domain::DefKind::Relation(_) => DefKind::Relation,
            domain::DefKind::Function(_) => DefKind::Function,
            domain::DefKind::Domain(_) => DefKind::Domain,
            domain::DefKind::Generator(_) => DefKind::Generator,
        }
    }
    fn data_relationships(&self, ctx: &OntologyCtx) -> Vec<gql_rel::DataRelationshipInfo> {
        ctx.def(self.id)
            .data_relationships
            .iter()
            .map(|(prop_id, dri)| {
                let (kind, edge_projection) = match dri.kind {
                    DataRelationshipKind::Id => (gql_rel::RelationshipKind::Id, None),
                    DataRelationshipKind::Tree(_) => (gql_rel::RelationshipKind::Tree, None),
                    DataRelationshipKind::Edge(cardinal_id) => {
                        (gql_rel::RelationshipKind::Edge, Some(cardinal_id))
                    }
                };
                gql_rel::DataRelationshipInfo {
                    kind,
                    def_id: self.id,
                    prop_id: *prop_id,
                    edge_projection,
                }
            })
            .collect()
    }
    fn maps_to(&self, ctx: &OntologyCtx) -> Vec<MapEdge> {
        ctx.iter_map_meta()
            .filter(|(map_key, _)| map_key.input.def_id == self.id)
            .map(|(key, meta)| MapEdge {
                key,
                meta: meta.clone(),
            })
            .collect()
    }

    fn maps_from(&self, ctx: &OntologyCtx) -> Vec<MapEdge> {
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
#[graphql(context = OntologyCtx)]
impl Entity {
    fn is_self_identifying(&self, ctx: &OntologyCtx) -> bool {
        ctx.def(self.id).entity().unwrap().is_self_identifying
    }
}

#[juniper::graphql_object]
#[graphql(context = OntologyCtx, scalar = GqlScalar)]
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

    fn property_flows(&self, ctx: &OntologyCtx) -> Vec<PropertyFlow> {
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
#[graphql(context = OntologyCtx)]
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
#[graphql(context = OntologyCtx)]
impl NamedMap {
    fn name(&self, context: &OntologyCtx) -> String {
        context[self.name].to_string()
    }
}
