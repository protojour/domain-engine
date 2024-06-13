use ontol_runtime::ontology::map::MapMeta;
use ontol_runtime::ontology::ontol::TextConstant;
use ontol_runtime::{ontology::domain::DefKind, DefId, PackageId};
use ontol_runtime::{MapKey, RelationshipId};

use crate::juniper;

use super::Ctx;

use super::gql_rel;

pub struct Domain {
    pub id: PackageId,
}

#[derive(Clone)]
pub struct Def {
    pub id: DefId,
}

struct Entity {
    pub id: DefId,
}

#[derive(juniper::GraphQLEnum, PartialEq)]
enum TypeKindEnum {
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

struct NamedMap {
    name: TextConstant,
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl Domain {
    fn name(&self, ctx: &Ctx) -> String {
        let domain = ctx.find_domain(self.id).unwrap();
        String::from(&ctx[domain.unique_name()])
    }
    fn entities(&self, ctx: &Ctx) -> Vec<Entity> {
        let domain = ctx.find_domain(self.id).unwrap();
        let mut entities = vec![];
        for def in domain.defs() {
            if matches!(def.kind, DefKind::Entity(_)) {
                entities.push(Entity { id: def.id })
            }
        }
        entities
    }
    fn defs(&self, ctx: &Ctx, kind: Option<TypeKindEnum>) -> Vec<Def> {
        let infos = ctx
            .find_domain(self.id)
            .unwrap()
            .defs()
            .map(|info| Def { id: info.id });
        if let Some(kind) = kind {
            infos.filter(|info| info.kind(ctx) == kind).collect()
        } else {
            infos.collect()
        }
    }
    fn maps(&self, ctx: &Ctx) -> Vec<NamedMap> {
        ctx.iter_named_downmaps()
            .filter(|(package_id, ..)| package_id == &self.id)
            .map(|(_, name, _)| NamedMap { name })
            .collect()
    }
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
    fn kind(&self, ctx: &Ctx) -> TypeKindEnum {
        match ctx.def(self.id).kind {
            DefKind::Entity(_) => TypeKindEnum::Entity,
            DefKind::Data(_) => TypeKindEnum::Data,
            DefKind::Relationship(_) => TypeKindEnum::Relationship,
            DefKind::Function(_) => TypeKindEnum::Function,
            DefKind::Domain(_) => TypeKindEnum::Domain,
            DefKind::Generator(_) => TypeKindEnum::Generator,
        }
    }
    fn data_relationships(&self, ctx: &Ctx) -> Vec<gql_rel::DataRelationshipInfo> {
        ctx.def(self.id)
            .data_relationships
            .iter()
            .map(|(rel_id, dri)| {
                let (kind, edge_projection) = match dri.kind {
                    ontol_runtime::ontology::domain::DataRelationshipKind::Id => {
                        (gql_rel::RelationshipKindEnum::Id, None)
                    }
                    ontol_runtime::ontology::domain::DataRelationshipKind::Tree => {
                        (gql_rel::RelationshipKindEnum::Tree, None)
                    }
                    ontol_runtime::ontology::domain::DataRelationshipKind::Edge(cardinal_id) => {
                        (gql_rel::RelationshipKindEnum::Edge, Some(cardinal_id))
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
