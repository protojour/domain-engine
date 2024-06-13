use ontol_runtime::ontology::map::MapMeta;
use ontol_runtime::ontology::ontol::TextConstant;
use ontol_runtime::{ontology::domain::TypeKind, DefId, PackageId};
use ontol_runtime::{MapKey, RelationshipId};

use crate::juniper;

use super::Ctx;

use super::gql_rel;

pub struct Domain {
    pub id: PackageId,
}

pub struct TypeInfo {
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
    fn name(&self, context: &Ctx) -> String {
        let domain = context.find_domain(self.id).unwrap();
        String::from(&context[domain.unique_name()])
    }
    fn entities(&self, context: &Ctx) -> Vec<Entity> {
        let domain = context.find_domain(self.id).unwrap();
        let mut entities = vec![];
        for type_info in domain.type_infos() {
            if matches!(type_info.kind, TypeKind::Entity(_)) {
                entities.push(Entity {
                    id: type_info.def_id,
                })
            }
        }
        entities
    }
    fn types(&self, context: &Ctx, kind: Option<TypeKindEnum>) -> Vec<TypeInfo> {
        let domain = context.find_domain(self.id).unwrap();
        let infos = domain.type_infos().map(|info| TypeInfo { id: info.def_id });
        if let Some(kind) = kind {
            infos.filter(|info| info.kind(context) == kind).collect()
        } else {
            infos.collect()
        }
    }
    fn maps(&self, context: &Ctx) -> Vec<NamedMap> {
        // let domain = context.find_domain(self.id).unwrap();
        context
            .ontology
            .iter_named_downmaps()
            .filter(|(package_id, ..)| package_id == &self.id)
            .map(|(_, name, _)| NamedMap { name })
            .collect()
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl TypeInfo {
    fn id(&self) -> String {
        format!("{:?}", self.id)
    }
    fn name(&self, context: &Ctx) -> Option<String> {
        let type_info = context.get_type_info(self.id);
        type_info.name().map(|name| context[name].into())
    }
    fn doc_string(&self, context: &Ctx) -> Option<String> {
        let type_info = context.get_type_info(self.id);
        context
            .get_docs(type_info.def_id)
            .map(|docs_constant| context[docs_constant].into())
    }
    fn entity_info(&self, context: &Ctx) -> Option<Entity> {
        let type_info = context.get_type_info(self.id);
        type_info.entity_info().map(|_| Entity { id: self.id })
    }
    fn union_variants(&self, context: &Ctx) -> Vec<TypeInfo> {
        context
            .union_variants(self.id)
            .iter()
            .copied()
            .map(|id| TypeInfo { id })
            .collect()
    }
    fn kind(&self, context: &Ctx) -> TypeKindEnum {
        let type_info = context.get_type_info(self.id);
        match type_info.kind {
            TypeKind::Entity(_) => TypeKindEnum::Entity,
            TypeKind::Data(_) => TypeKindEnum::Data,
            TypeKind::Relationship(_) => TypeKindEnum::Relationship,
            TypeKind::Function(_) => TypeKindEnum::Function,
            TypeKind::Domain(_) => TypeKindEnum::Domain,
            TypeKind::Generator(_) => TypeKindEnum::Generator,
        }
    }
    fn data_relationships(&self, context: &Ctx) -> Vec<gql_rel::DataRelationshipInfo> {
        let type_info = context.get_type_info(self.id);
        type_info
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
    fn maps_to(&self, context: &Ctx) -> Vec<MapEdge> {
        context
            .iter_map_meta()
            .filter(|(map_key, _)| map_key.input.def_id == self.id)
            .map(|(key, meta)| MapEdge {
                key,
                meta: meta.clone(),
            })
            .collect()
    }

    fn maps_from(&self, context: &Ctx) -> Vec<MapEdge> {
        context
            .iter_map_meta()
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
    fn is_self_identifying(&self, context: &Ctx) -> bool {
        let type_info = context.get_type_info(self.id);
        let entity_info = type_info.entity_info().unwrap();
        entity_info.is_self_identifying
    }
}

#[juniper::graphql_object]
#[graphql(context = Ctx)]
impl MapEdge {
    fn output(&self) -> TypeInfo {
        TypeInfo {
            id: self.key.output.def_id,
        }
    }
    fn input(&self) -> TypeInfo {
        TypeInfo {
            id: self.key.input.def_id,
        }
    }

    fn property_flows(&self, context: &Ctx) -> Vec<PropertyFlow> {
        let Some(slice) = context.get_prop_flow_slice(&self.meta) else {
            return vec![];
        };
        slice
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
