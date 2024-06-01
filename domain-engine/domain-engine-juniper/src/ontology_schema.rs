use std::{ops::Deref, str::FromStr, sync::Arc};

use crate::juniper::{
    self, graphql_object, graphql_value, EmptyMutation, EmptySubscription, FieldError, FieldResult,
    RootNode,
};

use ::juniper::{GraphQLEnum, GraphQLObject};
use ontol_runtime::{
    ontology::{
        domain::{self, EdgeCardinalId, TypeKind},
        map::MapMeta,
        ontol::TextConstant,
        Ontology,
    },
    property::{self, PropertyId},
    DefId, MapKey, PackageId,
};

struct Domain {
    id: PackageId,
}

struct TypeInfo {
    id: DefId,
}

struct NamedMap {
    name: TextConstant,
}

struct MapEdge {
    key: MapKey,
    meta: MapMeta,
}

struct DataRelationshipInfo {
    def_id: DefId,
    kind: RelationshipKindEnum,
    property_id: PropertyId,
    edge_cardinal_id: Option<EdgeCardinalId>,
}

struct DataRelationshipEdge {
    id: EdgeCardinalId,
}

struct Edge {
    edge_id: DefId,
}

struct EdgeCardinal {
    idx: usize,
    inner: domain::EdgeCardinal,
}

struct Entity {
    id: DefId,
}

struct PropertyFlow {
    source: PropertyId,
    target: PropertyId,
    kind: PropertyFlowKind,
}

#[derive(GraphQLEnum)]
enum PropertyFlowKind {
    ChildOf,
    DependentOn,
}

#[derive(GraphQLEnum)]
enum PropertyCardinality {
    Optional,
    Mandatory,
}

#[derive(GraphQLEnum)]
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

#[derive(GraphQLObject)]
struct Cardinality {
    property_cardinality: PropertyCardinality,
    value_cardinality: ValueCardinality,
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

#[derive(GraphQLEnum)]
enum DataRelationshipSource {
    Inherent,
    ByUnionProxy,
}

pub struct Query;

#[derive(Clone)]
pub struct Context {
    pub ontology: Arc<Ontology>,
}

impl Deref for Context {
    type Target = Ontology;

    fn deref(&self) -> &Self::Target {
        &self.ontology
    }
}

impl juniper::Context for Context {}

#[graphql_object]
#[graphql(context = Context)]
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

#[graphql_object]
#[graphql(context = Context)]
impl DataRelationshipInfo {
    fn property_id(&self) -> String {
        self.property_id.to_string()
    }
    fn source(&self, context: &Context) -> DataRelationshipSource {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        match data_relationship.source {
            ontol_runtime::ontology::domain::DataRelationshipSource::Inherent => {
                DataRelationshipSource::Inherent
            }
            ontol_runtime::ontology::domain::DataRelationshipSource::ByUnionProxy => {
                DataRelationshipSource::ByUnionProxy
            }
        }
    }
    fn target(&self, context: &Context) -> TypeInfo {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        let target_def_id = match data_relationship.target {
            ontol_runtime::ontology::domain::DataRelationshipTarget::Unambiguous(def_id) => def_id,
            ontol_runtime::ontology::domain::DataRelationshipTarget::Union(def_id) => def_id,
        };
        TypeInfo { id: target_def_id }
    }
    fn name(&self, context: &Context) -> String {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        context[data_relationship.name].into()
    }
    fn cardinality(&self, context: &Context) -> Cardinality {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        Cardinality::from(data_relationship.cardinality)
    }
    fn kind(&self) -> RelationshipKindEnum {
        self.kind
    }
    fn edge(&self) -> Option<DataRelationshipEdge> {
        self.edge_cardinal_id
            .map(|edge_cardinal_id| DataRelationshipEdge {
                id: edge_cardinal_id,
            })
    }
}

#[graphql_object]
#[graphql(context = Context)]
impl DataRelationshipEdge {
    fn cardinal_idx(&self) -> i32 {
        self.id.cardinal_idx.0.into()
    }

    fn edge(&self) -> Edge {
        Edge {
            edge_id: self.id.edge_id,
        }
    }
}

#[graphql_object]
#[graphql(context = Context)]
impl Edge {
    fn type_info(&self) -> TypeInfo {
        TypeInfo { id: self.edge_id }
    }

    fn cardinals(&self, context: &Context) -> Vec<EdgeCardinal> {
        let edge = context.find_edge(self.edge_id).unwrap();
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

#[graphql_object]
#[graphql(context = Context)]
impl EdgeCardinal {
    fn index(&self) -> i32 {
        self.idx.try_into().unwrap()
    }

    fn target(&self) -> TypeInfo {
        let target_def_id = match self.inner.target {
            ontol_runtime::ontology::domain::DataRelationshipTarget::Unambiguous(def_id) => def_id,
            ontol_runtime::ontology::domain::DataRelationshipTarget::Union(def_id) => def_id,
        };
        TypeInfo { id: target_def_id }
    }
}

#[graphql_object]
#[graphql(context = Context)]
impl NamedMap {
    fn name(&self, context: &Context) -> String {
        context[self.name].to_string()
    }
}

#[graphql_object]
#[graphql(context = Context)]
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

    fn property_flows(&self, context: &Context) -> Vec<PropertyFlow> {
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

#[graphql_object]
#[graphql(context = Context)]
impl Entity {
    fn is_self_identifying(&self, context: &Context) -> bool {
        let type_info = context.get_type_info(self.id);
        let entity_info = type_info.entity_info().unwrap();
        entity_info.is_self_identifying
    }
}

#[derive(GraphQLEnum, PartialEq)]
enum TypeKindEnum {
    Entity,
    Data,
    Relationship,
    Function,
    Domain,
    Generator,
}

#[derive(GraphQLEnum, PartialEq, Clone, Copy)]
enum RelationshipKindEnum {
    Id,
    Tree,
    Edge,
}

#[graphql_object]
#[graphql(context = Context)]
impl TypeInfo {
    fn id(&self) -> String {
        format!("{:?}", self.id)
    }
    fn name(&self, context: &Context) -> Option<String> {
        let type_info = context.get_type_info(self.id);
        type_info.name().map(|name| context[name].into())
    }
    fn doc_string(&self, context: &Context) -> Option<String> {
        let type_info = context.get_type_info(self.id);
        context
            .get_docs(type_info.def_id)
            .map(|docs_constant| context[docs_constant].into())
    }
    fn entity_info(&self, context: &Context) -> Option<Entity> {
        let type_info = context.get_type_info(self.id);
        type_info.entity_info().map(|_| Entity { id: self.id })
    }
    fn union_variants(&self, context: &Context) -> Vec<TypeInfo> {
        context
            .union_variants(self.id)
            .iter()
            .copied()
            .map(|id| TypeInfo { id })
            .collect()
    }
    fn kind(&self, context: &Context) -> TypeKindEnum {
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
    fn data_relationships(&self, context: &Context) -> Vec<DataRelationshipInfo> {
        let type_info = context.get_type_info(self.id);
        type_info
            .data_relationships
            .iter()
            .map(|(property_id, dri)| {
                let (kind, edge_cardinal_id) = match dri.kind {
                    ontol_runtime::ontology::domain::DataRelationshipKind::Id => {
                        (RelationshipKindEnum::Id, None)
                    }
                    ontol_runtime::ontology::domain::DataRelationshipKind::Tree => {
                        (RelationshipKindEnum::Tree, None)
                    }
                    ontol_runtime::ontology::domain::DataRelationshipKind::Edge(cardinal_id) => {
                        (RelationshipKindEnum::Edge, Some(cardinal_id))
                    }
                };
                DataRelationshipInfo {
                    kind,
                    def_id: self.id,
                    property_id: *property_id,
                    edge_cardinal_id,
                }
            })
            .collect()
    }
    fn maps_to(&self, context: &Context) -> Vec<MapEdge> {
        context
            .iter_map_meta()
            .filter(|(map_key, _)| map_key.input.def_id == self.id)
            .map(|(key, meta)| MapEdge {
                key,
                meta: meta.clone(),
            })
            .collect()
    }

    fn maps_from(&self, context: &Context) -> Vec<MapEdge> {
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

#[graphql_object]
#[graphql(context = Context)]
impl Domain {
    fn name(&self, context: &Context) -> String {
        let domain = context.find_domain(self.id).unwrap();
        String::from(&context[domain.unique_name()])
    }
    fn entities(&self, context: &Context) -> Vec<Entity> {
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
    fn types(&self, context: &Context, kind: Option<TypeKindEnum>) -> Vec<TypeInfo> {
        let domain = context.find_domain(self.id).unwrap();
        let infos = domain.type_infos().map(|info| TypeInfo { id: info.def_id });
        if let Some(kind) = kind {
            infos.filter(|info| info.kind(context) == kind).collect()
        } else {
            infos.collect()
        }
    }
    fn maps(&self, context: &Context) -> Vec<NamedMap> {
        // let domain = context.find_domain(self.id).unwrap();
        context
            .ontology
            .iter_named_downmaps()
            .filter(|(package_id, ..)| package_id == &self.id)
            .map(|(_, name, _)| NamedMap { name })
            .collect()
    }
}

#[graphql_object]
#[graphql(context = Context)]
impl Query {
    fn api_version() -> &'static str {
        "1.0"
    }
    fn domain(name: String, context: &Context) -> FieldResult<Domain> {
        let ontology = &context.ontology;
        let domain = ontology
            .domains()
            .find(|(_, d)| ontology.get_text_constant(d.unique_name()).to_string() == name);
        if let Some((id, _)) = domain {
            Ok(Domain { id: *id })
        } else {
            Err(FieldError::new(
                "Domain not found",
                graphql_value!({"internal_error": "Domain not found"}),
            ))
        }
    }
    fn domains(context: &Context) -> FieldResult<Vec<Domain>> {
        let ontology = &context.ontology;
        let mut domains = vec![];
        for (package_id, _ontology_domain) in ontology.domains() {
            let domain = Domain { id: *package_id };
            domains.push(domain);
        }
        Ok(domains)
    }
    fn def(def_id: String, context: &Context) -> FieldResult<TypeInfo> {
        let ontology = &context.ontology;
        if let Ok(def_id) = DefId::from_str(&def_id) {
            if let Some(type_info) = ontology.get_type_info_option(def_id) {
                return Ok(TypeInfo {
                    id: type_info.def_id,
                });
            }
        }
        Err(FieldError::new(
            "TypeInfo not found",
            graphql_value!({"internal_error": "TypeInfo not found"}),
        ))
    }
}

pub type Schema = RootNode<'static, Query, EmptyMutation<Context>, EmptySubscription<Context>>;
