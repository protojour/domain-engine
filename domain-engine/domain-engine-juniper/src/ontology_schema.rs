use std::{ops::Deref, str::FromStr, sync::Arc};

use crate::juniper::{
    self, graphql_object, graphql_value, EmptyMutation, EmptySubscription, FieldError, FieldResult,
    RootNode,
};

use ::juniper::{GraphQLEnum, GraphQLObject};
use ontol_runtime::{
    ontology::{domain::TypeKind, Ontology},
    property::PropertyId,
    DefId, PackageId,
};

struct Domain {
    id: PackageId,
}

struct TypeInfo {
    id: DefId,
}

struct DataRelationshipInfo {
    def_id: DefId,
    property_id: PropertyId,
}

struct Entity {
    id: DefId,
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
    fn subject_name(&self, context: &Context) -> String {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        context[data_relationship.subject_name].into()
    }
    fn object_name(&self, context: &Context) -> Option<String> {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        data_relationship
            .object_name
            .map(|text_constant| context[text_constant].into())
    }
    fn subject_cardinality(&self, context: &Context) -> Cardinality {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        let (property_cardinality, value_cardinality) = data_relationship.subject_cardinality;
        let property_cardinality = match property_cardinality {
            ontol_runtime::property::PropertyCardinality::Optional => PropertyCardinality::Optional,
            ontol_runtime::property::PropertyCardinality::Mandatory => {
                PropertyCardinality::Mandatory
            }
        };
        let value_cardinality = match value_cardinality {
            ontol_runtime::property::ValueCardinality::Unit => ValueCardinality::Unit,
            ontol_runtime::property::ValueCardinality::IndexSet => ValueCardinality::IndexSet,
            ontol_runtime::property::ValueCardinality::List => ValueCardinality::List,
        };
        Cardinality {
            property_cardinality,
            value_cardinality,
        }
    }
    fn object_cardinality(&self, context: &Context) -> Cardinality {
        let type_info = context.get_type_info(self.def_id);
        let data_relationship = type_info.data_relationships.get(&self.property_id).unwrap();
        let (property_cardinality, value_cardinality) = data_relationship.object_cardinality;
        let property_cardinality = match property_cardinality {
            ontol_runtime::property::PropertyCardinality::Optional => PropertyCardinality::Optional,
            ontol_runtime::property::PropertyCardinality::Mandatory => {
                PropertyCardinality::Mandatory
            }
        };
        let value_cardinality = match value_cardinality {
            ontol_runtime::property::ValueCardinality::Unit => ValueCardinality::Unit,
            ontol_runtime::property::ValueCardinality::IndexSet => ValueCardinality::IndexSet,
            ontol_runtime::property::ValueCardinality::List => ValueCardinality::List,
        };
        Cardinality {
            property_cardinality,
            value_cardinality,
        }
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

// #[derive(GraphQLEnum, PartialEq)]
// enum RelationshipKindEnum {
//     Id,
//     Tree,
//     EntityGraph,
// }

#[graphql_object]
#[graphql(context = Context)]
impl TypeInfo {
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
            .keys()
            .copied()
            .map(|property_id| DataRelationshipInfo {
                def_id: self.id,
                property_id,
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
