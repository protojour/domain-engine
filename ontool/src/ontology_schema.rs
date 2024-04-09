use std::{ops::Deref, str::FromStr, sync::Arc};

use domain_engine_juniper::juniper::{
    self, graphql_object, graphql_value, EmptyMutation, EmptySubscription, FieldError, FieldResult,
    RootNode,
};

use ::juniper::GraphQLEnum;
use ontol_runtime::{
    ontology::{domain::TypeKind, Ontology},
    DefId, PackageId,
};

struct Domain {
    id: PackageId,
}

struct TypeInfo {
    id: DefId,
    kind: TypeKindEnum,
}

struct Entity {
    id: DefId,
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
impl Entity {
    fn name(&self, context: &Context) -> Option<String> {
        let entity = context.get_type_info(self.id);
        entity.name().map(|name| context[name].into())
    }
    fn doc_string(&self, context: &Context) -> Option<String> {
        let entity = context.get_type_info(self.id);
        context.get_docs(entity.def_id)
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

#[graphql_object]
#[graphql(context = Context)]
impl TypeInfo {
    fn entity_info(&self, context: &Context) -> Option<Entity> {
        let type_info = context.get_type_info(self.id);
        type_info.entity_info().map(|_| Entity { id: self.id })
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
        let infos = domain.type_infos().map(|info| TypeInfo {
            id: info.def_id,
            kind: match info.kind {
                TypeKind::Entity(_) => TypeKindEnum::Entity,
                TypeKind::Data(_) => TypeKindEnum::Data,
                TypeKind::Relationship(_) => TypeKindEnum::Relationship,
                TypeKind::Function(_) => TypeKindEnum::Function,
                TypeKind::Domain(_) => TypeKindEnum::Domain,
                TypeKind::Generator(_) => TypeKindEnum::Generator,
            },
        });
        if let Some(kind) = kind {
            infos.filter(|info| info.kind == kind).collect()
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
                    kind: match type_info.kind {
                        TypeKind::Entity(_) => TypeKindEnum::Entity,
                        TypeKind::Data(_) => TypeKindEnum::Data,
                        TypeKind::Relationship(_) => TypeKindEnum::Relationship,
                        TypeKind::Function(_) => TypeKindEnum::Function,
                        TypeKind::Domain(_) => TypeKindEnum::Domain,
                        TypeKind::Generator(_) => TypeKindEnum::Generator,
                    },
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
