use std::str::FromStr;

use crate::juniper::{self, graphql_object, graphql_value, FieldError, FieldResult};

use ontol_runtime::DefId;

use super::gql_domain;
use super::Ctx;

#[derive(Default)]
pub struct Query;

#[graphql_object]
#[graphql(context = Ctx)]
impl Query {
    fn api_version() -> &'static str {
        "1.0"
    }
    fn domain(name: String, context: &Ctx) -> FieldResult<gql_domain::Domain> {
        let ontology = &context.ontology;
        let domain = ontology
            .domains()
            .find(|(_, d)| ontology.get_text_constant(d.unique_name()).to_string() == name);
        if let Some((id, _)) = domain {
            Ok(gql_domain::Domain { id: *id })
        } else {
            Err(FieldError::new(
                "Domain not found",
                graphql_value!({"internal_error": "Domain not found"}),
            ))
        }
    }
    fn domains(context: &Ctx) -> FieldResult<Vec<gql_domain::Domain>> {
        let ontology = &context.ontology;
        let mut domains = vec![];
        for (package_id, _ontology_domain) in ontology.domains() {
            let domain = gql_domain::Domain { id: *package_id };
            domains.push(domain);
        }
        Ok(domains)
    }
    fn def(def_id: String, context: &Ctx) -> FieldResult<gql_domain::TypeInfo> {
        let ontology = &context.ontology;
        if let Ok(def_id) = DefId::from_str(&def_id) {
            if let Some(type_info) = ontology.get_type_info_option(def_id) {
                return Ok(gql_domain::TypeInfo {
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
