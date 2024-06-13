use std::collections::BTreeMap;
use std::str::FromStr;

use crate::juniper::{self, graphql_object, graphql_value, FieldError, FieldResult};

use ontol_runtime::DefId;

use super::gql_dictionary;
use super::gql_dictionary::DefDictionaryEntry;
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

    fn domain(name: String, ctx: &Ctx) -> FieldResult<gql_domain::Domain> {
        let domain = ctx
            .domains()
            .find(|(_, d)| ctx.get_text_constant(d.unique_name()).to_string() == name);
        if let Some((id, _)) = domain {
            Ok(gql_domain::Domain { id: *id })
        } else {
            Err(FieldError::new(
                "Domain not found",
                graphql_value!({"internal_error": "Domain not found"}),
            ))
        }
    }

    fn domains(ctx: &Ctx) -> FieldResult<Vec<gql_domain::Domain>> {
        let mut domains = vec![];
        for (package_id, _ontology_domain) in ctx.domains() {
            let domain = gql_domain::Domain { id: *package_id };
            domains.push(domain);
        }
        Ok(domains)
    }

    fn def(def_id: String, ctx: &Ctx) -> FieldResult<gql_domain::TypeInfo> {
        if let Ok(def_id) = DefId::from_str(&def_id) {
            if let Some(type_info) = ctx.get_type_info_option(def_id) {
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

    fn def_dictionary(ctx: &Ctx) -> Vec<DefDictionaryEntry> {
        let mut dict: BTreeMap<String, Vec<gql_domain::TypeInfo>> = Default::default();

        for (_, domain) in ctx.domains() {
            for type_info in domain.type_infos() {
                if let Some(name) = type_info.name() {
                    let name = ctx[name].to_string();

                    dict.entry(name).or_default().push(gql_domain::TypeInfo {
                        id: type_info.def_id,
                    });
                }
            }
        }

        dict.into_iter()
            .map(|(name, type_infos)| gql_dictionary::DefDictionaryEntry {
                name,
                definitions: type_infos,
            })
            .collect()
    }
}
