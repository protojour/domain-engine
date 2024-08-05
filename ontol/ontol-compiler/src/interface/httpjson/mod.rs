use ontol_runtime::{
    interface::{
        http_json::{Endpoint, HttpJson, HttpResource},
        serde::{SerdeDef, SerdeModifier},
    },
    ontology::{domain::DefKind, Ontology},
    DefId, PackageId,
};

use crate::repr::repr_model::ReprKind;

use super::serde::{serde_generator::SerdeGenerator, SerdeKey};

pub fn generate_httpjson_interface(
    package_id: PackageId,
    partial_ontology: &Ontology,
    serde_gen: &mut SerdeGenerator,
) -> Option<HttpJson> {
    let domain = partial_ontology.find_domain(package_id).unwrap();

    let contains_entities = domain.defs().any(|def| def.entity().is_some());
    if !contains_entities {
        return None;
    }

    let mut http_json = HttpJson { resources: vec![] };

    for def in domain.defs() {
        match &def.kind {
            DefKind::Entity(entity) => {
                let Some(addr) = serde_gen.gen_addr_greedy(SerdeKey::Def(SerdeDef::new(
                    def.id,
                    SerdeModifier::json_default(),
                ))) else {
                    continue;
                };

                http_json.resources.push(HttpResource {
                    name: entity.name,
                    operator_addr: addr,
                    put: Some(Endpoint {}),
                });
            }
            DefKind::Data(_) => {
                let Some(name) = def.name() else {
                    continue;
                };
                if is_entity_union(def.id, serde_gen) {
                    let Some(addr) = serde_gen.gen_addr_greedy(SerdeKey::Def(SerdeDef::new(
                        def.id,
                        SerdeModifier::json_default(),
                    ))) else {
                        continue;
                    };

                    http_json.resources.push(HttpResource {
                        name,
                        operator_addr: addr,
                        put: Some(Endpoint {}),
                    });
                }
            }
            _ => {}
        }
    }

    Some(http_json)
}

fn is_entity_union(def_id: DefId, serde_gen: &SerdeGenerator) -> bool {
    let members = match serde_gen.repr_ctx.get_repr_kind(&def_id) {
        Some(ReprKind::Union(members, _bound)) => members,
        _ => return false,
    };

    members.iter().copied().all(|(def_id, _)| {
        let Some(props) = serde_gen.prop_ctx.properties_by_def_id(def_id) else {
            return false;
        };

        props.identified_by.is_some()
    })
}
