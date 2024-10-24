use ontol_runtime::{
    interface::{
        http_json::{Endpoint, HttpJson, HttpKeyedResource, HttpResource},
        serde::{SerdeDef, SerdeModifier},
    },
    ontology::{
        aspects::DefsAspect,
        domain::{DataRelationshipKind, DataTreeRepr, DefKind},
        ontol::TextConstant,
    },
    DefId, DomainIndex, PropId,
};

use crate::repr::repr_model::ReprKind;

use super::serde::{serde_generator::SerdeGenerator, SerdeKey};

pub fn generate_httpjson_interface(
    domain_index: DomainIndex,
    partial_defs: &DefsAspect,
    serde_gen: &mut SerdeGenerator,
) -> Option<HttpJson> {
    let domain = partial_defs.domain_by_index(domain_index).unwrap();

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

                let mut keyed: Vec<HttpKeyedResource> = vec![];

                if let Some(id_rel_info) = def.data_relationships.get(&entity.id_prop) {
                    let mut crdts: Vec<(PropId, TextConstant)> = vec![];

                    for (prop_id, rel_info) in &def.data_relationships {
                        if let DataRelationshipKind::Tree(DataTreeRepr::Crdt) = &rel_info.kind {
                            crdts.push((*prop_id, rel_info.name));
                        }
                    }

                    keyed.push(HttpKeyedResource {
                        key_name: id_rel_info.name,
                        key_operator_addr: entity.id_operator_addr,
                        key_prop_id: entity.id_prop,
                        get: Some(Endpoint {}),
                        put: None,
                        crdts,
                    });
                }

                http_json.resources.push(HttpResource {
                    def_id: def.id,
                    name: entity.ident,
                    operator_addr: addr,
                    put: Some(Endpoint {}),
                    post: Some(Endpoint {}),
                    keyed,
                });
            }
            DefKind::Data(_) => {
                let Some(ident) = def.ident() else {
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
                        def_id: def.id,
                        name: ident,
                        operator_addr: addr,
                        put: Some(Endpoint {}),
                        post: None,
                        keyed: vec![],
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
