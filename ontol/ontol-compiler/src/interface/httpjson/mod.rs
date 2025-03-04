use std::collections::BTreeMap;

use indexmap::IndexMap;
use ontol_runtime::{
    DefId, DomainIndex, MapDefFlags, MapKey, PropId,
    interface::{
        http_json::{
            Endpoint, HttpDefResource, HttpJson, HttpKeyedResource, HttpMapGetResource,
            HttpResource,
        },
        serde::{SerdeDef, SerdeModifier},
    },
    ontology::{
        aspects::DefsAspect,
        domain::{DataRelationshipKind, DataTreeRepr, DefKind},
        ontol::TextConstant,
    },
};

use crate::{codegen::task::CodeCtx, repr::repr_model::ReprKind};

use super::serde::{SerdeKey, serde_generator::SerdeGenerator};

pub fn generate_httpjson_interface(
    domain_index: DomainIndex,
    partial_defs: &DefsAspect,
    map_namespace: Option<&IndexMap<&str, DefId>>,
    code_ctx: &CodeCtx,
    serde_gen: &mut SerdeGenerator,
) -> Option<HttpJson> {
    let domain = partial_defs.domain_by_index(domain_index).unwrap();

    let contains_entities = domain.defs().any(|def| def.entity().is_some());
    if !contains_entities {
        return None;
    }

    let mut http_json = HttpJson { resources: vec![] };

    let mut named_maps: BTreeMap<TextConstant, MapKey> = Default::default();

    if let Some(map_namespace) = map_namespace {
        // Register named maps in the user-specified order (using the IndexMap from the namespace)
        for name in map_namespace.keys() {
            let name_constant = serde_gen.str_ctx.intern_constant(name);

            if let Some(map_key) = code_ctx
                .result_named_downmaps
                .get(&(domain_index, name_constant))
            {
                named_maps.insert(name_constant, *map_key);
            }
        }
    }

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

                http_json.resources.push(HttpResource::Def(HttpDefResource {
                    def_id: def.id,
                    name: entity.ident,
                    operator_addr: addr,
                    get: make_map_get_resource(entity.ident, &mut named_maps, serde_gen),
                    put: Some(Endpoint {}),
                    post: Some(Endpoint {}),
                    keyed,
                }));
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

                    http_json.resources.push(HttpResource::Def(HttpDefResource {
                        def_id: def.id,
                        name: ident,
                        operator_addr: addr,
                        get: make_map_get_resource(ident, &mut named_maps, serde_gen),
                        put: Some(Endpoint {}),
                        post: None,
                        keyed: vec![],
                    }));
                }
            }
            _ => {}
        }
    }

    for ident in named_maps.keys().copied().collect::<Vec<_>>() {
        if let Some(list_resource) = make_map_get_resource(ident, &mut named_maps, serde_gen) {
            http_json
                .resources
                .push(HttpResource::MapGet(list_resource));
        }
    }

    Some(http_json)
}

fn make_map_get_resource(
    ident: TextConstant,
    named_maps: &mut BTreeMap<TextConstant, MapKey>,
    serde_gen: &mut SerdeGenerator,
) -> Option<HttpMapGetResource> {
    let map_key = named_maps.remove(&ident)?;

    let mut serde_modifier = SerdeModifier::json_default();

    if map_key.output.flags.contains(MapDefFlags::SEQUENCE) {
        serde_modifier = serde_modifier.union(SerdeModifier::LIST);
    }

    let addr = serde_gen.gen_addr_greedy(SerdeKey::Def(SerdeDef::new(
        map_key.output.def_id,
        SerdeModifier::json_default().union(serde_modifier),
    )))?;

    Some(HttpMapGetResource {
        name: ident,
        output_operator_addr: addr,
        map_key,
    })
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
