use ontol_runtime::MapKey;
use tracing::warn;

use crate::{
    def::{Defs, LookupRelationshipMeta},
    relation::{Constructor, Properties, Relations},
    types::{Type, TypeRef},
};

use super::task::MapKeyPair;

#[derive(Debug)]
pub struct MapInfo {
    pub key: MapKey,
    pub punned: Option<MapKey>,
    pub anonymous: bool,
}

#[derive(Clone, Copy)]
pub struct TypeMapper<'c, 'm> {
    pub relations: &'c Relations,
    pub defs: &'c Defs<'m>,
}

impl<'c, 'm> TypeMapper<'c, 'm> {
    pub fn new(relations: &'c Relations, defs: &'c Defs<'m>) -> Self {
        Self { relations, defs }
    }

    pub fn find_map_key_pair(&self, first: TypeRef, second: TypeRef) -> Option<MapKeyPair> {
        let first = self.find_domain_mapping_info(first)?;
        let second = self.find_domain_mapping_info(second)?;

        Some(MapKeyPair::new(first.key, second.key))
    }

    pub fn find_domain_mapping_info(&self, ty: TypeRef) -> Option<MapInfo> {
        let anonymous = matches!(ty, Type::Anonymous(_));

        match ty {
            Type::Domain(def_id) | Type::Anonymous(def_id) => {
                match self.relations.properties_by_def_id(*def_id) {
                    Some(Properties {
                        constructor: Constructor::Alias(relationship_id, ..),
                        ..
                    }) => {
                        let meta = self
                            .defs
                            .lookup_relationship_meta(*relationship_id)
                            .expect("BUG: problem getting anonymous relationship meta");

                        Some(MapInfo {
                            key: MapKey {
                                def_id: *def_id,
                                seq: false,
                            },
                            punned: Some(MapKey {
                                def_id: meta.relationship.object.0.def_id,
                                seq: false,
                            }),
                            anonymous,
                        })
                    }
                    _ => Some(MapInfo {
                        key: MapKey {
                            def_id: *def_id,
                            seq: false,
                        },
                        punned: None,
                        anonymous,
                    }),
                }
            }
            Type::Seq(_, val_ty) => {
                let def_id = val_ty.get_single_def_id()?;
                Some(MapInfo {
                    key: MapKey { def_id, seq: true },
                    punned: None,
                    anonymous,
                })
            }
            other => {
                warn!("unable to get mapping key: {other:?}");
                None
            }
        }
    }
}

impl<'c, 'm> AsRef<Relations> for TypeMapper<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        self.relations
    }
}

impl<'c, 'm> AsRef<Defs<'m>> for TypeMapper<'c, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        self.defs
    }
}
