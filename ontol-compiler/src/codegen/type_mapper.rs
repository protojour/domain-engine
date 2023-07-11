use ontol_runtime::{DefId, MapKey};
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
    /// When mapping a type using the `is` relation, the map key is the inner type
    /// and the alias is the outer type.
    pub alias: Option<DefId>,
}

impl MapInfo {
    pub fn unique_key(&self) -> MapKey {
        if let Some(alias) = self.alias {
            MapKey {
                def_id: alias,
                seq: self.key.seq,
            }
        } else {
            self.key
        }
    }
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
        let first = self.find_mapping_info(first)?;
        let second = self.find_mapping_info(second)?;

        Some(MapKeyPair::new(first.unique_key(), second.unique_key()))
    }

    pub fn find_mapping_info(&self, ty: TypeRef) -> Option<MapInfo> {
        match ty {
            Type::Domain(def_id) => Some(MapInfo {
                key: MapKey {
                    def_id: *def_id,
                    seq: false,
                },
                alias: None,
            }),
            Type::Anonymous(def_id) => match self.relations.properties_by_def_id(*def_id) {
                Some(Properties {
                    constructor: Constructor::Value(relationship_id, ..),
                    ..
                }) => {
                    let meta = self
                        .defs
                        .lookup_relationship_meta(*relationship_id)
                        .expect("BUG: problem getting anonymous relationship meta");

                    Some(MapInfo {
                        key: MapKey {
                            def_id: meta.relationship.object.0.def_id,
                            seq: false,
                        },
                        alias: Some(*def_id),
                    })
                }
                _ => Some(MapInfo {
                    key: MapKey {
                        def_id: *def_id,
                        seq: false,
                    },
                    alias: None,
                }),
            },
            Type::Seq(_, val_ty) => {
                let def_id = val_ty.get_single_def_id()?;
                Some(MapInfo {
                    key: MapKey { def_id, seq: true },
                    alias: None,
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
