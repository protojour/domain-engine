use ontol_runtime::MapKey;
use tracing::warn;

use crate::{
    def::Defs,
    relation::Relations,
    type_check::{repr::repr_model::ReprKind, seal::SealedDefs},
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
    pub sealed_defs: &'c SealedDefs,
}

impl<'c, 'm> TypeMapper<'c, 'm> {
    pub fn new(relations: &'c Relations, defs: &'c Defs<'m>, sealed_defs: &'c SealedDefs) -> Self {
        Self {
            relations,
            defs,
            sealed_defs,
        }
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
                let repr = self.sealed_defs.repr_table.get(def_id).unwrap();

                match repr {
                    ReprKind::Scalar(scalar_def_id, _) => Some(MapInfo {
                        key: MapKey {
                            def_id: *def_id,
                            seq: false,
                        },
                        punned: if scalar_def_id != def_id {
                            Some(MapKey {
                                def_id: *scalar_def_id,
                                seq: false,
                            })
                        } else {
                            None
                        },
                        anonymous,
                    }),
                    ReprKind::StructIntersection(members) => {
                        if members.len() == 1 {
                            let (member_def_id, _) = members.iter().next().unwrap();

                            Some(MapInfo {
                                key: MapKey {
                                    def_id: *def_id,
                                    seq: false,
                                },
                                punned: Some(MapKey {
                                    def_id: *member_def_id,
                                    seq: false,
                                }),
                                anonymous,
                            })
                        } else {
                            todo!("Unhandled({def_id:?}): {repr:?}");
                        }
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
