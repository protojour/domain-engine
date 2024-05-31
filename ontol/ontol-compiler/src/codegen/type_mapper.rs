use ontol_runtime::{MapDef, MapDefFlags};
use tracing::warn;

use crate::{
    def::Defs,
    map::UndirectedMapKey,
    relation::RelCtx,
    repr::{repr_ctx::ReprCtx, repr_model::ReprKind},
    types::{Type, TypeRef},
};

#[derive(Debug)]
pub struct MapInfo {
    pub map_def: MapDef,
    pub punned: Option<MapDef>,
    pub anonymous: bool,
}

#[derive(Clone, Copy)]
pub struct TypeMapper<'c, 'm> {
    pub relations: &'c RelCtx,
    pub defs: &'c Defs<'m>,
    pub repr_ctx: &'c ReprCtx,
}

impl<'c, 'm> TypeMapper<'c, 'm> {
    pub fn new(relations: &'c RelCtx, defs: &'c Defs<'m>, repr_ctx: &'c ReprCtx) -> Self {
        Self {
            relations,
            defs,
            repr_ctx,
        }
    }

    pub fn find_map_key_pair(&self, types: [TypeRef; 2]) -> Option<UndirectedMapKey> {
        let first = self.find_domain_mapping_info(types[0])?;
        let second = self.find_domain_mapping_info(types[1])?;

        Some(UndirectedMapKey::new([first.map_def, second.map_def]))
    }

    pub fn find_domain_mapping_info(&self, ty: TypeRef) -> Option<MapInfo> {
        let anonymous = matches!(ty, Type::Anonymous(_));

        match ty {
            Type::Domain(def_id) | Type::Anonymous(def_id) => {
                let repr = self.repr_ctx.get_repr_kind(def_id).unwrap();

                match repr {
                    ReprKind::Scalar(scalar_def_id, _, _) => Some(MapInfo {
                        map_def: MapDef {
                            def_id: *def_id,
                            flags: MapDefFlags::empty(),
                        },
                        punned: if scalar_def_id != def_id {
                            Some(MapDef {
                                def_id: *scalar_def_id,
                                flags: MapDefFlags::empty(),
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
                                map_def: MapDef {
                                    def_id: *def_id,
                                    flags: MapDefFlags::empty(),
                                },
                                punned: Some(MapDef {
                                    def_id: *member_def_id,
                                    flags: MapDefFlags::empty(),
                                }),
                                anonymous,
                            })
                        } else {
                            todo!("Unhandled({def_id:?}): {repr:?}");
                        }
                    }
                    _ => Some(MapInfo {
                        map_def: MapDef {
                            def_id: *def_id,
                            flags: MapDefFlags::empty(),
                        },
                        punned: None,
                        anonymous,
                    }),
                }
            }
            Type::Seq(_, val_ty) => {
                let def_id = val_ty.get_single_def_id()?;
                Some(MapInfo {
                    map_def: MapDef {
                        def_id,
                        flags: MapDefFlags::SEQUENCE,
                    },
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

impl<'c, 'm> AsRef<RelCtx> for TypeMapper<'c, 'm> {
    fn as_ref(&self) -> &RelCtx {
        self.relations
    }
}

impl<'c, 'm> AsRef<Defs<'m>> for TypeMapper<'c, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        self.defs
    }
}
