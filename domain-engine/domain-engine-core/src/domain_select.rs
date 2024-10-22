use ontol_runtime::{
    ontology::{
        aspects::DefsAspect,
        domain::{DataRelationshipKind, DataRelationshipTarget, Def, DefRepr},
    },
    query::select::{Select, StructSelect},
    DefId,
};

/// Produce a Select matching a domain def
pub fn domain_select(def_id: DefId, ontology_defs: &DefsAspect) -> Select {
    let builder = DomainSelectBuilder { ontology_defs };
    builder.domain_select(def_id, true)
}

pub fn domain_select_no_edges(def_id: DefId, ontology_defs: &DefsAspect) -> Select {
    let builder = DomainSelectBuilder { ontology_defs };
    builder.domain_select(def_id, false)
}

struct DomainSelectBuilder<'o> {
    ontology_defs: &'o DefsAspect,
}

impl<'o> DomainSelectBuilder<'o> {
    fn domain_select(&self, def_id: DefId, follow_edge: bool) -> Select {
        let def = self.ontology_defs.def(def_id);

        match def.repr() {
            Some(DefRepr::Struct) => Select::Struct(self.domain_struct_select(def, follow_edge)),
            _ => Select::Unit,
        }
    }

    fn domain_struct_select(&self, def: &Def, follow_edge: bool) -> StructSelect {
        let mut struct_select = StructSelect {
            def_id: def.id,
            properties: Default::default(),
        };

        for (prop_id, rel_info) in &def.data_relationships {
            let next_follow_edge = match &rel_info.kind {
                DataRelationshipKind::Edge(_) => {
                    if follow_edge {
                        false
                    } else {
                        continue;
                    }
                }
                DataRelationshipKind::Id | DataRelationshipKind::Tree(_) => follow_edge,
            };

            match &rel_info.target {
                DataRelationshipTarget::Unambiguous(def_id) => {
                    struct_select
                        .properties
                        .insert(*prop_id, self.domain_select(*def_id, next_follow_edge));
                }
                DataRelationshipTarget::Union(union_def_id) => {
                    let variants = self.ontology_defs.union_variants(*union_def_id);
                    let select = if variants.iter().all(|def_id| {
                        matches!(
                            self.ontology_defs.def(*def_id).repr(),
                            Some(DefRepr::Struct)
                        )
                    }) {
                        Select::StructUnion(
                            *union_def_id,
                            variants
                                .iter()
                                .map(|def_id| {
                                    self.domain_struct_select(
                                        self.ontology_defs.def(*def_id),
                                        next_follow_edge,
                                    )
                                })
                                .collect(),
                        )
                    } else {
                        self.domain_select(*union_def_id, next_follow_edge)
                    };

                    struct_select.properties.insert(*prop_id, select);
                }
            }
        }

        struct_select
    }
}
