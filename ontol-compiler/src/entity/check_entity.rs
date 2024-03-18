use ontol_runtime::{ontology::domain::ExtendedEntityInfo, DefId};

use crate::{
    def::{DefKind, LookupRelationshipMeta, TypeDef, TypeDefFlags},
    CompileError, Compiler,
};

impl<'m> Compiler<'m> {
    /// Check entity-related relationships.
    /// This is also run for non-entities.
    pub fn check_entity(&mut self, def_id: DefId) {
        let identified_by = self
            .relations
            .properties_by_def_id
            .get(&def_id)
            .and_then(|properties| properties.identified_by);

        let is_entity = identified_by.is_some();

        let mut info = ExtendedEntityInfo::default();

        if let Some(order_rels) = self.relations.order_relationships.remove(&def_id) {
            if !is_entity {
                for order_rel in &order_rels {
                    let meta = self.defs.relationship_meta(*order_rel);
                    self.errors.error(
                        CompileError::RelationSubjectMustBeEntity,
                        &meta.relationship.subject.1,
                    );
                }
            } else {
                let entity_span = self.defs.def_span(def_id);
                let order_union = self.defs.add_def(
                    DefKind::Type(TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::PUBLIC,
                    }),
                    def_id.package_id(),
                    entity_span,
                );

                for order_rel in order_rels {
                    if let Some((order_def_id, entity_order)) =
                        self.check_order(def_id, order_rel, order_union)
                    {
                        info.order_table.insert(order_def_id, entity_order);
                    }
                }

                // This type is introduced very late, after the main repr check.
                // But before the union check!
                self.type_check().repr_check(order_union).check_repr_root();

                info.order_union = Some(order_union);
            }
        }

        if is_entity {
            self.entity_ctx.entities.insert(def_id, info);
        }
    }
}
