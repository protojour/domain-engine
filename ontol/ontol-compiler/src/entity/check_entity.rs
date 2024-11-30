use ontol_runtime::{ontology::domain::ExtendedEntityInfo, DefId};

use crate::{
    def::{DefKind, TypeDef, TypeDefFlags},
    relation::rel_def_meta,
    CompileError, Compiler,
};

impl Compiler<'_> {
    /// Check entity-related relationships.
    /// This is also run for non-entities.
    pub fn check_entity(&mut self, def_id: DefId) {
        let identified_by = self
            .prop_ctx
            .properties_by_def_id
            .get(&def_id)
            .and_then(|properties| properties.identified_by);

        let is_entity = identified_by.is_some();

        let mut info = ExtendedEntityInfo::default();

        if let Some(order_rels) = self.misc_ctx.order_relationships.remove(&def_id) {
            if !is_entity {
                for order_rel in &order_rels {
                    let meta = rel_def_meta(*order_rel, &self.rel_ctx, &self.defs);
                    CompileError::RelationSubjectMustBeEntity
                        .span(meta.relationship.subject.1)
                        .report(self);
                }
            } else {
                let entity_span = self.defs.def_span(def_id);
                let order_union = self.defs.add_def(
                    DefKind::Type(TypeDef {
                        ident: None,
                        rel_type_for: None,
                        flags: TypeDefFlags::PUBLIC,
                    }),
                    def_id.domain_index(),
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

                let domain_def_id = self.domain_def_ids.get(&def_id.domain_index()).unwrap();
                self.namespaces.add_anonymous(*domain_def_id, order_union);

                self.type_check().check_def(order_union);

                info.order_union = Some(order_union);
            }
        }

        if is_entity {
            self.entity_ctx.entities.insert(def_id, info);
        }
    }
}
