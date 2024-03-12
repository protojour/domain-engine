use ontol_runtime::{DefId, RelationshipId};

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    repr::repr_model::{ReprKind, ReprScalarKind},
    CompileError,
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    /// Check entity-related relationships.
    /// This is also run for non-entities.
    pub fn check_entity_post_seal(&mut self, def_id: DefId) {
        let identified_by = self
            .relations
            .properties_by_def_id
            .get(&def_id)
            .and_then(|properties| properties.identified_by);

        if let Some(orderings) = self.relations.order_relationships.remove(&def_id) {
            for ordering in orderings {
                if identified_by.is_some() {
                    self.check_ordering(def_id, ordering);
                } else {
                    let meta = self.defs.relationship_meta(ordering);
                    self.errors.error(
                        CompileError::RelationSubjectMustBeEntity,
                        &meta.relationship.subject.1,
                    );
                }
            }
        }
    }

    fn check_ordering(&mut self, entity_def_id: DefId, order_relationship: RelationshipId) {
        let package_id = entity_def_id.package_id();
        let meta = self.defs.relationship_meta(order_relationship);
        let object = meta.relationship.object;

        match self.seal_ctx.get_repr_kind(&object.0) {
            Some(ReprKind::Scalar(scalar_def_id, ReprScalarKind::Text, _)) => {
                if object.0.package_id() != package_id
                    || !matches!(self.defs.def_kind(*scalar_def_id), DefKind::TextLiteral(_))
                {
                    self.errors
                        .error(CompileError::EntityOrderMustBeSymbolInThisDomain, &object.1);
                    return;
                }
            }
            _other => {
                self.errors
                    .error(CompileError::EntityOrderMustBeSymbolInThisDomain, &object.1);
                return;
            }
        }

        match meta.relationship.rel_params {
            RelParams::Type(_) => {}
            _ => {
                self.errors.error(
                    CompileError::EntityOrderMustSpecifyParameters,
                    meta.relationship.span,
                );
            }
        }
    }
}
