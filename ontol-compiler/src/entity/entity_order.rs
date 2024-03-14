use ontol_runtime::{order::Direction, smart_format, DefId, RelationshipId};
use tracing::{debug, info};

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    relation::Constructor,
    repr::repr_model::{ReprKind, ReprScalarKind},
    thesaurus::TypeRelation,
    CompileError, CompileErrors, Compiler, SourceSpan,
};

impl<'m> Compiler<'m> {
    /// Returns the object (symbol) of the order.
    /// Adds the order as a subvariant to the order union
    pub(super) fn check_order(
        &mut self,
        entity_def_id: DefId,
        order_relationship: RelationshipId,
        order_union: DefId,
    ) {
        let package_id = entity_def_id.package_id();
        let meta = self.defs.relationship_meta(order_relationship);
        let object = meta.relationship.object;
        let rel_span = *meta.relationship.span;

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
            RelParams::Type(params_def_id) => {
                self.check_order_params(entity_def_id, params_def_id, *meta.relationship.span);
            }
            _ => {
                self.errors.error(
                    CompileError::EntityOrderMustSpecifyParameters,
                    meta.relationship.span,
                );
            }
        }

        self.thesaurus
            .insert_domain_is(order_union, TypeRelation::SubVariant, object.0, rel_span);
    }

    fn check_order_params(
        &mut self,
        entity_def_id: DefId,
        params_def_id: DefId,
        rel_span: SourceSpan,
    ) {
        let Some(properties) = self.relations.properties_by_def_id(params_def_id) else {
            self.errors
                .error(CompileError::EntityOrderMustSpecifyParameters, &rel_span);
            return;
        };
        let Constructor::Sequence(sequence) = &properties.constructor else {
            self.errors.error(
                CompileError::TODO("must specify a tuple of fields".into()),
                &rel_span,
            );
            return;
        };

        if sequence.is_infinite() {
            self.errors.error(
                CompileError::TODO("order tuple sequence must be finite".into()),
                &rel_span,
            );
            return;
        }

        let mut fields: Vec<Vec<RelationshipId>> = vec![];

        for (_index, relationship) in sequence.elements() {
            let Some(relationship_id) = relationship else {
                self.errors
                    .error(CompileError::TODO("missing relationship".into()), &rel_span);
                continue;
            };

            let meta = self.defs.relationship_meta(relationship_id);
            match self.defs.def_kind(meta.relationship.object.0) {
                DefKind::TextLiteral(literal) => {
                    match self.parse_order_field(
                        literal,
                        *meta.relationship.span,
                        // self.defs.def_span(meta.relationship.sp),
                        entity_def_id,
                    ) {
                        Ok(relationships) => {
                            fields.push(relationships);
                        }
                        Err(errors) => {
                            self.errors.extend(errors);
                        }
                    }
                }
                _ => {
                    self.errors.error(
                        CompileError::TODO("invalid ordering field".into()),
                        meta.relationship.span,
                    );
                }
            }
        }

        let direction = match self.relations.direction_relationships.get(&params_def_id) {
            Some((rel_id, dir_def_id)) => {
                if dir_def_id == &self.primitives.symbols.ascending {
                    Direction::Ascending
                } else if dir_def_id == &self.primitives.symbols.descending {
                    Direction::Descending
                } else {
                    let span = self.defs.def_span(rel_id.0);
                    self.errors
                        .error(CompileError::TODO("invalid direction".into()), &span);
                    return;
                }
            }
            None => Direction::Ascending,
        };

        info!("ordering: {fields:?} {direction:?}");
    }

    fn parse_order_field(
        &self,
        field: &str,
        field_span: SourceSpan,
        mut def_id: DefId,
    ) -> Result<Vec<RelationshipId>, CompileErrors> {
        let mut output: Vec<RelationshipId> = vec![];
        let mut errors = CompileErrors::default();

        for field_segment in field.split('.') {
            let Ok((relationship_id, next_def_id)) = self.lookup_order_field(def_id, field_segment)
            else {
                errors.error(
                    CompileError::TODO(smart_format!("no such field: `{field_segment}`")),
                    &field_span,
                );
                break;
            };

            output.push(relationship_id);
            def_id = next_def_id;
        }

        if errors.errors.is_empty() {
            Ok(output)
        } else {
            Err(errors)
        }
    }

    fn lookup_order_field(
        &self,
        parent_def_id: DefId,
        field_name: &str,
    ) -> Result<(RelationshipId, DefId), ()> {
        let Some(literal_def_id) = self.defs.text_literals.get(field_name) else {
            debug!("order field: no text literal for {field_name}");
            return Err(());
        };

        let Some(table) = self.relations.properties_table_by_def_id(parent_def_id) else {
            debug!("order field: no properties table for {parent_def_id:?}");
            return Err(());
        };

        for (property_id, _) in table {
            let meta = self.defs.relationship_meta(property_id.relationship_id);

            if meta.relationship.relation_def_id == *literal_def_id {
                return Ok((property_id.relationship_id, meta.relationship.object.0));
            }
        }

        debug!("order field: property not found");

        Err(())
    }
}
