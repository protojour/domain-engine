use ontol_runtime::{
    ontology::domain::{EntityOrder, FieldPath},
    query::order::Direction,
    DefId, RelationshipId,
};
use tracing::{debug, info};

use crate::{
    def::{rel_def_meta, DefKind, RelParams},
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
    ) -> Option<(DefId, EntityOrder)> {
        let package_id = entity_def_id.package_id();
        let meta = rel_def_meta(order_relationship, &self.defs);
        let object = meta.relationship.object;
        let rel_span = *meta.relationship.span;

        match self.repr_ctx.get_repr_kind(&object.0) {
            Some(ReprKind::Scalar(scalar_def_id, ReprScalarKind::TextConstant(_), _)) => {
                if object.0.package_id() != package_id
                    || !matches!(self.defs.def_kind(*scalar_def_id), DefKind::TextLiteral(_))
                {
                    CompileError::EntityOrderMustBeSymbolInThisDomain
                        .span(object.1)
                        .report(self);
                    return None;
                }
            }
            _other => {
                CompileError::EntityOrderMustBeSymbolInThisDomain
                    .span(object.1)
                    .report(self);
                return None;
            }
        }

        let entity_order = match meta.relationship.rel_params {
            RelParams::Type(params_def_id) => {
                self.check_order_params(entity_def_id, params_def_id, *meta.relationship.span)?
            }
            _ => {
                CompileError::EntityOrderMustSpecifyParameters
                    .span(*meta.relationship.span)
                    .report(self);
                return None;
            }
        };

        self.thesaurus
            .insert_domain_is(order_union, TypeRelation::SubVariant, object.0, rel_span);

        Some((object.0, entity_order))
    }

    fn check_order_params(
        &mut self,
        entity_def_id: DefId,
        params_def_id: DefId,
        rel_span: SourceSpan,
    ) -> Option<EntityOrder> {
        let Some(properties) = self.rel_ctx.properties_by_def_id(params_def_id) else {
            CompileError::EntityOrderMustSpecifyParameters
                .span(rel_span)
                .report(self);
            return None;
        };
        let Constructor::Sequence(sequence) = &properties.constructor else {
            CompileError::TODO("must specify a tuple of fields")
                .span(rel_span)
                .report(self);
            return None;
        };

        if sequence.is_infinite() {
            CompileError::TODO("order tuple sequence must be finite")
                .span(rel_span)
                .report(self);
            return None;
        }

        let mut tuple: Vec<FieldPath> = vec![];

        for (_index, relationship) in sequence.elements() {
            let Some(relationship_id) = relationship else {
                CompileError::TODO("missing relationship")
                    .span(rel_span)
                    .report(&mut self.errors);
                continue;
            };

            let meta = rel_def_meta(relationship_id, &self.defs);
            match self.defs.def_kind(meta.relationship.object.0) {
                DefKind::TextLiteral(literal) => {
                    match self.parse_order_field(
                        literal,
                        *meta.relationship.span,
                        // self.defs.def_span(meta.relationship.sp),
                        entity_def_id,
                    ) {
                        Ok(path) => {
                            tuple.push(path);
                        }
                        Err(errors) => {
                            self.errors.extend(errors);
                        }
                    }
                }
                _ => {
                    CompileError::TODO("invalid ordering field")
                        .span(*meta.relationship.span)
                        .report(&mut self.errors);
                }
            }
        }

        let direction = match self.rel_ctx.direction_relationships.get(&params_def_id) {
            Some((rel_id, dir_def_id)) => {
                if dir_def_id == &self.primitives.symbols.ascending {
                    Direction::Ascending
                } else if dir_def_id == &self.primitives.symbols.descending {
                    Direction::Descending
                } else {
                    let span = self.defs.def_span(rel_id.0);
                    CompileError::TODO("invalid direction")
                        .span(span)
                        .report(self);
                    return None;
                }
            }
            None => Direction::Ascending,
        };

        info!("ordering: {tuple:?} {direction:?}");

        Some(EntityOrder {
            tuple: tuple.into(),
            direction,
        })
    }

    fn parse_order_field(
        &self,
        field: &str,
        field_span: SourceSpan,
        mut def_id: DefId,
    ) -> Result<FieldPath, CompileErrors> {
        let mut output: Vec<RelationshipId> = vec![];
        let mut errors = CompileErrors::default();

        for field_segment in field.split('.') {
            let Ok((rel_id, next_def_id)) = self.lookup_order_field(def_id, field_segment) else {
                CompileError::TODO(format!("no such field: `{field_segment}`"))
                    .span(field_span)
                    .report(&mut errors);
                break;
            };

            output.push(rel_id);
            def_id = next_def_id;
        }

        if errors.errors.is_empty() {
            Ok(FieldPath(output.into()))
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

        let Some(table) = self.rel_ctx.properties_table_by_def_id(parent_def_id) else {
            debug!("order field: no properties table for {parent_def_id:?}");
            return Err(());
        };

        for (rel_id, _) in table {
            let meta = rel_def_meta(*rel_id, &self.defs);

            if meta.relationship.relation_def_id == *literal_def_id {
                return Ok((*rel_id, meta.relationship.object.0));
            }
        }

        debug!("order field: property not found");

        Err(())
    }
}
