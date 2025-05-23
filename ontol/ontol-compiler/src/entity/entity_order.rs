use ontol_parser::source::SourceSpan;
use ontol_runtime::{
    DefId, OntolDefTag, OntolDefTagExt, PropId,
    ontology::domain::{FieldPath, VertexOrder},
    query::order::Direction,
};
use tracing::{debug, info};

use crate::{
    CompileError, CompileErrors, Compiler,
    def::DefKind,
    properties::Constructor,
    relation::{RelId, RelParams, rel_def_meta},
    repr::repr_model::{ReprKind, ReprScalarKind},
    thesaurus::TypeRelation,
};

impl Compiler<'_> {
    /// Returns the object (symbol) of the order.
    /// Adds the order as a subvariant to the order union
    pub(super) fn check_order(
        &mut self,
        entity_def_id: DefId,
        order_relationship: RelId,
        order_union: DefId,
    ) -> Option<(DefId, VertexOrder)> {
        let domain_index = entity_def_id.domain_index();
        let meta = rel_def_meta(order_relationship, &self.rel_ctx, &self.defs);
        let object = meta.relationship.object;
        let rel_span = *meta.relationship.span;

        match self.repr_ctx.get_repr_kind(&object.0) {
            Some(ReprKind::Scalar(scalar_def_id, ReprScalarKind::TextConstant(_), _)) => {
                if object.0.domain_index() != domain_index
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
            RelParams::Def(params_def_id) => {
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
    ) -> Option<VertexOrder> {
        let Some(properties) = self.prop_ctx.properties_by_def_id(params_def_id) else {
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

            let meta = rel_def_meta(relationship_id, &self.rel_ctx, &self.defs);
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

        let direction = match self.misc_ctx.direction_relationships.get(&params_def_id) {
            Some((rel_id, dir_def_id)) => {
                if dir_def_id == &OntolDefTag::SymAscending.def_id() {
                    Direction::Ascending
                } else if dir_def_id == &OntolDefTag::SymDescending.def_id() {
                    Direction::Descending
                } else {
                    CompileError::TODO("invalid direction")
                        .span(self.rel_ctx.span(*rel_id))
                        .report(self);
                    return None;
                }
            }
            None => Direction::Ascending,
        };

        info!("ordering: {tuple:?} {direction:?}");

        Some(VertexOrder {
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
        let mut output: Vec<PropId> = vec![];
        let mut errors = CompileErrors::default();

        for field_segment in field.split('.') {
            let Ok((prop_id, next_def_id)) = self.lookup_order_field(def_id, field_segment) else {
                CompileError::TODO(format!("no such field: `{field_segment}`"))
                    .span(field_span)
                    .report(&mut errors);
                break;
            };

            output.push(prop_id);
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
    ) -> Result<(PropId, DefId), ()> {
        let Some(literal_def_id) = self.defs.text_literals.get(field_name) else {
            debug!("order field: no text literal for {field_name}");
            return Err(());
        };

        let Some(table) = self.prop_ctx.properties_table_by_def_id(parent_def_id) else {
            debug!("order field: no properties table for {parent_def_id:?}");
            return Err(());
        };

        for (prop_id, property) in table {
            let meta = rel_def_meta(property.rel_id, &self.rel_ctx, &self.defs);

            if meta.relationship.relation_def_id == *literal_def_id {
                return Ok((*prop_id, meta.relationship.object.0));
            }
        }

        debug!("order field: property not found");

        Err(())
    }
}
