use fnv::FnvHashSet;
use ontol_runtime::{
    ontology::ontol::{TextLikeType, ValueGenerator},
    property::{PropertyCardinality, ValueCardinality},
    DefId, RelId,
};
use tracing::{debug, instrument, trace};

use crate::{
    def::{Def, DefKind},
    error::CompileError,
    primitive::PrimitiveKind,
    properties::{Constructor, Property},
    relation::rel_def_meta,
    repr::repr_model::{ReprKind, ReprScalarKind, UnionBound},
    text_patterns::TextPatternSegment,
    types::{FormatType, Type},
    SourceSpan,
};

use super::TypeCheck;

#[derive(Debug)]
enum Action {
    /// Many(*) value cardinality between two entities are always considered optional
    AdjustEntityPropertyCardinality(DefId, RelId),
    RedefineAsPrimaryId {
        def_id: DefId,
        inherent_property_id: RelId,
        identifies_relationship_id: RelId,
    },
    CheckValueGenerator {
        relationship_id: RelId,
        generator_def_id: DefId,
        object_def_id: DefId,
        span: SourceSpan,
    },
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_domain_type_pre_repr(&mut self, def_id: DefId, _def: &Def) {
        let Some(table) = self.prop_ctx.properties_table_by_def_id(def_id) else {
            return;
        };

        let mut actions = vec![];

        for (rel_id, property) in table {
            trace!("check pre-repr {def_id:?} {rel_id:?} {property:?}");

            let meta = rel_def_meta(*rel_id, self.rel_ctx, self.defs);

            let object_properties = self
                .prop_ctx
                .properties_by_def_id(meta.relationship.object.0)
                .unwrap();

            // Check if the property is the primary id
            if let Some(id_relationship_id) = object_properties.identifies {
                let id_meta = rel_def_meta(id_relationship_id, self.rel_ctx, self.defs);

                if id_meta.relationship.object.0 == def_id {
                    debug!(
                        "redefine as primary id: {id_relationship_id:?} <-> inherent {:?}",
                        rel_id
                    );

                    actions.push(Action::RedefineAsPrimaryId {
                        def_id,
                        inherent_property_id: *rel_id,
                        identifies_relationship_id: id_relationship_id,
                    });
                }
            }
        }

        self.perform_actions(actions);
    }

    pub fn check_domain_type_post_repr(&mut self, def_id: DefId, _def: &Def) {
        let Some(properties) = self.prop_ctx.properties_by_def_id.get(&def_id) else {
            return;
        };
        let Some(table) = properties.table.as_ref() else {
            return;
        };

        let mut actions = vec![];
        let mut subject_relation_set: FnvHashSet<DefId> = Default::default();

        for (rel_id, property) in table {
            trace!("check post-repr {def_id:?} {rel_id:?} {property:?}");

            let meta = rel_def_meta(*rel_id, self.rel_ctx, self.defs);

            // Check that the same relation_def_id is not reused for subject properties
            if !subject_relation_set.insert(meta.relationship.relation_def_id) {
                CompileError::UnionInNamedRelationshipNotSupported
                    .span(self.rel_ctx.span(meta.rel_id))
                    .report(&mut self.errors);
            }

            let object_properties = self
                .prop_ctx
                .properties_by_def_id(meta.relationship.object.0)
                .unwrap();

            if properties.identified_by.is_some() && object_properties.identified_by.is_some() {
                if matches!(property.cardinality.1, ValueCardinality::List) {
                    let span = self.rel_ctx.span(meta.rel_id);
                    CompileError::EntityRelationshipCannotBeAList
                        .span(span)
                        .report(&mut self.errors);
                }

                actions.push(Action::AdjustEntityPropertyCardinality(def_id, *rel_id));
            }

            if let Some((generator_def_id, gen_span)) =
                self.misc_ctx.value_generators_unchecked.remove(rel_id)
            {
                actions.push(Action::CheckValueGenerator {
                    relationship_id: *rel_id,
                    generator_def_id,
                    object_def_id: meta.relationship.object.0,
                    span: gen_span,
                });
            }

            if let DefKind::Type(_) = self.defs.def_kind(meta.relationship.relation_def_id) {
                let relation_repr_kind = self
                    .repr_ctx
                    .get_repr_kind(&meta.relationship.relation_def_id)
                    .unwrap();

                match relation_repr_kind {
                    ReprKind::Unit => {
                        let object = meta.relationship.object;
                        let object_repr_kind = self.repr_ctx.get_repr_kind(&object.0).unwrap();

                        if !matches!(object_repr_kind, ReprKind::Union(_, UnionBound::Struct)) {
                            CompileError::FlattenedRelationshipObjectMustBeStructUnion
                                .span(object.1)
                                .report(&mut self.errors);
                        }
                    }
                    ReprKind::Scalar(_, ReprScalarKind::TextConstant(_), _) => {}
                    _ => {
                        CompileError::InvalidRelationType
                            .span(meta.relationship.relation_span)
                            .report(&mut self.errors);
                    }
                }
            }
        }

        self.perform_actions(actions);
    }

    fn perform_actions(&mut self, actions: Vec<Action>) {
        for action in actions {
            trace!("perform action {action:?}");
            match action {
                Action::AdjustEntityPropertyCardinality(def_id, property_id) => {
                    let properties = self.prop_ctx.properties_by_def_id_mut(def_id);
                    if let Some(table) = &mut properties.table {
                        let cardinality = table.get_mut(&property_id).unwrap();
                        adjust_entity_prop_cardinality(cardinality);
                    }
                }
                Action::RedefineAsPrimaryId {
                    def_id,
                    inherent_property_id,
                    identifies_relationship_id,
                } => {
                    self.misc_ctx
                        .inherent_id_map
                        .insert(identifies_relationship_id, inherent_property_id);
                    let properties = self.prop_ctx.properties_by_def_id_mut(def_id);

                    if let Some(table) = &mut properties.table {
                        table.insert(
                            inherent_property_id,
                            Property {
                                cardinality: (
                                    PropertyCardinality::Mandatory,
                                    ValueCardinality::Unit,
                                ),
                                is_entity_id: true,
                                is_edge_partial: false,
                            },
                        );
                    }
                }
                Action::CheckValueGenerator {
                    relationship_id,
                    generator_def_id,
                    object_def_id,
                    span,
                } => match self.determine_value_generator(generator_def_id, object_def_id) {
                    Ok(value_generator) => {
                        self.misc_ctx
                            .value_generators
                            .insert(relationship_id, value_generator);
                    }
                    Err(_) => {
                        let object_ty = self.def_ty_ctx.def_table.get(&object_def_id).unwrap();
                        CompileError::CannotGenerateValue(format!(
                            "{}",
                            FormatType::new(object_ty, self.defs, self.primitives)
                        ))
                        .span(span)
                        .report(self);
                    }
                },
            }
        }
    }

    #[instrument(level = "trace", skip(self, generator_def_id))]
    fn determine_value_generator(
        &self,
        generator_def_id: DefId,
        object_def_id: DefId,
    ) -> Result<ValueGenerator, ()> {
        let properties = self.prop_ctx.properties_by_def_id.get(&object_def_id);
        let repr = self
            .repr_ctx
            .repr_table
            .get(&object_def_id)
            .unwrap_or_else(|| {
                panic!(
                    "No repr for {object_def_id:?} {:?}",
                    self.defs.table.get(&object_def_id)
                );
            });

        let scalar_def_id = match &repr.kind {
            ReprKind::Scalar(scalar_def_id, _, _) => *scalar_def_id,
            ReprKind::FmtStruct(Some((_rel_id, attr_def_id))) => *attr_def_id,
            _ => return Err(()),
        };

        let generators = &self.primitives.generators;

        match generator_def_id {
            _ if generator_def_id == generators.auto => {
                match self.def_ty_ctx.def_table.get(&scalar_def_id) {
                    Some(Type::Primitive(PrimitiveKind::Serial, _)) => {
                        Ok(ValueGenerator::Autoincrement)
                    }
                    Some(Type::Primitive(PrimitiveKind::Text, _)) => Ok(ValueGenerator::Uuid),
                    Some(Type::TextLike(_, TextLikeType::Uuid)) => Ok(ValueGenerator::Uuid),
                    _ => match properties.map(|p| &p.constructor) {
                        Some(Constructor::TextFmt(segment)) => {
                            self.auto_generator_for_text_pattern_segment(segment)
                        }
                        _ => Err(()),
                    },
                }
            }
            _ if generator_def_id == generators.create_time => {
                match self.def_ty_ctx.def_table.get(&scalar_def_id) {
                    Some(Type::TextLike(_, TextLikeType::DateTime)) => {
                        Ok(ValueGenerator::CreatedAtTime)
                    }
                    _ => Err(()),
                }
            }
            _ if generator_def_id == generators.update_time => {
                match self.def_ty_ctx.def_table.get(&scalar_def_id) {
                    Some(Type::TextLike(_, TextLikeType::DateTime)) => {
                        Ok(ValueGenerator::UpdatedAtTime)
                    }
                    _ => Err(()),
                }
            }
            _ => Err(()),
        }
    }

    fn auto_generator_for_text_pattern_segment(
        &self,
        segment: &TextPatternSegment,
    ) -> Result<ValueGenerator, ()> {
        match segment {
            TextPatternSegment::AnyString => Ok(ValueGenerator::Uuid),
            TextPatternSegment::Concat(segments) => {
                let mut output_generator = None;
                for concat_segment in segments {
                    if let Ok(generator) =
                        self.auto_generator_for_text_pattern_segment(concat_segment)
                    {
                        if output_generator.is_some() {
                            return Err(());
                        }
                        output_generator = Some(generator);
                    }
                }

                output_generator.ok_or(())
            }
            TextPatternSegment::Attribute { type_def_id, .. } => {
                self.determine_value_generator(self.primitives.generators.auto, *type_def_id)
            }
            _ => Err(()),
        }
    }
}

fn adjust_entity_prop_cardinality(property: &mut Property) {
    if let ValueCardinality::IndexSet = property.cardinality.1 {
        property.cardinality.0 = PropertyCardinality::Optional;
    }
}
