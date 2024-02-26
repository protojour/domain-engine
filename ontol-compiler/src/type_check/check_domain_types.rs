use fnv::FnvHashSet;
use ontol_runtime::{
    ontology::{PropertyCardinality, ValueCardinality},
    smart_format,
    text_like_types::TextLikeType,
    value::PropertyId,
    value_generator::ValueGenerator,
    DefId, RelationshipId, Role,
};
use tracing::{debug, instrument, trace};

use crate::{
    def::{Def, LookupRelationshipMeta},
    error::CompileError,
    primitive::PrimitiveKind,
    relation::{Constructor, Property, TypeRelation},
    repr::repr_model::ReprKind,
    text_patterns::TextPatternSegment,
    types::{FormatType, Type},
    SourceSpan,
};

use super::TypeCheck;

#[derive(Debug)]
enum Action {
    ReportNonEntityInObjectRelationship(DefId, RelationshipId),
    /// Many(*) value cardinality between two entities are always considered optional
    AdjustEntityPropertyCardinality(DefId, PropertyId),
    RedefineAsPrimaryId {
        def_id: DefId,
        inherent_property_id: PropertyId,
        identifies_relationship_id: RelationshipId,
    },
    CheckValueGenerator {
        relationship_id: RelationshipId,
        generator_def_id: DefId,
        object_def_id: DefId,
        span: SourceSpan,
    },
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_domain_type_pre_repr(&mut self, def_id: DefId, _def: &Def) -> Option<()> {
        let properties = self.relations.properties_by_def_id.get(&def_id)?;
        let table = properties.table.as_ref()?;

        let mut actions = vec![];

        for (property_id, cardinality) in table {
            trace!("check pre-repr {def_id:?} {property_id:?} {cardinality:?}");

            match property_id.role {
                Role::Subject => {
                    let meta = self.defs.relationship_meta(property_id.relationship_id);

                    let object_properties = self
                        .relations
                        .properties_by_def_id(meta.relationship.object.0)
                        .unwrap();

                    // Check if the property is the primary id
                    if let Some(id_relationship_id) = object_properties.identifies {
                        let id_meta = self.defs.relationship_meta(id_relationship_id);

                        if id_meta.relationship.object.0 == def_id {
                            debug!(
                                "redefine as primary id: {id_relationship_id:?} <-> inherent {:?}",
                                property_id.relationship_id
                            );

                            actions.push(Action::RedefineAsPrimaryId {
                                def_id,
                                inherent_property_id: *property_id,
                                identifies_relationship_id: id_relationship_id,
                            });
                        }
                    }
                }
                Role::Object => {}
            }
        }

        self.perform_actions(actions);

        None
    }

    pub fn check_domain_type_post_repr(&mut self, def_id: DefId, _def: &Def) -> Option<()> {
        let properties = self.relations.properties_by_def_id.get(&def_id)?;
        let ontology_mesh = self.relations.ontology_mesh.get(&def_id);
        let table = properties.table.as_ref()?;

        let mut actions = vec![];
        let mut subject_relation_set: FnvHashSet<DefId> = Default::default();

        for (property_id, cardinality) in table {
            trace!("check post-repr {def_id:?} {property_id:?} {cardinality:?}");

            match property_id.role {
                Role::Subject => {
                    let meta = self.defs.relationship_meta(property_id.relationship_id);

                    // Check that the same relation_def_id is not reused for subject properties
                    if !subject_relation_set.insert(meta.relationship.relation_def_id) {
                        let spanned_relationship_def = self
                            .defs
                            .get_spanned_def_kind(meta.relationship_id.0)
                            .unwrap();
                        self.errors.push(
                            CompileError::UnionInNamedRelationshipNotSupported
                                .spanned(spanned_relationship_def.span),
                        );
                    }

                    let object_properties = self
                        .relations
                        .properties_by_def_id(meta.relationship.object.0)
                        .unwrap();

                    if properties.identified_by.is_some()
                        && object_properties.identified_by.is_some()
                    {
                        actions.push(Action::AdjustEntityPropertyCardinality(
                            def_id,
                            *property_id,
                        ));
                    }

                    if let Some((generator_def_id, gen_span)) = self
                        .relations
                        .value_generators_unchecked
                        .remove(&property_id.relationship_id)
                    {
                        actions.push(Action::CheckValueGenerator {
                            relationship_id: property_id.relationship_id,
                            generator_def_id,
                            object_def_id: meta.relationship.object.0,
                            span: gen_span,
                        });
                    }
                }
                Role::Object => {
                    if properties.identified_by.is_none() {
                        match ontology_mesh {
                            Some(mesh)
                                if mesh.iter().any(|(is, _)| {
                                    matches!(
                                        is.rel,
                                        TypeRelation::SubVariant | TypeRelation::Subset
                                    )
                                }) =>
                            {
                                if let Some(table) = &properties.table {
                                    assert!(table.get(property_id).is_some());

                                    let all_entities =
                                        mesh.iter().filter(|(is, _)| is.is_sub()).any(|(is, _)| {
                                            let subject_properties = self
                                                .relations
                                                .properties_by_def_id(is.def_id)
                                                .unwrap();

                                            subject_properties.identified_by.is_some()
                                        });

                                    if all_entities {
                                        actions.push(Action::AdjustEntityPropertyCardinality(
                                            def_id,
                                            *property_id,
                                        ));
                                    }
                                } else {
                                    panic!("No table in value union");
                                }
                            }
                            _ => {
                                // it is illegal to specify an object property to something that is not an entity (has no id)
                                actions.push(Action::ReportNonEntityInObjectRelationship(
                                    def_id,
                                    property_id.relationship_id,
                                ));
                            }
                        }
                    } else {
                        let meta = self.defs.relationship_meta(property_id.relationship_id);
                        let subject_properties = self
                            .relations
                            .properties_by_def_id(meta.relationship.subject.0)
                            .unwrap();

                        if subject_properties.identified_by.is_some() {
                            actions.push(Action::AdjustEntityPropertyCardinality(
                                def_id,
                                *property_id,
                            ));
                        }
                    }
                }
            }
        }

        self.perform_actions(actions);

        None
    }

    fn perform_actions(&mut self, actions: Vec<Action>) {
        for action in actions {
            trace!("perform action {action:?}");
            match action {
                Action::ReportNonEntityInObjectRelationship(_def_id, relationship_id) => {
                    let meta = self.defs.relationship_meta(relationship_id);

                    self.error(
                        CompileError::NonEntityInReverseRelationship,
                        meta.relationship.span,
                    );
                }
                Action::AdjustEntityPropertyCardinality(def_id, property_id) => {
                    let properties = self.relations.properties_by_def_id_mut(def_id);
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
                    self.relations.inherent_id_map.insert(
                        identifies_relationship_id,
                        inherent_property_id.relationship_id,
                    );
                    let properties = self.relations.properties_by_def_id_mut(def_id);

                    if let Some(table) = &mut properties.table {
                        table.insert(
                            inherent_property_id,
                            Property {
                                cardinality: (
                                    PropertyCardinality::Mandatory,
                                    ValueCardinality::One,
                                ),
                                is_entity_id: true,
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
                        self.relations
                            .value_generators
                            .insert(relationship_id, value_generator);
                    }
                    Err(_) => {
                        let object_ty = self.def_types.table.get(&object_def_id).unwrap();
                        self.error(
                            CompileError::CannotGenerateValue(smart_format!(
                                "{}",
                                FormatType(object_ty, self.defs, self.primitives)
                            )),
                            &span,
                        );
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
        let properties = self.relations.properties_by_def_id.get(&object_def_id);
        let repr = self
            .seal_ctx
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
            _ => return Err(()),
        };

        let generators = &self.primitives.generators;

        match generator_def_id {
            _ if generator_def_id == generators.auto => {
                match self.def_types.table.get(&scalar_def_id) {
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
                match self.def_types.table.get(&scalar_def_id) {
                    Some(Type::TextLike(_, TextLikeType::DateTime)) => {
                        Ok(ValueGenerator::CreatedAtTime)
                    }
                    _ => Err(()),
                }
            }
            _ if generator_def_id == generators.update_time => {
                match self.def_types.table.get(&scalar_def_id) {
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
            TextPatternSegment::Property { type_def_id, .. } => {
                self.determine_value_generator(self.primitives.generators.auto, *type_def_id)
            }
            _ => Err(()),
        }
    }
}

fn adjust_entity_prop_cardinality(property: &mut Property) {
    if let ValueCardinality::Many = property.cardinality.1 {
        property.cardinality.0 = PropertyCardinality::Optional;
    }
}
