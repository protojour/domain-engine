use fnv::FnvHashSet;
use ontol_runtime::{value::PropertyId, DefId, RelationId, RelationshipId, Role};
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Def, DefKind, PropertyCardinality, RelationKind, ValueCardinality},
    error::CompileError,
    relation::{Constructor, Property},
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
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_domain_types(&mut self) {
        for (def_id, def) in &self.defs.map {
            if let DefKind::Type(_) = &def.kind {
                self.check_domain_type_properties(*def_id, def);
            }
        }
    }

    fn check_domain_type_properties(&mut self, def_id: DefId, _def: &Def) -> Option<()> {
        let properties = self.relations.properties_by_type(def_id)?;
        let map = properties.map.as_ref()?;

        let mut actions = vec![];
        let mut subject_relation_set: FnvHashSet<RelationId> = Default::default();

        for (property_id, cardinality) in map {
            debug!("check {def_id:?} {property_id:?} {cardinality:?}");

            match property_id.role {
                Role::Subject => {
                    let meta = self
                        .get_relationship_meta(property_id.relationship_id)
                        .unwrap();

                    // Check that the same relation_id is not reused for subject properties
                    if !subject_relation_set.insert(meta.relationship.relation_id) {
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
                        .properties_by_type(meta.relationship.object.0.def_id)
                        .unwrap();

                    // Check if the property is the primary id
                    if let Some(id_relationship_id) = object_properties.identifies {
                        let id_meta = self.get_relationship_meta(id_relationship_id).unwrap();

                        if id_meta.relationship.object.0.def_id == def_id {
                            debug!(
                                "redefine as primary id: {id_relationship_id:?} <-> {:?}",
                                property_id.relationship_id
                            );

                            actions.push(Action::RedefineAsPrimaryId {
                                def_id,
                                inherent_property_id: *property_id,
                                identifies_relationship_id: id_relationship_id,
                            });
                        }
                    }

                    if properties.identified_by.is_some()
                        && object_properties.identified_by.is_some()
                    {
                        actions.push(Action::AdjustEntityPropertyCardinality(
                            def_id,
                            *property_id,
                        ));
                    }
                }
                Role::Object => {
                    if properties.identified_by.is_none() {
                        match &properties.constructor {
                            Constructor::Union(relationships) => {
                                if let Some(map) = &properties.map {
                                    assert!(map.get(property_id).is_some());

                                    let all_entities =
                                        relationships.iter().all(|(relationship_id, _span)| {
                                            let meta = self
                                                .get_relationship_meta(*relationship_id)
                                                .expect("BUG: problem getting relationship meta");

                                            let variant_def = match &meta.relation.kind {
                                                RelationKind::Named(def)
                                                | RelationKind::FmtTransition(def, _) => def.def_id,
                                                _ => meta.relationship.object.0.def_id,
                                            };

                                            let subject_properties = self
                                                .relations
                                                .properties_by_type(variant_def)
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
                                    panic!("No map in value union");
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
                        let meta = self
                            .get_relationship_meta(property_id.relationship_id)
                            .unwrap();
                        let subject_properties = self
                            .relations
                            .properties_by_type(meta.relationship.subject.0.def_id)
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
            debug!("perform action {action:?}");
            match action {
                Action::ReportNonEntityInObjectRelationship(_def_id, relationship_id) => {
                    let meta = self.get_relationship_meta(relationship_id).unwrap();

                    self.error(
                        CompileError::NonEntityInReverseRelationship,
                        meta.relationship.span,
                    );
                }
                Action::AdjustEntityPropertyCardinality(def_id, property_id) => {
                    let properties = self.relations.properties_by_type_mut(def_id);
                    if let Some(map) = &mut properties.map {
                        let cardinality = map.get_mut(&property_id).unwrap();
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
                    let properties = self.relations.properties_by_type_mut(def_id);

                    if let Some(map) = &mut properties.map {
                        map.insert(
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
            }
        }
    }
}

fn adjust_entity_prop_cardinality(property: &mut Property) {
    if let ValueCardinality::Many = property.cardinality.1 {
        property.cardinality.0 = PropertyCardinality::Optional;
    }
}
