use ontol_runtime::{value::PropertyId, DefId, RelationId, Role};
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, DefKind, PropertyCardinality, RelationKind, ValueCardinality},
    error::CompileError,
    relation::Constructor,
};

use super::TypeCheck;

#[derive(Debug)]
enum Action {
    ReportNonEntityInObjectRelationship(DefId, RelationId),
    /// Many(*) value cardinality between two entities are always considered optional
    AdjustEntityPropertyCardinality(DefId, PropertyId),
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

        let mut actions = vec![];

        let map = properties.map.as_ref()?;

        for (property_id, cardinality) in map {
            debug!("check {def_id:?} {property_id:?} {cardinality:?}");

            match property_id.role {
                Role::Subject => {
                    let (relationship, _) = self
                        .property_meta_by_subject(def_id, property_id.relation_id)
                        .unwrap();
                    let object_properties = self
                        .relations
                        .properties_by_type(relationship.object.0.def_id)
                        .unwrap();

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
                                            let (relationship, relation) = self
                                                .get_relationship_meta(*relationship_id)
                                                .expect("BUG: problem getting property meta");

                                            let variant_def = match &relation.kind {
                                                RelationKind::Named(def)
                                                | RelationKind::Transition(def) => def.def_id,
                                                _ => relationship.object.0.def_id,
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
                                    property_id.relation_id,
                                ));
                            }
                        }
                    } else {
                        let (relationship, _) = self
                            .property_meta_by_object(def_id, property_id.relation_id)
                            .unwrap();
                        let subject_properties = self
                            .relations
                            .properties_by_type(relationship.subject.0.def_id)
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
                Action::ReportNonEntityInObjectRelationship(def_id, relation_id) => {
                    let (relationship, _) =
                        self.property_meta_by_object(def_id, relation_id).unwrap();

                    self.error(
                        CompileError::NonEntityInReverseRelationship,
                        relationship.span,
                    );
                }
                Action::AdjustEntityPropertyCardinality(def_id, property_id) => {
                    let properties = self.relations.properties_by_type_mut(def_id);
                    if let Some(map) = &mut properties.map {
                        let cardinality = map.get_mut(&property_id).unwrap();
                        adjust_entity_prop_cardinality(cardinality);
                    }
                }
            }
        }
    }
}

fn adjust_entity_prop_cardinality(cardinality: &mut Cardinality) {
    if let ValueCardinality::Many = cardinality.1 {
        cardinality.0 = PropertyCardinality::Optional;
    }
}
