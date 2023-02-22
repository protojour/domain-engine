use ontol_runtime::{value::PropertyId, DefId, RelationId, Role};

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Def, DefKind, PropertyCardinality, ValueCardinality},
    error::CompileError,
    relation::MapProperties,
};

use super::TypeCheck;

enum Correction {
    ReportNonEntityInObjectRelationship(RelationId),
    /// Many(*) value cardinality between two entities are always considered optional
    AdjustEntityPropertyCardinality(PropertyId),
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_domain_types(&mut self) {
        for (def_id, def) in &self.defs.map {
            if let DefKind::DomainType(_) = &def.kind {
                self.check_domain_type_properties(*def_id, def);
            }
        }
    }

    fn check_domain_type_properties(&mut self, def_id: DefId, _def: &Def) -> Option<()> {
        let properties = self.relations.properties_by_type(def_id)?;
        let is_entity = properties.id.is_some();

        let mut corrections = vec![];

        if let MapProperties::Map(map) = &properties.map {
            for (property_id, _) in map {
                match property_id.role {
                    Role::Subject => {
                        let (relationship, _) = self
                            .get_subject_property_meta(def_id, property_id.relation_id)
                            .unwrap();
                        let object_properties = self
                            .relations
                            .properties_by_type(relationship.object.0)
                            .unwrap();

                        if is_entity && object_properties.id.is_some() {
                            corrections
                                .push(Correction::AdjustEntityPropertyCardinality(*property_id));
                        }
                    }
                    Role::Object => {
                        if !is_entity {
                            // it is illegal to specify an object property to something that is not an entity (has no id)
                            corrections.push(Correction::ReportNonEntityInObjectRelationship(
                                property_id.relation_id,
                            ));
                        } else {
                            let (relationship, _) = self
                                .get_object_property_meta(def_id, property_id.relation_id)
                                .unwrap();
                            let subject_properties = self
                                .relations
                                .properties_by_type(relationship.subject.0)
                                .unwrap();

                            if subject_properties.id.is_some() {
                                corrections.push(Correction::AdjustEntityPropertyCardinality(
                                    *property_id,
                                ));
                            }
                        }
                    }
                }
            }
        }

        self.do_corrections(def_id, corrections);

        None
    }

    fn do_corrections(&mut self, def_id: DefId, corrections: Vec<Correction>) {
        for correction in corrections {
            match correction {
                Correction::ReportNonEntityInObjectRelationship(relation_id) => {
                    let (relationship, _) =
                        self.get_object_property_meta(def_id, relation_id).unwrap();

                    self.error(
                        CompileError::NonEntityInReverseRelationship,
                        relationship.span,
                    );
                }
                Correction::AdjustEntityPropertyCardinality(property_id) => {
                    let properties = self.relations.properties_by_type_mut(def_id);
                    if let MapProperties::Map(map) = &mut properties.map {
                        let (property_cardinality, value_cardinality) =
                            map.get_mut(&property_id).unwrap();
                        if let ValueCardinality::Many = value_cardinality {
                            *property_cardinality = PropertyCardinality::Optional;
                        }
                    }
                }
            }
        }
    }
}
