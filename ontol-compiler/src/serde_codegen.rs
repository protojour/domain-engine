use std::collections::HashMap;

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator},
    serde::{
        MapType, SequenceRange, SerdeModifier, SerdeOperator, SerdeOperatorId, SerdeProperty,
        ValueType, ValueUnionDiscriminator, ValueUnionType,
    },
    smart_format,
    value::PropertyId,
    DefId, Role,
};
use smallvec::SmallVec;
use tracing::debug;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{Cardinality, DefKind, Defs, PropertyCardinality, RelParams, ValueCardinality},
    patterns::Patterns,
    relation::{Constructor, MapProperties, Properties, Relations},
    types::{DefTypes, Type},
};

pub struct SerdeGenerator<'c, 'm> {
    defs: &'c Defs<'m>,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,
    patterns: &'c Patterns,
    serde_operators: Vec<SerdeOperator>,
    serde_operators_per_def: HashMap<(DefId, SerdeModifier), SerdeOperatorId>,
}

impl<'m> Compiler<'m> {
    pub fn serde_generator(&self) -> SerdeGenerator<'_, 'm> {
        SerdeGenerator {
            defs: &self.defs,
            def_types: &self.def_types,
            relations: &self.relations,
            patterns: &self.patterns,
            serde_operators: Default::default(),
            serde_operators_per_def: Default::default(),
        }
    }
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(
        self,
    ) -> (
        Vec<SerdeOperator>,
        HashMap<(DefId, SerdeModifier), SerdeOperatorId>,
    ) {
        (self.serde_operators, self.serde_operators_per_def)
    }

    pub fn get_serde_operator_id(
        &mut self,
        type_def_id: DefId,
        modifier: SerdeModifier,
    ) -> Option<SerdeOperatorId> {
        if let Some(id) = self.serde_operators_per_def.get(&(type_def_id, modifier)) {
            return Some(*id);
        }

        if let Some((operator_id, operator)) =
            self.create_serde_operator_with_modifier(type_def_id, modifier)
        {
            debug!("created operator {operator_id:?} {operator:?}");
            self.serde_operators[operator_id.0 as usize] = operator;
            Some(operator_id)
        } else {
            None
        }
    }

    fn get_property_operator(
        &mut self,
        type_def_id: DefId,
        cardinality: Cardinality,
    ) -> (PropertyCardinality, SerdeOperatorId) {
        match cardinality.1 {
            ValueCardinality::One => (
                cardinality.0,
                self.get_serde_operator_id(type_def_id, SerdeModifier::Identity)
                    .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.get_serde_operator_id(type_def_id, SerdeModifier::Array)
                    .expect("no property operator"),
            ),
        }
    }

    fn create_serde_operator_with_modifier(
        &mut self,
        type_def_id: DefId,
        modifier: SerdeModifier,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match modifier {
            SerdeModifier::Identity | SerdeModifier::PropertyMap => {
                self.create_item_operator(type_def_id, modifier)
            }
            SerdeModifier::Array => {
                let item_operator_id =
                    self.get_serde_operator_id(type_def_id, SerdeModifier::Identity)?;

                Some((
                    self.alloc_operator_id(type_def_id, modifier),
                    SerdeOperator::Sequence(
                        [SequenceRange {
                            operator_id: item_operator_id,
                            finite_repetition: None,
                        }]
                        .into_iter()
                        .collect(),
                        type_def_id,
                    ),
                ))
            }
            SerdeModifier::IdMap => match self.get_def_type(type_def_id)? {
                Type::Domain(_) => {
                    let id_relation_id = self.relations.properties_by_type.get(&type_def_id)?.id?;

                    let (relationship, _) = self
                        .get_subject_property_meta(type_def_id, id_relation_id)
                        .expect("Problem getting subject property meta");
                    let object = relationship.object;

                    let object_operator_id = self
                        .get_serde_operator_id(object.0, SerdeModifier::Identity)
                        .expect("No object operator for _id property");
                    let typename = match self.defs.get_def_kind(type_def_id) {
                        Some(DefKind::DomainType(Some(ident))) => smart_format!("{ident}._id"),
                        _ => smart_format!("Unknown type"),
                    };

                    Some((
                        self.alloc_operator_id(type_def_id, modifier),
                        SerdeOperator::Id(object_operator_id),
                        /*
                        SerdeOperator::MapType(MapType {
                            typename,
                            type_def_id,
                            properties: [(
                                smart_format!("_id"),
                                SerdeProperty {
                                    property_id: PropertyId::subject(id_relation_id),
                                    value_operator_id: object_operator_id,
                                    optional: false,
                                    rel_params_operator_id: None,
                                },
                            )]
                            .into(),
                            n_mandatory_properties: 1,
                        }),
                        */
                    ))
                }
                _ => None,
            },
        }
    }

    fn create_item_operator(
        &mut self,
        type_def_id: DefId,
        modifier: SerdeModifier,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match self.get_def_type(type_def_id) {
            Some(Type::Unit(_)) => Some((
                self.alloc_operator_id(type_def_id, modifier),
                SerdeOperator::Unit,
            )),
            Some(Type::IntConstant(_)) => todo!(),
            Some(Type::Int(_)) => Some((
                self.alloc_operator_id(type_def_id, modifier),
                SerdeOperator::Int(type_def_id),
            )),
            Some(Type::Number(_)) => Some((
                self.alloc_operator_id(type_def_id, modifier),
                SerdeOperator::Number(type_def_id),
            )),
            Some(Type::String(_)) => Some((
                self.alloc_operator_id(type_def_id, modifier),
                SerdeOperator::String(type_def_id),
            )),
            Some(Type::StringConstant(def_id)) => {
                assert_eq!(type_def_id, *def_id);

                let literal = self.defs.get_string_representation(*def_id);

                Some((
                    self.alloc_operator_id(*def_id, modifier),
                    SerdeOperator::StringConstant(literal.into(), type_def_id),
                ))
            }
            Some(Type::Regex(def_id)) => {
                assert_eq!(type_def_id, *def_id);
                assert!(self.patterns.string_patterns.contains_key(&type_def_id));

                Some((
                    self.alloc_operator_id(*def_id, modifier),
                    SerdeOperator::StringPattern(*def_id),
                ))
            }
            Some(Type::Uuid(_)) => Some((
                self.alloc_operator_id(type_def_id, modifier),
                SerdeOperator::String(type_def_id),
            )),
            Some(Type::EmptySequence(_)) => {
                todo!("not sure if this should be handled here")
            }
            Some(Type::Array(_)) => {
                panic!("Array not handled here")
            }
            Some(Type::Option(_)) => {
                panic!("Option not handled here")
            }
            Some(Type::Domain(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::DomainType(Some(ident))) => ident,
                    Some(DefKind::DomainType(None)) => "anonymous",
                    _ => "Unknown type",
                };
                let operator_id = self.alloc_operator_id(type_def_id, modifier);
                Some((
                    operator_id,
                    self.create_domain_type_serde_operator(typename, *def_id, properties, modifier),
                ))
            }
            Some(Type::Function { .. }) => None,
            Some(Type::Anonymous(_)) => None,
            Some(Type::Package | Type::BuiltinRelation) => None,
            Some(Type::Tautology | Type::Infer(_) | Type::Error) => {
                panic!("crap: {:?}", self.get_def_type(type_def_id));
            }
            None => panic!("No type available"),
        }
    }

    fn alloc_operator_id(&mut self, def_id: DefId, modifier: SerdeModifier) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        // We just need a temporary placeholder for this operator,
        // this will be properly overwritten afterwards:
        self.serde_operators.push(SerdeOperator::Unit);
        self.serde_operators_per_def
            .insert((def_id, modifier), operator_id);
        operator_id
    }

    fn create_domain_type_serde_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: Option<&Properties>,
        modifier: SerdeModifier,
    ) -> SerdeOperator {
        let properties = match properties {
            None => {
                return SerdeOperator::MapType(MapType {
                    typename: typename.into(),
                    type_def_id,
                    properties: Default::default(),
                    n_mandatory_properties: 0,
                })
            }
            Some(properties) => properties,
        };

        match &properties.constructor {
            Constructor::Identity => self.create_identity_constructor_operator(
                typename,
                type_def_id,
                &properties,
                modifier,
            ),
            Constructor::Value(relationship_id, _, cardinality) => {
                let Ok((_, relation)) = self.get_relationship_meta(*relationship_id) else {
                    panic!("Problem getting property meta");
                };

                let value_def = relation.ident_def().unwrap();

                let (requirement, inner_operator_id) =
                    self.get_property_operator(value_def, *cardinality);

                if !matches!(requirement, PropertyCardinality::Mandatory) {
                    panic!("Value properties must be mandatory, fix this during type check");
                }

                SerdeOperator::ValueType(ValueType {
                    typename: typename.into(),
                    type_def_id,
                    inner_operator_id,
                })
            }
            Constructor::ValueUnion(_) => {
                let union_disciminator = self
                    .relations
                    .union_discriminators
                    .get(&type_def_id)
                    .expect("no union discriminator available. Should fail earlier");

                SerdeOperator::ValueUnionType(ValueUnionType {
                    typename: typename.into(),
                    discriminators: union_disciminator
                        .variants
                        .iter()
                        .map(|discriminator| {
                            let operator_id = self
                                .get_serde_operator_id(
                                    discriminator.result_type,
                                    SerdeModifier::Identity,
                                )
                                .expect("No inner operator");

                            ValueUnionDiscriminator {
                                discriminator: discriminator.clone(),
                                operator_id,
                            }
                        })
                        .collect(),
                })
            }
            Constructor::Sequence(sequence) => {
                let mut sequence_range_builder = SequenceRangeBuilder {
                    ranges: Default::default(),
                };

                let mut element_iterator = sequence.elements().peekable();

                while let Some((_, element)) = element_iterator.next() {
                    let operator_id = match element {
                        None => self
                            .get_serde_operator_id(DefId::unit(), SerdeModifier::Identity)
                            .unwrap(),
                        Some(relationship_id) => {
                            let (relationship, _relation) = self
                                .get_relationship_meta(relationship_id)
                                .expect("Problem getting relationship meta");

                            self.get_serde_operator_id(
                                relationship.object.0,
                                SerdeModifier::Identity,
                            )
                            .expect("no inner operator")
                        }
                    };

                    if element_iterator.peek().is_some() || !sequence.is_infinite() {
                        sequence_range_builder.push_required_operator(operator_id);
                    } else {
                        // last operator is infinite and accepts zero or more items
                        sequence_range_builder.push_infinite_operator(operator_id);
                    }
                }

                debug!("sequence ranges: {:#?}", sequence_range_builder.ranges);

                SerdeOperator::Sequence(sequence_range_builder.ranges, type_def_id)
            }
            Constructor::StringPattern(_) => {
                assert!(self.patterns.string_patterns.contains_key(&type_def_id));
                SerdeOperator::CapturingStringPattern(type_def_id)
            }
        }
    }

    fn create_identity_constructor_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: &Properties,
        modifier: SerdeModifier,
    ) -> SerdeOperator {
        match modifier {
            SerdeModifier::PropertyMap => {
                self.create_map_operator(typename, type_def_id, properties)
            }
            _ => {
                if let Some(id_relation_id) = &properties.id {
                    let (relationship, _) = self
                        .get_subject_property_meta(type_def_id, *id_relation_id)
                        .expect("Problem getting subject property meta");

                    // Create a union between { '_id' } and the map properties itself
                    let map_properties_operator_id = self
                        .get_serde_operator_id(type_def_id, SerdeModifier::PropertyMap)
                        .expect("No property map operator");

                    let id_operator_id = self
                        .get_serde_operator_id(type_def_id, SerdeModifier::IdMap)
                        .expect("No _id operator");

                    SerdeOperator::ValueUnionType(ValueUnionType {
                        typename: typename.into(),
                        discriminators: vec![
                            ValueUnionDiscriminator {
                                discriminator: VariantDiscriminator {
                                    discriminant: Discriminant::HasProperty(
                                        *id_relation_id,
                                        "_id".into(),
                                    ),
                                    result_type: relationship.object.0,
                                },
                                operator_id: id_operator_id,
                            },
                            ValueUnionDiscriminator {
                                discriminator: VariantDiscriminator {
                                    discriminant: Discriminant::IsMap,
                                    result_type: type_def_id,
                                },
                                operator_id: map_properties_operator_id,
                            },
                        ],
                    })
                } else {
                    self.create_map_operator(typename, type_def_id, properties)
                }
            }
        }
    }

    fn create_map_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: &Properties,
    ) -> SerdeOperator {
        let mut n_mandatory_properties = 0;
        let mut serde_properties: IndexMap<_, _> = Default::default();

        if let MapProperties::Map(map) = &properties.map {
            for (property_id, cardinality) in map {
                let (relationship, prop_key, type_def_id) = match property_id.role {
                    Role::Subject => {
                        let (relationship, relation) = self
                            .get_subject_property_meta(type_def_id, property_id.relation_id)
                            .expect("Problem getting subject property meta");
                        let object = relationship.object;

                        let prop_key = relation
                            .subject_prop(self.defs)
                            .expect("Subject property has no name");

                        (relationship, prop_key, object.0)
                    }
                    Role::Object => {
                        let (relationship, relation) = self
                            .get_object_property_meta(type_def_id, property_id.relation_id)
                            .expect("Problem getting object property meta");
                        let subject = relationship.subject;

                        let prop_key = relation
                            .object_prop(self.defs)
                            .expect("Object property has no name");

                        (relationship, prop_key, subject.0)
                    }
                };

                let (property_cardinality, value_operator_id) =
                    self.get_property_operator(type_def_id, *cardinality);

                let rel_params_operator_id = match relationship.rel_params {
                    RelParams::Type(def_id) => {
                        self.get_serde_operator_id(def_id, SerdeModifier::Identity)
                    }
                    RelParams::Unit => None,
                    _ => todo!(),
                };

                if property_cardinality.is_mandatory() {
                    n_mandatory_properties += 1;
                }

                serde_properties.insert(
                    prop_key.into(),
                    SerdeProperty {
                        property_id: *property_id,
                        value_operator_id,
                        optional: property_cardinality.is_optional(),
                        rel_params_operator_id,
                    },
                );
            }
        };

        SerdeOperator::MapType(MapType {
            typename: typename.into(),
            type_def_id,
            properties: serde_properties,
            n_mandatory_properties,
        })
    }
}

struct SequenceRangeBuilder {
    ranges: SmallVec<[SequenceRange; 3]>,
}

impl SequenceRangeBuilder {
    fn push_required_operator(&mut self, operator_id: SerdeOperatorId) {
        match self.ranges.last_mut() {
            Some(range) => {
                if operator_id == range.operator_id {
                    // two or more identical operator ids in row;
                    // just increase repetition counter:
                    let finite_repetition = range.finite_repetition.unwrap();
                    range.finite_repetition = Some(finite_repetition + 1);
                } else {
                    self.ranges.push(SequenceRange {
                        operator_id,
                        finite_repetition: Some(1),
                    });
                }
            }
            None => {
                self.ranges.push(SequenceRange {
                    operator_id,
                    finite_repetition: Some(1),
                });
            }
        }
    }

    fn push_infinite_operator(&mut self, operator_id: SerdeOperatorId) {
        self.ranges.push(SequenceRange {
            operator_id,
            finite_repetition: None,
        })
    }
}

impl<'c, 'm> AsRef<Defs<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        self.defs
    }
}

impl<'c, 'm> AsRef<DefTypes<'m>> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &DefTypes<'m> {
        self.def_types
    }
}

impl<'c, 'm> AsRef<Relations> for SerdeGenerator<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        self.relations
    }
}
