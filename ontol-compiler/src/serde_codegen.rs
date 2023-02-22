use std::collections::HashMap;

use indexmap::IndexMap;
use ontol_runtime::{
    serde::{
        MapType, SequenceRange, SerdeOperator, SerdeOperatorId, SerdeProperty, ValueType,
        ValueUnionDiscriminator, ValueUnionType,
    },
    DefId, Role,
};
use smallvec::SmallVec;
use smartstring::alias::String;
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
    serde_operators_per_def: HashMap<DefId, SerdeOperatorId>,
    array_operators: HashMap<SerdeOperatorId, SerdeOperatorId>,
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
            array_operators: Default::default(),
        }
    }
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(self) -> (Vec<SerdeOperator>, HashMap<DefId, SerdeOperatorId>) {
        (self.serde_operators, self.serde_operators_per_def)
    }

    pub fn get_serde_operator_id(&mut self, type_def_id: DefId) -> Option<SerdeOperatorId> {
        if let Some(id) = self.serde_operators_per_def.get(&type_def_id) {
            return Some(*id);
        }

        if let Some((operator_id, operator)) = self.create_serde_operator(type_def_id) {
            debug!("created operator {operator_id:?} {operator:?}");
            self.serde_operators[operator_id.0 as usize] = operator;
            Some(operator_id)
        } else {
            None
        }
    }

    fn create_serde_operator(
        &mut self,
        type_def_id: DefId,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match self.get_def_type(type_def_id) {
            Some(Type::Unit(_)) => Some((self.alloc_operator_id(type_def_id), SerdeOperator::Unit)),
            Some(Type::IntConstant(_)) => todo!(),
            Some(Type::Int(_)) => Some((
                self.alloc_operator_id(type_def_id),
                SerdeOperator::Int(type_def_id),
            )),
            Some(Type::Number(_)) => Some((
                self.alloc_operator_id(type_def_id),
                SerdeOperator::Number(type_def_id),
            )),
            Some(Type::String(_)) => Some((
                self.alloc_operator_id(type_def_id),
                SerdeOperator::String(type_def_id),
            )),
            Some(Type::StringConstant(def_id)) => {
                assert_eq!(type_def_id, *def_id);

                let literal = self.defs.get_string_representation(*def_id);

                Some((
                    self.alloc_operator_id(*def_id),
                    SerdeOperator::StringConstant(literal.into(), type_def_id),
                ))
            }
            Some(Type::Regex(def_id)) => {
                assert_eq!(type_def_id, *def_id);
                assert!(self.patterns.string_patterns.contains_key(&type_def_id));

                Some((
                    self.alloc_operator_id(*def_id),
                    SerdeOperator::StringPattern(*def_id),
                ))
            }
            Some(Type::Uuid(_)) => Some((
                self.alloc_operator_id(type_def_id),
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
            Some(Type::Domain(def_id) | Type::DomainEntity(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::DomainType(Some(ident))) => ident,
                    Some(DefKind::DomainType(None)) => "anonymous",
                    Some(DefKind::DomainEntity(ident)) => ident,
                    _ => "Unknown type",
                };
                let operator_id = self.alloc_operator_id(type_def_id);
                Some((
                    operator_id,
                    self.create_domain_type_serde_operator(typename, *def_id, properties),
                ))
            }
            Some(Type::Function { .. }) => None,
            Some(Type::Anonymous(_)) => None,
            Some(Type::Namespace) => None,
            Some(Type::Tautology | Type::Infer(_) | Type::Error) => {
                panic!("crap: {:?}", self.get_def_type(type_def_id));
            }
            None => panic!("No type available"),
        }
    }

    fn alloc_operator_id(&mut self, def_id: DefId) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        // We just need a temporary placeholder for this operator,
        // this will be properly overwritten afterwards:
        self.serde_operators.push(SerdeOperator::Unit);
        self.serde_operators_per_def.insert(def_id, operator_id);
        operator_id
    }

    fn create_domain_type_serde_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: Option<&Properties>,
    ) -> SerdeOperator {
        let properties = match properties {
            None => return create_map_operator(typename, type_def_id, &MapProperties::Empty, self),
            Some(properties) => properties,
        };

        match &properties.constructor {
            Constructor::Identity => {
                create_map_operator(typename, type_def_id, &properties.map, self)
            }
            Constructor::Value(relationship_id, _, cardinality) => {
                let Ok((_, relation)) = self.get_relationship_meta(*relationship_id) else {
                    panic!("Problem getting property meta");
                };

                let value_def = relation.ident_def().unwrap();
                let inner_operator_id = self
                    .get_serde_operator_id(value_def)
                    .expect("No inner operator");

                let (requirement, inner_operator_id) =
                    self.property_operator(inner_operator_id, value_def, *cardinality);

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
                                .get_serde_operator_id(discriminator.result_type)
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
                        None => self.get_serde_operator_id(DefId::unit()).unwrap(),
                        Some(relationship_id) => {
                            let (relationship, _relation) = self
                                .get_relationship_meta(relationship_id)
                                .expect("Problem getting relationship meta");

                            self.get_serde_operator_id(relationship.object.0)
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

    fn property_operator(
        &mut self,
        operator_id: SerdeOperatorId,
        element_def_id: DefId,
        cardinality: Cardinality,
    ) -> (PropertyCardinality, SerdeOperatorId) {
        match cardinality.1 {
            ValueCardinality::One => (cardinality.0, operator_id),
            ValueCardinality::Many => (
                cardinality.0,
                self.many_operator(operator_id, element_def_id),
            ),
        }
    }

    fn many_operator(
        &mut self,
        operator_id: SerdeOperatorId,
        element_def_id: DefId,
    ) -> SerdeOperatorId {
        if let Some(array_operator_id) = self.array_operators.get(&operator_id) {
            return *array_operator_id;
        }

        let array_operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        self.serde_operators.push(SerdeOperator::Sequence(
            [SequenceRange {
                operator_id,
                finite_repetition: None,
            }]
            .into_iter()
            .collect(),
            element_def_id,
        ));
        self.array_operators.insert(operator_id, array_operator_id);
        array_operator_id
    }
}

fn create_map_operator(
    typename: &str,
    type_def_id: DefId,
    map_properties: &MapProperties,
    generator: &mut SerdeGenerator,
) -> SerdeOperator {
    let map_type = match map_properties {
        MapProperties::Empty => MapType {
            typename: typename.into(),
            type_def_id,
            properties: [].into(),
            n_mandatory_properties: 0,
        },
        MapProperties::Map(map) => {
            let mut n_mandatory_properties = 0;
            let properties: IndexMap<String, SerdeProperty> = map
                .iter()
                .map(|(property_id, cardinality)| {
                    let (relationship, prop_key, type_def_id) = match property_id.role {
                        Role::Subject => {
                            let (relationship, relation) = generator
                                .get_subject_property_meta(type_def_id, property_id.relation_id)
                                .expect("Problem getting subject property meta");

                            let prop_key = relation
                                .subject_prop(generator.defs)
                                .expect("Subject property has no name");

                            (relationship, prop_key, relationship.object.0)
                        }
                        Role::Object => {
                            let (relationship, relation) = generator
                                .get_object_property_meta(type_def_id, property_id.relation_id)
                                .expect("Problem getting object property meta");

                            let prop_key = relation
                                .object_prop(generator.defs)
                                .expect("Object property has no name");

                            (relationship, prop_key, relationship.subject.0)
                        }
                    };

                    let operator_id = generator
                        .get_serde_operator_id(type_def_id)
                        .expect("No inner operator");
                    let (requirement, value_operator_id) =
                        generator.property_operator(operator_id, type_def_id, *cardinality);

                    let edge_operator_id = match relationship.rel_params {
                        RelParams::Type(def_id) => generator.get_serde_operator_id(def_id),
                        RelParams::Unit => None,
                        _ => todo!(),
                    };

                    if requirement.is_mandatory() {
                        n_mandatory_properties += 1;
                    }

                    (
                        prop_key.into(),
                        SerdeProperty {
                            property_id: *property_id,
                            value_operator_id,
                            optional: requirement.is_optional(),
                            rel_params_operator_id: edge_operator_id,
                        },
                    )
                })
                .collect();

            MapType {
                typename: typename.into(),
                type_def_id,
                properties,
                n_mandatory_properties,
            }
        }
    };

    SerdeOperator::MapType(map_type)
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
