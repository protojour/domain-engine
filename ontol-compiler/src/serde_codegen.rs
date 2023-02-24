use std::collections::{BTreeMap, HashMap, HashSet};

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator},
    serde::{
        MapType, SequenceRange, SerdeOperator, SerdeOperatorId, SerdeOperatorKey, SerdeProperty,
        ValueType, ValueUnionDiscriminator, ValueUnionType,
    },
    smart_format, DefId, Role,
};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    compiler::Compiler,
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{Cardinality, DefKind, Defs, PropertyCardinality, RelParams, ValueCardinality},
    patterns::Patterns,
    relation::{Constructor, Properties, Relations},
    types::{DefTypes, Type},
};

pub struct SerdeGenerator<'c, 'm> {
    defs: &'c Defs<'m>,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,
    patterns: &'c Patterns,
    serde_operators: Vec<SerdeOperator>,
    serde_operators_per_def: HashMap<SerdeOperatorKey, SerdeOperatorId>,
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
        HashMap<SerdeOperatorKey, SerdeOperatorId>,
    ) {
        (self.serde_operators, self.serde_operators_per_def)
    }

    pub fn get_serde_operator(&mut self, key: SerdeOperatorKey) -> Option<&SerdeOperator> {
        let operator_id = self.get_serde_operator_id(key)?;
        Some(&self.serde_operators[operator_id.0 as usize])
    }

    pub fn get_serde_operator_id(&mut self, key: SerdeOperatorKey) -> Option<SerdeOperatorId> {
        if let Some(id) = self.serde_operators_per_def.get(&key) {
            return Some(*id);
        }

        if let Some((operator_id, operator)) = self.create_serde_operator_from_key(key.clone()) {
            debug!("created operator {operator_id:?} {key:?} {operator:?}");
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
                self.get_serde_operator_id(SerdeOperatorKey::Identity(type_def_id))
                    .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.get_serde_operator_id(SerdeOperatorKey::Array(type_def_id))
                    .expect("no property operator"),
            ),
        }
    }

    fn create_serde_operator_from_key(
        &mut self,
        key: SerdeOperatorKey,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match &key {
            SerdeOperatorKey::Identity(def_id)
            | SerdeOperatorKey::JoinedPropertyMap(def_id)
            | SerdeOperatorKey::InherentPropertyMap(def_id) => {
                let def_id = *def_id;
                self.create_item_operator(key, def_id)
            }
            SerdeOperatorKey::Array(def_id) => {
                let item_operator_id =
                    self.get_serde_operator_id(SerdeOperatorKey::Identity(*def_id))?;

                Some((
                    self.alloc_operator_id(&key),
                    SerdeOperator::Sequence(
                        [SequenceRange {
                            operator_id: item_operator_id,
                            finite_repetition: None,
                        }]
                        .into_iter()
                        .collect(),
                        *def_id,
                    ),
                ))
            }
            SerdeOperatorKey::IdMap(def_id) => match self.get_def_type(*def_id)? {
                Type::Domain(_) => {
                    let id_relation_id = self.relations.properties_by_type.get(def_id)?.id?;

                    let (relationship, _) = self
                        .get_subject_property_meta(*def_id, id_relation_id)
                        .expect("Problem getting subject property meta");
                    let object = relationship.object;

                    let object_operator_id = self
                        .get_serde_operator_id(SerdeOperatorKey::Identity(object.0))
                        .expect("No object operator for _id property");

                    Some((
                        self.alloc_operator_id(&key),
                        SerdeOperator::Id(object_operator_id),
                    ))
                }
                _ => None,
            },
            SerdeOperatorKey::Intersection(keys) => {
                let operator_id = self.alloc_operator_id(&key);
                let mut iterator = keys.iter();
                let first_id = self.get_serde_operator_id(iterator.next()?.clone())?;

                let mut intersected_map = self
                    .find_unambiguous_map_type(first_id)
                    .unwrap_or_else(|operator| {
                        panic!("Initial map not found for intersection: {operator:?}")
                    })
                    .clone();

                for next_key in iterator {
                    let next_id = self.get_serde_operator_id(next_key.clone()).unwrap();

                    let next_map_type =
                        self.find_unambiguous_map_type(next_id)
                            .unwrap_or_else(|operator| {
                                panic!("Map not found for intersection: {operator:?}")
                            });
                    for (key, value) in &next_map_type.properties {
                        intersected_map.properties.insert(key.clone(), *value);
                    }
                }

                Some((operator_id, SerdeOperator::MapType(intersected_map)))
            }
        }
    }

    fn find_unambiguous_map_type(&self, id: SerdeOperatorId) -> Result<&MapType, &SerdeOperator> {
        let operator = &self.serde_operators[id.0 as usize];
        match operator {
            SerdeOperator::MapType(map_type) => Ok(map_type),
            SerdeOperator::ValueUnionType(union_type) => {
                let mut map_count = 0;
                let mut result = Err(operator);

                for discriminator in &union_type.discriminators {
                    if let Ok(map_type) = self.find_unambiguous_map_type(discriminator.operator_id)
                    {
                        result = Ok(map_type);
                        map_count += 1;
                    } else {
                        let operator = &self.serde_operators[discriminator.operator_id.0 as usize];
                        debug!("SKIPPED SOMETHING: {operator:?}\n\n");
                    }
                }

                if map_count > 1 {
                    Err(operator)
                } else {
                    result
                }
            }
            _ => Err(operator),
        }
    }

    fn create_item_operator(
        &mut self,
        key: SerdeOperatorKey,
        type_def_id: DefId,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match self.get_def_type(type_def_id) {
            Some(Type::Unit(_)) => Some((self.alloc_operator_id(&key), SerdeOperator::Unit)),
            Some(Type::IntConstant(_)) => todo!(),
            Some(Type::Int(_)) => Some((
                self.alloc_operator_id(&key),
                SerdeOperator::Int(type_def_id),
            )),
            Some(Type::Number(_)) => Some((
                self.alloc_operator_id(&key),
                SerdeOperator::Number(type_def_id),
            )),
            Some(Type::String(_)) => Some((
                self.alloc_operator_id(&key),
                SerdeOperator::String(type_def_id),
            )),
            Some(Type::StringConstant(def_id)) => {
                assert_eq!(type_def_id, *def_id);

                let literal = self.defs.get_string_representation(*def_id);

                Some((
                    self.alloc_operator_id(&key),
                    SerdeOperator::StringConstant(literal.into(), type_def_id),
                ))
            }
            Some(Type::Regex(def_id)) => {
                assert_eq!(type_def_id, *def_id);
                assert!(self.patterns.string_patterns.contains_key(&type_def_id));

                Some((
                    self.alloc_operator_id(&key),
                    SerdeOperator::StringPattern(*def_id),
                ))
            }
            Some(Type::Uuid(_)) => Some((
                self.alloc_operator_id(&key),
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
                let operator_id = self.alloc_operator_id(&key);
                Some((
                    operator_id,
                    self.create_domain_type_serde_operator(typename, *def_id, properties, key),
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

    fn alloc_operator_id(&mut self, key: &SerdeOperatorKey) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.serde_operators.len() as u32);
        // We just need a temporary placeholder for this operator,
        // this will be properly overwritten afterwards:
        self.serde_operators.push(SerdeOperator::Unit);
        self.serde_operators_per_def
            .insert(key.clone(), operator_id);
        operator_id
    }

    fn create_domain_type_serde_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: Option<&Properties>,
        key: SerdeOperatorKey,
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
            Constructor::Identity => {
                self.create_identity_constructor_operator(typename, type_def_id, properties, key)
            }
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
                if let SerdeOperatorKey::InherentPropertyMap(_) = key {
                    // just the inherent properties are requested.
                    // Don't build a union
                    self.create_map_operator(typename, type_def_id, properties)
                } else {
                    self.create_value_union_operator(typename, type_def_id, properties)
                }
            }
            Constructor::Sequence(sequence) => {
                let mut sequence_range_builder = SequenceRangeBuilder {
                    ranges: Default::default(),
                };

                let mut element_iterator = sequence.elements().peekable();

                while let Some((_, element)) = element_iterator.next() {
                    let operator_id = match element {
                        None => self
                            .get_serde_operator_id(SerdeOperatorKey::Identity(DefId::unit()))
                            .unwrap(),
                        Some(relationship_id) => {
                            let (relationship, _relation) = self
                                .get_relationship_meta(relationship_id)
                                .expect("Problem getting relationship meta");

                            self.get_serde_operator_id(SerdeOperatorKey::Identity(
                                relationship.object.0,
                            ))
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
        key: SerdeOperatorKey,
    ) -> SerdeOperator {
        match key {
            SerdeOperatorKey::JoinedPropertyMap(_) => {
                self.create_map_operator(typename, type_def_id, properties)
            }
            _ => {
                if let Some(id_relation_id) = &properties.id {
                    let (relationship, _) = self
                        .get_subject_property_meta(type_def_id, *id_relation_id)
                        .expect("Problem getting subject property meta");

                    // Create a union between { '_id' } and the map properties itself
                    let id_operator_id = self
                        .get_serde_operator_id(SerdeOperatorKey::IdMap(type_def_id))
                        .expect("No _id operator");
                    let map_properties_operator_id = self
                        .get_serde_operator_id(SerdeOperatorKey::JoinedPropertyMap(type_def_id))
                        .expect("No property map operator");

                    SerdeOperator::ValueUnionType(ValueUnionType {
                        typename: typename.into(),
                        discriminators: vec![
                            ValueUnionDiscriminator {
                                discriminator: VariantDiscriminator {
                                    discriminant: Discriminant::IsSingletonProperty(
                                        *id_relation_id,
                                        "_id".into(),
                                    ),
                                    result_type: relationship.object.0,
                                },
                                operator_id: id_operator_id,
                            },
                            ValueUnionDiscriminator {
                                discriminator: VariantDiscriminator {
                                    discriminant: Discriminant::MapFallback,
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

    fn create_value_union_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: &Properties,
    ) -> SerdeOperator {
        let union_disciminator = self
            .relations
            .union_discriminators
            .get(&type_def_id)
            .expect("no union discriminator available. Should fail earlier");

        let discriminators: Vec<_> = if properties.map.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_key = SerdeOperatorKey::InherentPropertyMap(type_def_id);

            let mut union_builder = UnionBuilder {
                discriminator_candidates: vec![],
            };
            let mut root_types: HashSet<DefId> = Default::default();

            for root_discriminator in &union_disciminator.variants {
                union_builder
                    .add_root_discriminator(self, root_discriminator)
                    .expect("Could not add root discriminator to union builder");

                root_types.insert(root_discriminator.result_type);
            }

            union_builder
                .build(self, |this, operator_id, result_type| {
                    if root_types.contains(&result_type) {
                        // Make the intersection:
                        this.get_serde_operator_id(SerdeOperatorKey::Intersection(Box::new(
                            [
                                inherent_properties_key.clone(),
                                SerdeOperatorKey::Identity(result_type),
                            ]
                            .into(),
                        )))
                        .expect("No inner operator")
                    } else {
                        operator_id
                    }
                })
                .unwrap()
        } else {
            union_disciminator
                .variants
                .iter()
                .map(|discriminator| {
                    let operator_id = self
                        .get_serde_operator_id(SerdeOperatorKey::Identity(
                            discriminator.result_type,
                        ))
                        .expect("No inner operator");

                    ValueUnionDiscriminator {
                        discriminator: discriminator.clone(),
                        operator_id,
                    }
                })
                .collect()
        };

        SerdeOperator::ValueUnionType(ValueUnionType {
            typename: typename.into(),
            discriminators,
        })
    }

    fn create_map_operator(
        &mut self,
        typename: &str,
        type_def_id: DefId,
        properties: &Properties,
    ) -> SerdeOperator {
        let mut n_mandatory_properties = 0;
        let mut serde_properties: IndexMap<_, _> = Default::default();

        if let Some(map) = &properties.map {
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
                        self.get_serde_operator_id(SerdeOperatorKey::Identity(def_id))
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

struct UnionBuilder {
    discriminator_candidates: Vec<ValueUnionDiscriminator>,
}

impl UnionBuilder {
    fn build(
        self,
        generator: &mut SerdeGenerator,
        mut map_operator_fn: impl FnMut(&mut SerdeGenerator, SerdeOperatorId, DefId) -> SerdeOperatorId,
    ) -> Result<Vec<ValueUnionDiscriminator>, String> {
        let mut discriminators_by_discriminant: BTreeMap<
            Discriminant,
            Vec<(SerdeOperatorId, DefId)>,
        > = Default::default();

        for candidate in self.discriminator_candidates {
            let result_type = candidate.discriminator.result_type;
            let operator_id = map_operator_fn(generator, candidate.operator_id, result_type);

            match candidate.discriminator.discriminant {
                Discriminant::MapFallback => {
                    panic!("MapFallback should have been filtered already");
                }
                Discriminant::IsSingletonProperty(relation_id, prop) => {
                    // TODO: We don't know that we have to do any disambiguation here
                    // (there might be only one singleton property)
                    let operator =
                        generator.get_serde_operator(SerdeOperatorKey::Identity(result_type));

                    match operator {
                        Some(SerdeOperator::CapturingStringPattern(def_id)) => {
                            // convert this
                            discriminators_by_discriminant
                                .entry(Discriminant::HasAttributeMatchingStringPattern(
                                    relation_id,
                                    prop,
                                    *def_id,
                                ))
                                .or_default()
                                .push((operator_id, result_type));
                        }
                        _ => {
                            discriminators_by_discriminant
                                .entry(Discriminant::IsSingletonProperty(relation_id, prop))
                                .or_default()
                                .push((operator_id, result_type));
                        }
                    }
                }
                discriminant => {
                    discriminators_by_discriminant
                        .entry(discriminant)
                        .or_default()
                        .push((operator_id, result_type));
                }
            }
        }

        let mut discriminators = vec![];

        for (discriminant, entries) in discriminators_by_discriminant {
            if entries.len() > 1 {
                return Err(smart_format!(
                    "BUG: Discriminant {discriminant:?} has multiple entries: {entries:?}"
                ));
            }

            if let Some((operator_id, result_type)) = entries.into_iter().next() {
                discriminators.push(ValueUnionDiscriminator {
                    discriminator: VariantDiscriminator {
                        discriminant,
                        result_type,
                    },
                    operator_id,
                });
            }
        }

        Ok(discriminators)
    }

    fn add_root_discriminator(
        &mut self,
        generator: &mut SerdeGenerator,
        discriminator: &VariantDiscriminator,
    ) -> Result<(), String> {
        let operator_id = match generator
            .get_serde_operator_id(SerdeOperatorKey::Identity(discriminator.result_type))
        {
            Some(operator_id) => operator_id,
            None => return Ok(()),
        };

        // Push with empty scope ('root scope')
        self.push_discriminator(generator, &[], discriminator, operator_id)
    }

    fn push_discriminator(
        &mut self,
        generator: &SerdeGenerator,
        scope: &[&VariantDiscriminator],
        discriminator: &VariantDiscriminator,
        operator_id: SerdeOperatorId,
    ) -> Result<(), String> {
        let operator = &generator.serde_operators[operator_id.0 as usize];
        match operator {
            SerdeOperator::ValueUnionType(value_union) => {
                for inner_discriminator in &value_union.discriminators {
                    let mut child_scope: Vec<&VariantDiscriminator> = vec![];
                    child_scope.extend(scope.iter());
                    child_scope.push(discriminator);

                    self.push_discriminator(
                        generator,
                        &child_scope,
                        &inner_discriminator.discriminator,
                        inner_discriminator.operator_id,
                    )?;
                }
                Ok(())
            }
            other => {
                debug!("PUSH DISCR scope={scope:#?} discriminator={discriminator:#?} {other:?}");
                match discriminator.discriminant {
                    Discriminant::MapFallback => {
                        if let Some(scoping) = scope.last() {
                            self.discriminator_candidates.push(ValueUnionDiscriminator {
                                discriminator: (*scoping).clone(),
                                operator_id,
                            });
                            Ok(())
                        } else {
                            Err(smart_format!("MapFallback without scoping"))
                        }
                    }
                    _ => {
                        self.discriminator_candidates.push(ValueUnionDiscriminator {
                            discriminator: discriminator.clone(),
                            operator_id,
                        });
                        Ok(())
                    }
                }
            }
        }
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
