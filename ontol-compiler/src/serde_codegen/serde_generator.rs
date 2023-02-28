use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator},
    serde::{
        MapType, SequenceRange, SerdeKey, SerdeOperator, SerdeOperatorId, SerdeProperty, ValueType,
        ValueUnionDiscriminator, ValueUnionType,
    },
    DataVariant, DefId, DefVariant, Role,
};
use tracing::debug;

use crate::{
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{Cardinality, DefKind, Defs, PropertyCardinality, RelParams, ValueCardinality},
    patterns::Patterns,
    relation::{Constructor, Properties, Relations},
    serde_codegen::sequence_range_builder::SequenceRangeBuilder,
    types::{DefTypes, Type},
};

use super::union_builder::UnionBuilder;

pub struct SerdeGenerator<'c, 'm> {
    pub(super) defs: &'c Defs<'m>,
    pub(super) def_types: &'c DefTypes<'m>,
    pub(super) relations: &'c Relations,
    pub(super) patterns: &'c Patterns,
    pub(super) operators_by_id: Vec<SerdeOperator>,
    pub(super) operators_by_key: HashMap<SerdeKey, SerdeOperatorId>,
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(self) -> (Vec<SerdeOperator>, HashMap<SerdeKey, SerdeOperatorId>) {
        (self.operators_by_id, self.operators_by_key)
    }

    pub fn get_serde_operator(&mut self, key: SerdeKey) -> Option<&SerdeOperator> {
        let operator_id = self.get_serde_operator_id(key)?;
        Some(&self.operators_by_id[operator_id.0 as usize])
    }

    pub fn get_serde_operator_id(&mut self, key: SerdeKey) -> Option<SerdeOperatorId> {
        if let Some(id) = self.operators_by_key.get(&key) {
            return Some(*id);
        }

        if let Some((operator_id, operator)) = self.create_serde_operator_from_key(key.clone()) {
            debug!("created operator {operator_id:?} {key:?} {operator:?}");
            self.operators_by_id[operator_id.0 as usize] = operator;
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
                self.get_serde_operator_id(SerdeKey::identity(type_def_id))
                    .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.get_serde_operator_id(SerdeKey::variant(DataVariant::Array, type_def_id))
                    .expect("no property operator"),
            ),
        }
    }

    fn create_serde_operator_from_key(
        &mut self,
        key: SerdeKey,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match &key {
            SerdeKey::Variant(
                def_variant @ DefVariant(
                    DataVariant::Identity
                    | DataVariant::JoinedPropertyMap
                    | DataVariant::InherentPropertyMap,
                    _,
                ),
            ) => self.create_item_operator(*def_variant),
            SerdeKey::Variant(def_variant @ DefVariant(DataVariant::Array, def_id)) => {
                let item_operator_id = self.get_serde_operator_id(SerdeKey::identity(*def_id))?;

                Some((
                    self.alloc_operator_id_for_key(&key),
                    SerdeOperator::Sequence(
                        [SequenceRange {
                            operator_id: item_operator_id,
                            finite_repetition: None,
                        }]
                        .into_iter()
                        .collect(),
                        *def_variant,
                    ),
                ))
            }
            SerdeKey::Variant(DefVariant(DataVariant::IdMap, def_id)) => {
                match self.get_def_type(*def_id)? {
                    Type::Domain(_) => {
                        let id_relation_id = self.relations.properties_by_type.get(def_id)?.id?;

                        let (relationship, _) = self
                            .get_subject_property_meta(*def_id, id_relation_id)
                            .expect("Problem getting subject property meta");
                        let object = relationship.object;

                        let object_operator_id = self
                            .get_serde_operator_id(SerdeKey::identity(object.0))
                            .expect("No object operator for _id property");

                        Some((
                            self.alloc_operator_id_for_key(&key),
                            SerdeOperator::Id(object_operator_id),
                        ))
                    }
                    _ => None,
                }
            }
            SerdeKey::Intersection(keys) => {
                let operator_id = self.alloc_operator_id_for_key(&key);
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
        let operator = &self.operators_by_id[id.0 as usize];
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
                        let operator = &self.operators_by_id[discriminator.operator_id.0 as usize];
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
        def_variant: DefVariant,
    ) -> Option<(SerdeOperatorId, SerdeOperator)> {
        match self.get_def_type(def_variant.id()) {
            Some(Type::Unit(_)) => {
                Some((self.alloc_operator_id(&def_variant), SerdeOperator::Unit))
            }
            Some(Type::IntConstant(_)) => todo!(),
            Some(Type::Int(_)) => Some((
                self.alloc_operator_id(&def_variant),
                SerdeOperator::Int(def_variant.id()),
            )),
            Some(Type::Number(_)) => Some((
                self.alloc_operator_id(&def_variant),
                SerdeOperator::Number(def_variant.id()),
            )),
            Some(Type::String(_)) => Some((
                self.alloc_operator_id(&def_variant),
                SerdeOperator::String(def_variant.id()),
            )),
            Some(Type::StringConstant(def_id)) => {
                assert_eq!(def_variant.id(), *def_id);

                let literal = self.defs.get_string_representation(*def_id);

                Some((
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::StringConstant(literal.into(), def_variant.id()),
                ))
            }
            Some(Type::Regex(def_id)) => {
                assert_eq!(def_variant.id(), *def_id);
                assert!(self
                    .patterns
                    .string_patterns
                    .contains_key(&def_variant.id()));

                Some((
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::StringPattern(*def_id),
                ))
            }
            Some(Type::Uuid(_)) => Some((
                self.alloc_operator_id(&def_variant),
                SerdeOperator::String(def_variant.id()),
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
                let operator_id = self.alloc_operator_id(&def_variant);
                Some((
                    operator_id,
                    self.create_domain_type_serde_operator(
                        DefVariant(def_variant.data_variant(), *def_id),
                        typename,
                        properties,
                    ),
                ))
            }
            Some(Type::Function { .. }) => None,
            Some(Type::Anonymous(_)) => None,
            Some(Type::Package | Type::BuiltinRelation) => None,
            Some(Type::Tautology | Type::Infer(_) | Type::Error) => {
                panic!("crap: {:?}", self.get_def_type(def_variant.id()));
            }
            None => panic!("No type available"),
        }
    }

    fn alloc_operator_id(&mut self, def_variant: &DefVariant) -> SerdeOperatorId {
        self.alloc_operator_id_for_key(&SerdeKey::Variant(*def_variant))
    }

    fn alloc_operator_id_for_key(&mut self, key: &SerdeKey) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.operators_by_id.len() as u32);
        // We just need a temporary placeholder for this operator,
        // this will be properly overwritten afterwards:
        self.operators_by_id.push(SerdeOperator::Unit);
        self.operators_by_key.insert(key.clone(), operator_id);
        operator_id
    }

    fn create_domain_type_serde_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: Option<&Properties>,
    ) -> SerdeOperator {
        let properties = match properties {
            None => {
                return SerdeOperator::MapType(MapType {
                    typename: typename.into(),
                    def_variant,
                    properties: Default::default(),
                    n_mandatory_properties: 0,
                })
            }
            Some(properties) => properties,
        };

        match &properties.constructor {
            Constructor::Identity => {
                self.create_identity_constructor_operator(def_variant, typename, properties)
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
                    def_variant,
                    inner_operator_id,
                })
            }
            Constructor::ValueUnion(_) => {
                if let DataVariant::InherentPropertyMap = def_variant.data_variant() {
                    // just the inherent properties are requested.
                    // Don't build a union
                    self.create_map_operator(def_variant, typename, properties)
                } else {
                    self.create_value_union_operator(def_variant, typename, properties)
                }
            }
            Constructor::Sequence(sequence) => {
                let mut sequence_range_builder = SequenceRangeBuilder::default();

                let mut element_iterator = sequence.elements().peekable();

                while let Some((_, element)) = element_iterator.next() {
                    let operator_id = match element {
                        None => self
                            .get_serde_operator_id(SerdeKey::identity(DefId::unit()))
                            .unwrap(),
                        Some(relationship_id) => {
                            let (relationship, _relation) = self
                                .get_relationship_meta(relationship_id)
                                .expect("Problem getting relationship meta");

                            self.get_serde_operator_id(SerdeKey::identity(relationship.object.0))
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

                let ranges = sequence_range_builder.build();
                debug!("sequence ranges: {:#?}", ranges);

                SerdeOperator::Sequence(ranges, def_variant)
            }
            Constructor::StringPattern(_) => {
                assert!(self
                    .patterns
                    .string_patterns
                    .contains_key(&def_variant.id()));
                SerdeOperator::CapturingStringPattern(def_variant.id())
            }
        }
    }

    fn create_identity_constructor_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> SerdeOperator {
        match def_variant.data_variant() {
            DataVariant::JoinedPropertyMap => {
                self.create_map_operator(def_variant, typename, properties)
            }
            _ => {
                if let Some(id_relation_id) = &properties.id {
                    let (relationship, _) = self
                        .get_subject_property_meta(def_variant.id(), *id_relation_id)
                        .expect("Problem getting subject property meta");

                    let id_def_variant = DefVariant(DataVariant::IdMap, def_variant.id());
                    let map_def_variant =
                        DefVariant(DataVariant::JoinedPropertyMap, def_variant.id());

                    // Create a union between { '_id' } and the map properties itself
                    let id_operator_id = self
                        .get_serde_operator_id(SerdeKey::Variant(id_def_variant))
                        .expect("No _id operator");
                    let map_properties_operator_id = self
                        .get_serde_operator_id(SerdeKey::Variant(map_def_variant))
                        .expect("No property map operator");

                    SerdeOperator::ValueUnionType(ValueUnionType {
                        typename: typename.into(),
                        union_def_variant: def_variant,
                        discriminators: vec![
                            ValueUnionDiscriminator {
                                discriminator: VariantDiscriminator {
                                    discriminant: Discriminant::IsSingletonProperty(
                                        *id_relation_id,
                                        "_id".into(),
                                    ),
                                    def_variant: DefVariant(
                                        DataVariant::Identity,
                                        relationship.object.0,
                                    ),
                                },
                                operator_id: id_operator_id,
                            },
                            ValueUnionDiscriminator {
                                discriminator: VariantDiscriminator {
                                    discriminant: Discriminant::MapFallback,
                                    def_variant: map_def_variant,
                                },
                                operator_id: map_properties_operator_id,
                            },
                        ],
                    })
                } else {
                    self.create_map_operator(def_variant, typename, properties)
                }
            }
        }
    }

    fn create_value_union_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> SerdeOperator {
        let union_disciminator = self
            .relations
            .union_discriminators
            .get(&def_variant.id())
            .expect("no union discriminator available. Should fail earlier");

        let mut union_builder = UnionBuilder::default();
        let mut root_types: HashSet<DefId> = Default::default();

        for root_discriminator in &union_disciminator.variants {
            union_builder
                .add_root_discriminator(self, root_discriminator)
                .expect("Could not add root discriminator to union builder");

            root_types.insert(root_discriminator.def_variant.id());
        }

        let discriminators: Vec<_> = if properties.map.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_key =
                SerdeKey::variant(DataVariant::InherentPropertyMap, def_variant.id());

            union_builder
                .build(self, |this, operator_id, result_type| {
                    if root_types.contains(&result_type) {
                        // Make the intersection:
                        this.get_serde_operator_id(SerdeKey::Intersection(Box::new(
                            [
                                inherent_properties_key.clone(),
                                SerdeKey::identity(result_type),
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
            union_builder
                .build(self, |_this, operator_id, _result_type| operator_id)
                .unwrap()
        };

        SerdeOperator::ValueUnionType(ValueUnionType {
            typename: typename.into(),
            union_def_variant: def_variant,
            discriminators,
        })
    }

    fn create_map_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> SerdeOperator {
        let mut n_mandatory_properties = 0;
        let mut serde_properties: IndexMap<_, _> = Default::default();

        if let Some(map) = &properties.map {
            for (property_id, cardinality) in map {
                let (relationship, prop_key, type_def_id) = match property_id.role {
                    Role::Subject => {
                        let (relationship, relation) = self
                            .get_subject_property_meta(def_variant.id(), property_id.relation_id)
                            .expect("Problem getting subject property meta");
                        let object = relationship.object;

                        let prop_key = relation
                            .subject_prop(self.defs)
                            .expect("Subject property has no name");

                        (relationship, prop_key, object.0)
                    }
                    Role::Object => {
                        let (relationship, relation) = self
                            .get_object_property_meta(def_variant.id(), property_id.relation_id)
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
                        self.get_serde_operator_id(SerdeKey::identity(def_id))
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
            def_variant,
            properties: serde_properties,
            n_mandatory_properties,
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
