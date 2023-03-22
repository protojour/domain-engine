use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    serde::operator::{
        ConstructorSequenceOperator, MapOperator, RelationSequenceOperator, SequenceRange,
        SerdeOperator, SerdeOperatorId, SerdeProperty, UnionOperator, ValueOperator,
        ValueUnionVariant,
    },
    serde::SerdeKey,
    DataModifier, DefId, DefVariant, Role,
};
use tracing::debug;

use crate::{
    compiler_queries::{GetDefType, GetPropertyMeta},
    def::{Cardinality, DefKind, Defs, PropertyCardinality, RelParams, ValueCardinality},
    patterns::Patterns,
    relation::{Constructor, Properties, Relations},
    serde_codegen::sequence_range_builder::SequenceRangeBuilder,
    types::{DefTypes, Type, TypeRef},
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

enum OperatorAllocation {
    Allocated(SerdeOperatorId, SerdeOperator),
    Redirect(DefVariant),
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

        match self.create_serde_operator_from_key(key.clone()) {
            Some(OperatorAllocation::Allocated(operator_id, operator)) => {
                debug!("CREATED {operator_id:?} {key:?} {operator:?}");
                self.operators_by_id[operator_id.0 as usize] = operator;
                Some(operator_id)
            }
            Some(OperatorAllocation::Redirect(def_variant)) => {
                let operator_id = self.get_serde_operator_id(SerdeKey::Def(def_variant))?;
                // debug!("key {key:?} redirected to {:?}", def_variant.modifier());
                self.operators_by_key.insert(key, operator_id);
                Some(operator_id)
            }
            None => None,
        }
    }

    fn get_property_operator(
        &mut self,
        type_def_id: DefId,
        cardinality: Cardinality,
        global_mod: DataModifier,
    ) -> (PropertyCardinality, SerdeOperatorId) {
        // FIXME: The variant of the property depends on what kind of map we are building?
        // Anyway; when requesting a property, the data variant should be "reset".

        match cardinality.1 {
            ValueCardinality::One => (
                cardinality.0,
                self.get_serde_operator_id(SerdeKey::Def(DefVariant {
                    def_id: type_def_id,
                    local_mod: global_mod,
                    global_mod,
                }))
                .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.get_serde_operator_id(SerdeKey::Def(DefVariant {
                    def_id: type_def_id,
                    local_mod: global_mod | DataModifier::ARRAY,
                    global_mod,
                }))
                .expect("no property operator"),
            ),
        }
    }

    fn alloc_operator_id(&mut self, def_variant: &DefVariant) -> SerdeOperatorId {
        self.alloc_operator_id_for_key(&SerdeKey::Def(*def_variant))
    }

    fn alloc_operator_id_for_key(&mut self, key: &SerdeKey) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.operators_by_id.len() as u32);
        // We just need a temporary placeholder for this operator,
        // this will be properly overwritten afterwards:
        self.operators_by_id.push(SerdeOperator::Unit);
        self.operators_by_key.insert(key.clone(), operator_id);
        operator_id
    }

    fn create_serde_operator_from_key(&mut self, key: SerdeKey) -> Option<OperatorAllocation> {
        match &key {
            SerdeKey::Intersection(keys) => {
                debug!("create intersection: {keys:?}");

                let operator_id = self.alloc_operator_id_for_key(&key);
                let mut iterator = keys.iter();
                let first_id = self.get_serde_operator_id(iterator.next()?.clone())?;

                let mut intersected_map = self
                    .find_unambiguous_map_operator(first_id)
                    .unwrap_or_else(|operator| {
                        panic!("Initial map not found for intersection: {operator:?}")
                    })
                    .clone();

                for next_key in iterator {
                    let next_id = self.get_serde_operator_id(next_key.clone()).unwrap();

                    if let Ok(next_map_type) = self.find_unambiguous_map_operator(next_id) {
                        for (key, value) in &next_map_type.properties {
                            intersected_map.properties.insert(key.clone(), *value);
                        }
                    }
                }

                Some(OperatorAllocation::Allocated(
                    operator_id,
                    SerdeOperator::Map(intersected_map),
                ))
            }
            SerdeKey::Def(def_variant) if def_variant.local_mod == DataModifier::ID => {
                match self.get_def_type(def_variant.def_id)? {
                    Type::Domain(_) => {
                        let id_relation_id = self
                            .relations
                            .properties_by_type
                            .get(&def_variant.def_id)?
                            .id?;

                        let (relationship, _) = self
                            .get_subject_property_meta(def_variant.def_id, id_relation_id)
                            .expect("Problem getting subject property meta");
                        let object = relationship.object;

                        let object_operator_id = self
                            .get_serde_operator_id(SerdeKey::identity(object.0))
                            .expect("No object operator for _id property");

                        Some(OperatorAllocation::Allocated(
                            self.alloc_operator_id_for_key(&key),
                            SerdeOperator::Id(object_operator_id),
                        ))
                    }
                    _ => None,
                }
            }
            SerdeKey::Def(def_variant) if def_variant.local_mod.contains(DataModifier::ARRAY) => {
                let item_operator_id = self.get_serde_operator_id(SerdeKey::Def(
                    def_variant.minus_local_mod(DataModifier::ARRAY),
                ))?;

                Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id_for_key(&key),
                    SerdeOperator::RelationSequence(RelationSequenceOperator {
                        ranges: [SequenceRange {
                            operator_id: item_operator_id,
                            finite_repetition: None,
                        }],
                        def_variant: *def_variant,
                    }),
                ))
            }
            SerdeKey::Def(def_variant) => self.create_item_operator(*def_variant),
        }
    }

    fn find_unambiguous_map_operator(
        &self,
        id: SerdeOperatorId,
    ) -> Result<&MapOperator, &SerdeOperator> {
        let operator = &self.operators_by_id[id.0 as usize];
        match operator {
            SerdeOperator::Map(map_op) => Ok(map_op),
            SerdeOperator::Union(union_op) => {
                let mut map_count = 0;
                let mut result = Err(operator);

                for discriminator in union_op.unfiltered_variants() {
                    if let Ok(map_type) =
                        self.find_unambiguous_map_operator(discriminator.operator_id)
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

    fn create_item_operator(&mut self, def_variant: DefVariant) -> Option<OperatorAllocation> {
        match self.get_def_type(def_variant.def_id) {
            Some(Type::Domain(def_id)) => {
                let properties = self.relations.properties_by_type.get(def_id);
                let typename = match self.defs.get_def_kind(*def_id) {
                    Some(DefKind::DomainType(Some(ident))) => ident,
                    Some(DefKind::DomainType(None)) => "anonymous",
                    _ => "Unknown type",
                };
                self.create_domain_type_serde_operator(
                    def_variant.with_def(*def_id),
                    typename,
                    properties,
                )
            }
            Some(type_ref) => match def_variant.local_mod {
                DataModifier::IDENTITY => {
                    self.create_core_type_serde_operator(def_variant, type_ref)
                }
                _ => Some(OperatorAllocation::Redirect(DefVariant::new(
                    def_variant.def_id,
                    DataModifier::IDENTITY,
                ))),
            },
            None => panic!("no type available"),
        }
    }

    fn create_core_type_serde_operator(
        &mut self,
        def_variant: DefVariant,
        type_ref: TypeRef,
    ) -> Option<OperatorAllocation> {
        match type_ref {
            Type::Unit(_) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::Unit,
            )),
            Type::IntConstant(_) => todo!(),
            Type::Int(_) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::Int(def_variant.def_id),
            )),
            Type::Number(_) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::Number(def_variant.def_id),
            )),
            Type::String(_) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::String(def_variant.def_id),
            )),
            Type::StringConstant(def_id) => {
                assert_eq!(def_variant.def_id, *def_id);

                let literal = self.defs.get_string_representation(*def_id);

                Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::StringConstant(literal.into(), def_variant.def_id),
                ))
            }
            Type::Regex(def_id) => {
                assert_eq!(def_variant.def_id, *def_id);
                assert!(self
                    .patterns
                    .string_patterns
                    .contains_key(&def_variant.def_id));

                Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::StringPattern(*def_id),
                ))
            }
            Type::Uuid(_) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::String(def_variant.def_id),
            )),
            Type::DateTime(_) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::String(def_variant.def_id),
            )),
            Type::EmptySequence(_) => {
                todo!("not sure if this should be handled here")
            }
            Type::Array(_) => {
                panic!("Array not handled here")
            }
            Type::Option(_) => {
                panic!("Option not handled here")
            }
            Type::Domain(_) => {
                panic!("Domain not handled here")
            }
            Type::Function { .. } => None,
            Type::Anonymous(_) => None,
            Type::Package | Type::BuiltinRelation => None,
            Type::Tautology | Type::Infer(_) | Type::Error => {
                panic!("crap: {:?}", self.get_def_type(def_variant.def_id));
            }
        }
    }

    fn create_domain_type_serde_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: Option<&Properties>,
    ) -> Option<OperatorAllocation> {
        let properties = match (properties, def_variant.local_mod) {
            (None, DataModifier::IDENTITY) => {
                return Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::Map(MapOperator {
                        typename: typename.into(),
                        def_variant,
                        properties: Default::default(),
                        n_mandatory_properties: 0,
                    }),
                ));
            }
            (None, _) => {
                return Some(OperatorAllocation::Redirect(DefVariant::new(
                    def_variant.def_id,
                    DataModifier::IDENTITY,
                )))
            }
            (Some(properties), _) => properties,
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
                    self.get_property_operator(value_def, *cardinality, def_variant.global_mod);

                if !matches!(requirement, PropertyCardinality::Mandatory) {
                    panic!("Value properties must be mandatory, fix this during type check");
                }

                let operator_id = self.alloc_operator_id(&def_variant);

                Some(OperatorAllocation::Allocated(
                    operator_id,
                    SerdeOperator::ValueType(ValueOperator {
                        typename: typename.into(),
                        def_variant,
                        inner_operator_id,
                    }),
                ))
            }
            Constructor::ValueUnion(_) => {
                let operator_id = self.alloc_operator_id(&def_variant);
                Some(OperatorAllocation::Allocated(
                    operator_id,
                    if def_variant.local_mod.contains(DataModifier::UNION) {
                        self.create_union_operator(def_variant, typename, properties)
                    } else {
                        // just the inherent properties are requested.
                        // Don't build a union
                        self.create_map_operator(def_variant, typename, properties)
                    },
                ))
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

                            self.get_serde_operator_id(SerdeKey::Def(
                                def_variant.with_def(relationship.object.0),
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

                let ranges = sequence_range_builder.build();
                debug!("sequence ranges: {:#?}", ranges);

                let operator_id = self.alloc_operator_id(&def_variant);
                Some(OperatorAllocation::Allocated(
                    operator_id,
                    SerdeOperator::ConstructorSequence(ConstructorSequenceOperator {
                        ranges,
                        def_variant,
                    }),
                ))
            }
            Constructor::StringPattern(_) => {
                assert!(self
                    .patterns
                    .string_patterns
                    .contains_key(&def_variant.def_id));

                Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::CapturingStringPattern(def_variant.def_id),
                ))
            }
        }
    }

    fn create_identity_constructor_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> Option<OperatorAllocation> {
        let union_id = DataModifier::UNION | DataModifier::ID;

        if def_variant.local_mod.contains(union_id) {
            let id_relation_id = match properties.id {
                Some(id) => id,
                None => {
                    return Some(OperatorAllocation::Redirect(
                        def_variant.minus_local_mod(union_id),
                    ));
                }
            };
            let (relationship, _) = self
                .get_subject_property_meta(def_variant.def_id, id_relation_id)
                .expect("Problem getting subject property meta");

            let id_def_variant = def_variant.with_local_mod(DataModifier::ID);
            let map_def_variant = def_variant.minus_local_mod(union_id);

            // Create a union between { '_id' } and the map properties itself
            let id_operator_id = self
                .get_serde_operator_id(SerdeKey::Def(id_def_variant))
                .expect("No _id operator");
            let map_properties_operator_id = self
                .get_serde_operator_id(SerdeKey::Def(map_def_variant))
                .expect("No property map operator");

            let operator_id = self.alloc_operator_id(&def_variant);

            Some(OperatorAllocation::Allocated(
                operator_id,
                SerdeOperator::Union(UnionOperator::new(
                    typename.into(),
                    def_variant,
                    vec![
                        ValueUnionVariant {
                            discriminator: VariantDiscriminator {
                                discriminant: Discriminant::IsSingletonProperty(
                                    id_relation_id,
                                    "_id".into(),
                                ),
                                purpose: VariantPurpose::Identification,
                                def_variant: DefVariant::new(
                                    relationship.object.0,
                                    DataModifier::IDENTITY,
                                ),
                            },
                            operator_id: id_operator_id,
                        },
                        ValueUnionVariant {
                            discriminator: VariantDiscriminator {
                                discriminant: Discriminant::MapFallback,
                                purpose: VariantPurpose::Data,
                                def_variant: map_def_variant,
                            },
                            operator_id: map_properties_operator_id,
                        },
                    ],
                )),
            ))
        } else {
            let operator_id = self.alloc_operator_id(&def_variant);
            Some(OperatorAllocation::Allocated(
                operator_id,
                self.create_map_operator(def_variant, typename, properties),
            ))
        }
    }

    fn create_union_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> SerdeOperator {
        let union_disciminator = self
            .relations
            .union_discriminators
            .get(&def_variant.def_id)
            .expect("no union discriminator available. Should fail earlier");

        let mut union_builder = UnionBuilder::new(def_variant);
        let mut root_types: HashSet<DefId> = Default::default();

        for root_discriminator in &union_disciminator.variants {
            union_builder
                .add_root_discriminator(self, root_discriminator)
                .expect("Could not add root discriminator to union builder");

            root_types.insert(root_discriminator.def_variant.def_id);
        }

        let variants: Vec<_> = if properties.map.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_key =
                SerdeKey::Def(def_variant.with_local_mod(DataModifier::PROPS));

            union_builder
                .build(self, |this, operator_id, result_type| {
                    if root_types.contains(&result_type) {
                        // Make the intersection:
                        this.get_serde_operator_id(SerdeKey::Intersection(Box::new(
                            [
                                inherent_properties_key.clone(),
                                SerdeKey::Def(def_variant.with_def(result_type)),
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

        SerdeOperator::Union(UnionOperator::new(typename.into(), def_variant, variants))
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
                            .get_subject_property_meta(def_variant.def_id, property_id.relation_id)
                            .expect("Problem getting subject property meta");
                        let object = relationship.object;

                        let prop_key = relation
                            .subject_prop(self.defs)
                            .expect("Subject property has no name");

                        (relationship, prop_key, object.0)
                    }
                    Role::Object => {
                        let (relationship, relation) = self
                            .get_object_property_meta(def_variant.def_id, property_id.relation_id)
                            .expect("Problem getting object property meta");
                        let subject = relationship.subject;

                        let prop_key = relation
                            .object_prop(self.defs)
                            .expect("Object property has no name");

                        (relationship, prop_key, subject.0)
                    }
                };

                let (property_cardinality, value_operator_id) =
                    self.get_property_operator(type_def_id, *cardinality, def_variant.global_mod);

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

        SerdeOperator::Map(MapOperator {
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
