use std::collections::{BTreeSet, HashMap, HashSet};

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    ontology::{Cardinality, PropertyCardinality, ValueCardinality},
    serde::operator::{
        AliasOperator, ConstructorSequenceOperator, RelationSequenceOperator, SequenceRange,
        SerdeOperator, SerdeOperatorId, SerdeProperty, StructOperator, UnionOperator,
        ValueUnionVariant,
    },
    serde::{operator::SerdePropertyFlags, SerdeKey},
    value_generator::ValueGenerator,
    DataModifier, DefId, DefVariant, Role,
};
use tracing::{debug, trace};

use crate::{
    codegen::task::CodegenTasks,
    compiler_queries::GetDefType,
    def::{DefKind, Defs, LookupRelationshipMeta, RelParams, TypeDef},
    patterns::{Patterns, StringPatternSegment},
    primitive::{PrimitiveKind, Primitives},
    relation::{Constructor, Properties, Relations},
    serde_codegen::sequence_range_builder::SequenceRangeBuilder,
    type_check::{
        repr::repr_model::{ReprKind, ReprScalarKind},
        seal::SealCtx,
    },
    types::{DefTypes, Type, TypeRef},
    SourceSpan,
};

use super::union_builder::UnionBuilder;

pub struct SerdeGenerator<'c, 'm> {
    pub(super) defs: &'c Defs<'m>,
    pub(super) primitives: &'c Primitives,
    pub(super) def_types: &'c DefTypes<'m>,
    pub(super) relations: &'c Relations,
    pub(super) seal_ctx: &'c SealCtx,
    pub(super) patterns: &'c Patterns,
    pub(super) codegen_tasks: &'c CodegenTasks<'m>,
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

    pub fn make_dynamic_sequence_operator(&mut self) -> SerdeOperatorId {
        let operator_id = SerdeOperatorId(self.operators_by_id.len() as u32);
        self.operators_by_id.push(SerdeOperator::DynamicSequence);
        operator_id
    }

    pub fn gen_operator_id(&mut self, mut key: SerdeKey) -> Option<SerdeOperatorId> {
        let mut discarded_keys = vec![];

        loop {
            if let Some(id) = self.operators_by_key.get(&key) {
                return Some(*id);
            }

            match self.alloc_serde_operator_from_key(key.clone()) {
                Some(OperatorAllocation::Allocated(operator_id, operator)) => {
                    trace!("CREATED {operator_id:?} {key:?} {operator:?}");
                    self.operators_by_id[operator_id.0 as usize] = operator;

                    for key in discarded_keys {
                        self.operators_by_key.insert(key, operator_id);
                    }

                    return Some(operator_id);
                }
                Some(OperatorAllocation::Redirect(def_variant)) => {
                    discarded_keys.push(key);
                    key = SerdeKey::Def(def_variant);
                }
                None => return None,
            }
        }
    }

    pub fn gen_operator(&mut self, key: SerdeKey) -> Option<&SerdeOperator> {
        let operator_id = self.gen_operator_id(key)?;
        Some(&self.operators_by_id[operator_id.0 as usize])
    }

    fn get_property_operator(
        &mut self,
        type_def_id: DefId,
        cardinality: Cardinality,
    ) -> (PropertyCardinality, SerdeOperatorId) {
        match cardinality.1 {
            ValueCardinality::One => (
                cardinality.0,
                self.gen_operator_id(SerdeKey::Def(DefVariant {
                    def_id: type_def_id,
                    modifier: DataModifier::default(),
                }))
                .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.gen_operator_id(SerdeKey::Def(DefVariant {
                    def_id: type_def_id,
                    modifier: DataModifier::default() | DataModifier::ARRAY,
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

    fn alloc_serde_operator_from_key(&mut self, key: SerdeKey) -> Option<OperatorAllocation> {
        match &key {
            SerdeKey::Intersection(keys) => {
                debug!("create intersection: {keys:?}");

                let operator_id = self.alloc_operator_id_for_key(&key);
                let mut iterator = keys.iter();
                let first_id = self.gen_operator_id(iterator.next()?.clone())?;

                let mut intersected_map = self
                    .find_unambiguous_struct_operator(first_id)
                    .unwrap_or_else(|operator| {
                        panic!("Initial map not found for intersection: {operator:?}")
                    })
                    .clone();

                for next_key in iterator {
                    let next_id = self.gen_operator_id(next_key.clone()).unwrap();

                    if let Ok(next_map_type) = self.find_unambiguous_struct_operator(next_id) {
                        for (key, value) in &next_map_type.properties {
                            intersected_map.properties.insert(key.clone(), *value);
                        }
                    }
                }

                Some(OperatorAllocation::Allocated(
                    operator_id,
                    SerdeOperator::Struct(intersected_map),
                ))
            }
            SerdeKey::Def(def_variant) if def_variant.modifier == DataModifier::PRIMARY_ID => {
                let table = self
                    .relations
                    .properties_by_def_id
                    .get(&def_variant.def_id)?
                    .table
                    .as_ref()?;

                let (property_id, _) = table.iter().find(|(_, property)| property.is_entity_id)?;

                let meta = self.defs.relationship_meta(property_id.relationship_id);

                let DefKind::StringLiteral(property_name) = *meta.relation_def_kind.value else {
                    return None;
                };

                let object_operator_id = self
                    .gen_operator_id(SerdeKey::no_modifier(meta.relationship.object.0))
                    .expect("No object operator for primary id property");

                Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id_for_key(&key),
                    SerdeOperator::PrimaryId(property_name.into(), object_operator_id),
                ))
            }
            SerdeKey::Def(def_variant) if def_variant.modifier.contains(DataModifier::ARRAY) => {
                let item_operator_id = self.gen_operator_id(SerdeKey::Def(
                    def_variant.remove_modifier(DataModifier::ARRAY),
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
            SerdeKey::Def(def_variant) => self.alloc_def_type_operator(*def_variant),
        }
    }

    fn find_unambiguous_struct_operator(
        &self,
        id: SerdeOperatorId,
    ) -> Result<&StructOperator, &SerdeOperator> {
        let operator = &self.operators_by_id[id.0 as usize];
        match operator {
            SerdeOperator::Struct(struct_op) => Ok(struct_op),
            SerdeOperator::Union(union_op) => {
                let mut map_count = 0;
                let mut result = Err(operator);

                for discriminator in union_op.unfiltered_variants() {
                    if let Ok(map_type) =
                        self.find_unambiguous_struct_operator(discriminator.operator_id)
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
            SerdeOperator::Alias(value_op) => {
                self.find_unambiguous_struct_operator(value_op.inner_operator_id)
            }
            _ => Err(operator),
        }
    }

    fn alloc_def_type_operator(&mut self, def_variant: DefVariant) -> Option<OperatorAllocation> {
        match self.get_def_type(def_variant.def_id) {
            Some(Type::Domain(def_id) | Type::Anonymous(def_id)) => {
                let properties = self.relations.properties_by_def_id.get(def_id);
                let typename = match self.defs.def_kind(*def_id) {
                    DefKind::Type(TypeDef {
                        ident: Some(ident), ..
                    }) => ident,
                    DefKind::Type(TypeDef { ident: None, .. }) => "<anonymous>",
                    _ => "Unknown type",
                };
                self.alloc_domain_type_serde_operator(
                    def_variant.with_def(*def_id),
                    typename,
                    properties,
                )
            }
            Some(type_ref) => match def_variant.modifier {
                DataModifier::NONE => self.alloc_ontol_type_serde_operator(def_variant, type_ref),
                _ => Some(OperatorAllocation::Redirect(DefVariant::new(
                    def_variant.def_id,
                    DataModifier::NONE,
                ))),
            },
            None => panic!(
                "no type available for {def_variant:?} ({:?})",
                self.defs.def_kind(def_variant.def_id)
            ),
        }
    }

    fn alloc_ontol_type_serde_operator(
        &mut self,
        def_variant: DefVariant,
        type_ref: TypeRef,
    ) -> Option<OperatorAllocation> {
        match type_ref {
            Type::Primitive(kind, _) => match kind {
                PrimitiveKind::Unit => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::Unit,
                )),
                PrimitiveKind::Boolean => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::Boolean(def_variant.def_id),
                )),
                PrimitiveKind::False => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::False(def_variant.def_id),
                )),
                PrimitiveKind::True => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::True(def_variant.def_id),
                )),
                PrimitiveKind::Number => None,
                PrimitiveKind::Integer => None,
                PrimitiveKind::I64 => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::I64(def_variant.def_id, None),
                )),
                PrimitiveKind::Float => None,
                // TODO: f32
                PrimitiveKind::F32 => None,
                PrimitiveKind::F64 => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::F64(def_variant.def_id, None),
                )),
                PrimitiveKind::String => Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::String(def_variant.def_id),
                )),
            },
            Type::IntConstant(_) | Type::FloatConstant(_) => todo!(),
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
            Type::StringLike(_, _) => Some(OperatorAllocation::Allocated(
                self.alloc_operator_id(&def_variant),
                SerdeOperator::StringPattern(def_variant.def_id),
            )),
            Type::EmptySequence(_) => {
                todo!("not sure if this should be handled here")
            }
            Type::Seq(..) => {
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
            Type::Package | Type::BuiltinRelation | Type::ValueGenerator(_) => None,
            Type::Tautology | Type::Infer(_) | Type::Error => {
                panic!("crap: {:?}", self.get_def_type(def_variant.def_id));
            }
        }
    }

    fn alloc_domain_type_serde_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: Option<&Properties>,
    ) -> Option<OperatorAllocation> {
        let repr = self.seal_ctx.repr_table.get(&def_variant.def_id)?;

        let properties = match (properties, def_variant.modifier) {
            (None, DataModifier::NONE) => {
                return Some(OperatorAllocation::Allocated(
                    self.alloc_operator_id(&def_variant),
                    SerdeOperator::Struct(StructOperator {
                        typename: typename.into(),
                        def_variant,
                        properties: Default::default(),
                    }),
                ));
            }
            (None, _) => {
                return Some(OperatorAllocation::Redirect(DefVariant::new(
                    def_variant.def_id,
                    DataModifier::NONE,
                )))
            }
            (Some(properties), _) => properties,
        };

        match &repr.kind {
            ReprKind::Scalar(def_id, scalar_kind, _span) => {
                match scalar_kind {
                    ReprScalarKind::I64(range) => {
                        return Some(OperatorAllocation::Allocated(
                            self.alloc_operator_id(&def_variant),
                            SerdeOperator::I64(
                                *def_id,
                                if *range == (i64::MIN..=i64::MAX) {
                                    None
                                } else {
                                    Some(range.clone())
                                },
                            ),
                        ));
                    }
                    ReprScalarKind::F64(range) => {
                        let f64_range = range.start().into_inner()..=range.end().into_inner();
                        return Some(OperatorAllocation::Allocated(
                            self.alloc_operator_id(&def_variant),
                            SerdeOperator::F64(
                                *def_id,
                                if f64_range == (f64::MIN..=f64::MAX) {
                                    None
                                } else {
                                    Some(f64_range.clone())
                                },
                            ),
                        ));
                    }
                    _ => {
                        if def_id == &def_variant.def_id {
                            // If it's a "self-scalar" it must be a string fmt (for now).
                            if let Constructor::StringFmt(segment) = &properties.constructor {
                                return self.alloc_string_fmt_operator(def_variant, segment);
                            }

                            panic!("Self-scalar without fmt: {def_id:?}");
                        } else {
                            let (requirement, inner_operator_id) = self.get_property_operator(
                                *def_id,
                                (PropertyCardinality::Mandatory, ValueCardinality::One),
                            );

                            if !matches!(requirement, PropertyCardinality::Mandatory) {
                                panic!("Scalar cardinality must be mandatory, fix this during type check");
                            }

                            let operator_id = self.alloc_operator_id(&def_variant);

                            return Some(OperatorAllocation::Allocated(
                                operator_id,
                                SerdeOperator::Alias(AliasOperator {
                                    typename: typename.into(),
                                    def_variant,
                                    inner_operator_id,
                                }),
                            ));
                        }
                    }
                }
            }
            ReprKind::Unit | ReprKind::Struct => {
                return self.alloc_struct_constructor_operator(def_variant, typename, properties);
            }
            ReprKind::StructIntersection(members) => {
                if members.len() == 1 {
                    let (member_def_id, _) = members.first().unwrap();
                    if member_def_id == &def_variant.def_id {
                        return self.alloc_struct_constructor_operator(
                            def_variant,
                            typename,
                            properties,
                        );
                    }
                } else if members.is_empty() {
                    return self.alloc_struct_constructor_operator(
                        def_variant,
                        typename,
                        properties,
                    );
                }

                return self.alloc_struct_intersection_operator(
                    def_variant,
                    typename,
                    properties,
                    members.as_slice(),
                );
            }
            ReprKind::Union(_) | ReprKind::StructUnion(_) => {
                let operator_id = self.alloc_operator_id(&def_variant);
                return Some(OperatorAllocation::Allocated(
                    operator_id,
                    if def_variant.modifier.contains(DataModifier::UNION) {
                        self.create_union_operator(def_variant, typename, properties)
                    } else {
                        // just the inherent properties are requested.
                        // Don't build a union
                        self.create_struct_operator(def_variant, typename, properties)
                    },
                ));
            }
            _ => {}
        }

        match &properties.constructor {
            Constructor::Sequence(sequence) => {
                let mut sequence_range_builder = SequenceRangeBuilder::default();

                let mut element_iterator = sequence.elements().peekable();

                while let Some((_, element)) = element_iterator.next() {
                    let operator_id = match element {
                        None => self
                            .gen_operator_id(SerdeKey::no_modifier(DefId::unit()))
                            .unwrap(),
                        Some(relationship_id) => {
                            let meta = self.defs.relationship_meta(relationship_id);

                            self.gen_operator_id(SerdeKey::Def(
                                def_variant.with_def(meta.relationship.object.0),
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
            constructor => {
                unreachable!("{:?}: {constructor:?}: repr {repr:?}", def_variant.def_id)
            }
        }
    }

    fn alloc_struct_constructor_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> Option<OperatorAllocation> {
        let union_id = DataModifier::UNION | DataModifier::PRIMARY_ID;

        if def_variant.modifier.contains(union_id) {
            let Some(identifies_relationship_id) = properties.identified_by else {
                return Some(OperatorAllocation::Redirect(
                    def_variant.remove_modifier(union_id),
                ));
            };
            let Some(id_operator_id) = self.gen_operator_id(SerdeKey::Def(
                def_variant.with_local_mod(DataModifier::PRIMARY_ID),
            )) else {
                // This type has no inherent id
                return Some(OperatorAllocation::Redirect(
                    def_variant.remove_modifier(union_id),
                ));
            };

            let map_def_variant = def_variant.remove_modifier(union_id);
            let identifies_meta = self.defs.relationship_meta(identifies_relationship_id);

            // prevent recursion
            let new_operator_id = self.alloc_operator_id(&def_variant);

            // Create a union between { '_id' } and the map properties itself
            let map_properties_operator_id = self
                .gen_operator_id(SerdeKey::Def(map_def_variant))
                .expect("No property map operator");

            let id_property_name =
                match self.operators_by_id.get(id_operator_id.0 as usize).unwrap() {
                    SerdeOperator::PrimaryId(id_property_name, _) => id_property_name.clone(),
                    other => panic!("id operator was not an Id: {other:?}"),
                };

            Some(OperatorAllocation::Allocated(
                new_operator_id,
                SerdeOperator::Union(UnionOperator::new(
                    typename.into(),
                    def_variant,
                    vec![
                        ValueUnionVariant {
                            discriminator: VariantDiscriminator {
                                discriminant: Discriminant::IsSingletonProperty(
                                    identifies_relationship_id,
                                    id_property_name,
                                ),
                                purpose: VariantPurpose::Identification,
                                def_variant: DefVariant::new(
                                    identifies_meta.relationship.subject.0,
                                    DataModifier::NONE,
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
            // prevent recursion
            let new_operator_id = self.alloc_operator_id(&def_variant);

            Some(OperatorAllocation::Allocated(
                new_operator_id,
                self.create_struct_operator(def_variant, typename, properties),
            ))
        }
    }

    fn alloc_struct_intersection_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
        members: &[(DefId, SourceSpan)],
    ) -> Option<OperatorAllocation> {
        let modifier = &def_variant.modifier;

        if members.is_empty() {
            Some(OperatorAllocation::Redirect(
                def_variant.remove_modifier(DataModifier::INTERSECTION),
            ))
        } else if modifier.contains(DataModifier::INTERSECTION) {
            if members.len() == 1 && properties.table.is_none() {
                Some(OperatorAllocation::Redirect(def_variant.remove_modifier(
                    DataModifier::INTERSECTION | DataModifier::INHERENT_PROPS,
                )))
            } else if members.len() == 1 {
                self.alloc_serde_operator_from_key(SerdeKey::Intersection(Box::new(
                    [
                        // Require intersected properties via [is] relation:
                        SerdeKey::Def(def_variant.remove_modifier(
                            DataModifier::INTERSECTION | DataModifier::INHERENT_PROPS,
                        )),
                        // Require inherent propserties:
                        SerdeKey::Def(def_variant.remove_modifier(DataModifier::INTERSECTION)),
                    ]
                    .into(),
                )))
            } else {
                let mut intersection_keys = BTreeSet::new();

                if properties.table.is_some() {
                    // inherent properties:
                    intersection_keys.insert(SerdeKey::Def(DefVariant::new(
                        def_variant.def_id,
                        def_variant
                            .remove_modifier(DataModifier::INTERSECTION)
                            .modifier,
                    )));
                }

                for (def_id, _span) in members {
                    intersection_keys.insert(SerdeKey::Def(DefVariant::new(
                        *def_id,
                        DataModifier::default(),
                    )));
                }

                self.alloc_serde_operator_from_key(SerdeKey::Intersection(Box::new(
                    intersection_keys,
                )))
            }
        } else if modifier.contains(DataModifier::INHERENT_PROPS) {
            let operator_id = self.alloc_operator_id(&def_variant);
            Some(OperatorAllocation::Allocated(
                operator_id,
                self.create_struct_operator(def_variant, typename, properties),
            ))
        } else {
            let (value_def, _span) = members[0];

            let (requirement, inner_operator_id) = self.get_property_operator(
                value_def,
                (PropertyCardinality::Mandatory, ValueCardinality::One),
            );

            if !matches!(requirement, PropertyCardinality::Mandatory) {
                panic!("Value properties must be mandatory, fix this during type check");
            }

            let operator_id = self.alloc_operator_id(&def_variant);

            Some(OperatorAllocation::Allocated(
                operator_id,
                SerdeOperator::Alias(AliasOperator {
                    typename: typename.into(),
                    def_variant,
                    inner_operator_id,
                }),
            ))
        }
    }

    fn alloc_string_fmt_operator(
        &mut self,
        def_variant: DefVariant,
        segment: &StringPatternSegment,
    ) -> Option<OperatorAllocation> {
        assert!(self
            .patterns
            .string_patterns
            .contains_key(&def_variant.def_id));

        let operator = match segment {
            StringPatternSegment::AllStrings => SerdeOperator::String(def_variant.def_id),
            _ => SerdeOperator::CapturingStringPattern(def_variant.def_id),
        };

        Some(OperatorAllocation::Allocated(
            self.alloc_operator_id(&def_variant),
            operator,
        ))
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

        let variants: Vec<_> = if properties.table.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_key =
                SerdeKey::Def(def_variant.with_local_mod(DataModifier::INHERENT_PROPS));

            union_builder
                .build(self, |this, operator_id, result_type| {
                    if root_types.contains(&result_type) {
                        // Make the intersection:
                        this.gen_operator_id(SerdeKey::Intersection(Box::new(
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

    fn create_struct_operator(
        &mut self,
        def_variant: DefVariant,
        typename: &str,
        properties: &Properties,
    ) -> SerdeOperator {
        let mut serde_properties: IndexMap<_, _> = Default::default();

        if let Some(table) = &properties.table {
            for (property_id, property) in table {
                let (meta, prop_key, type_def_id) = match property_id.role {
                    Role::Subject => {
                        if property_id.relationship_id.0 == self.primitives.relations.identifies {
                            // panic!();
                        }

                        let meta = self.defs.relationship_meta(property_id.relationship_id);
                        let object = meta.relationship.object.0;

                        let DefKind::StringLiteral(prop_key) = meta.relation_def_kind.value else {
                            panic!("Subject property is not a string literal");
                        };

                        (meta, *prop_key, object)
                    }
                    Role::Object => {
                        let meta = self.defs.relationship_meta(property_id.relationship_id);
                        let subject = meta.relationship.subject.0;

                        let prop_key = meta
                            .relationship
                            .object_prop
                            .expect("Object property has no name");

                        (meta, prop_key, subject)
                    }
                };

                let (property_cardinality, value_operator_id) =
                    self.get_property_operator(type_def_id, property.cardinality);

                let rel_params_operator_id = match &meta.relationship.rel_params {
                    RelParams::Type(def_id) => {
                        let key = SerdeKey::Def(DefVariant::new(*def_id, DataModifier::default()));
                        match self.gen_operator(key.clone()) {
                            Some(SerdeOperator::Struct(struct_op)) => {
                                if !struct_op.properties.is_empty() {
                                    self.gen_operator_id(key)
                                } else {
                                    None
                                }
                            }
                            Some(SerdeOperator::Unit) => None,
                            Some(_) => self.gen_operator_id(key),
                            _ => None,
                        }
                    }
                    RelParams::Unit => None,
                    _ => todo!(),
                };

                let mut value_generator: Option<ValueGenerator> = None;
                let mut flags = SerdePropertyFlags::default();

                if let Some(default_const_def) = self
                    .relations
                    .default_const_objects
                    .get(&meta.relationship_id)
                {
                    let proc = self
                        .codegen_tasks
                        .result_const_procs
                        .get(default_const_def)
                        .unwrap_or_else(|| panic!());

                    value_generator = Some(ValueGenerator::DefaultProc(proc.address));
                }

                if let Some(explicit_value_generator) = self
                    .relations
                    .value_generators
                    .get(&property_id.relationship_id)
                {
                    flags |= SerdePropertyFlags::READ_ONLY;
                    if value_generator.is_some() {
                        panic!("BUG: Cannot have both a default value and a generator. Solve this in type check.");
                    }
                    value_generator = Some(*explicit_value_generator);
                }

                if property_cardinality.is_optional() {
                    flags |= SerdePropertyFlags::OPTIONAL;
                }

                if property.is_entity_id {
                    flags |= SerdePropertyFlags::ENTITY_ID;
                }

                serde_properties.insert(
                    prop_key.into(),
                    SerdeProperty {
                        property_id: *property_id,
                        value_operator_id,
                        flags,
                        value_generator,
                        rel_params_operator_id,
                    },
                );
            }
        };

        SerdeOperator::Struct(StructOperator {
            typename: typename.into(),
            def_variant,
            properties: serde_properties,
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
