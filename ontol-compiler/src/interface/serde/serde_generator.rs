use std::{
    collections::{BTreeSet, HashMap, HashSet},
    ops::RangeInclusive,
};

use indexmap::IndexMap;
use ontol_runtime::{
    interface::discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    interface::serde::operator::{
        AliasOperator, ConstructorSequenceOperator, RelationSequenceOperator, SequenceRange,
        SerdeOperator, SerdeOperatorAddr, SerdeProperty, StructOperator, UnionOperator,
        ValueUnionVariant,
    },
    interface::{
        discriminator::LeafDiscriminant,
        serde::{
            operator::{SerdePropertyFlags, SerdeStructFlags},
            SerdeDef, SerdeKey, SerdeModifier,
        },
    },
    ontology::{Cardinality, PropertyCardinality, ValueCardinality},
    smart_format,
    value_generator::ValueGenerator,
    DefId, Role,
};
use smartstring::alias::String;
use tracing::{debug, error, trace, warn};

use super::sequence_range_builder::SequenceRangeBuilder;

use crate::{
    codegen::task::CodegenTasks,
    compiler_queries::GetDefType,
    def::{DefKind, Defs, LookupRelationshipMeta, RelParams, TypeDef},
    interface::graphql::graphql_namespace::{adapt_graphql_identifier, GqlAdaptedIdent},
    primitive::{PrimitiveKind, Primitives},
    relation::{Constructor, Properties, Relations},
    text_patterns::{TextPatternSegment, TextPatterns},
    type_check::{
        repr::repr_model::{ReprKind, ReprScalarKind},
        seal::SealCtx,
    },
    types::{DefTypes, Type, TypeRef},
    SourceSpan,
};

use super::union_builder::UnionBuilder;

pub struct SerdeGenerator<'c, 'm> {
    pub defs: &'c Defs<'m>,
    pub primitives: &'c Primitives,
    pub def_types: &'c DefTypes<'m>,
    pub relations: &'c Relations,
    pub seal_ctx: &'c SealCtx,
    pub patterns: &'c TextPatterns,
    pub codegen_tasks: &'c CodegenTasks<'m>,

    pub(super) operators_by_addr: Vec<SerdeOperator>,
    pub(super) operators_by_key: HashMap<SerdeKey, SerdeOperatorAddr>,
}

enum OperatorAllocation {
    Allocated(SerdeOperatorAddr, SerdeOperator),
    Redirect(SerdeDef),
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(self) -> (Vec<SerdeOperator>, HashMap<SerdeKey, SerdeOperatorAddr>) {
        (self.operators_by_addr, self.operators_by_key)
    }

    pub fn make_dynamic_sequence_addr(&mut self) -> SerdeOperatorAddr {
        let addr = SerdeOperatorAddr(self.operators_by_addr.len() as u32);
        self.operators_by_addr.push(SerdeOperator::DynamicSequence);
        addr
    }

    pub fn gen_addr(&mut self, mut key: SerdeKey) -> Option<SerdeOperatorAddr> {
        let mut discarded_keys = vec![];

        loop {
            if let Some(addr) = self.operators_by_key.get(&key) {
                return Some(*addr);
            }

            match self.alloc_serde_operator_from_key(key.clone()) {
                Some(OperatorAllocation::Allocated(addr, operator)) => {
                    trace!("CREATED {addr:?} {key:?} {operator:?}");
                    self.operators_by_addr[addr.0 as usize] = operator;

                    for key in discarded_keys {
                        self.operators_by_key.insert(key, addr);
                    }

                    return Some(addr);
                }
                Some(OperatorAllocation::Redirect(def)) => {
                    discarded_keys.push(key);
                    key = SerdeKey::Def(def);
                }
                None => return None,
            }
        }
    }

    pub fn get_operator(&self, addr: SerdeOperatorAddr) -> &SerdeOperator {
        &self.operators_by_addr[addr.0 as usize]
    }

    pub fn gen_operator(&mut self, key: SerdeKey) -> Option<&SerdeOperator> {
        let addr = self.gen_addr(key)?;
        Some(&self.operators_by_addr[addr.0 as usize])
    }

    fn get_property_operator(
        &mut self,
        type_def_id: DefId,
        cardinality: Cardinality,
        cross_def_modifier: SerdeModifier,
    ) -> (PropertyCardinality, SerdeOperatorAddr) {
        let default_modifier = SerdeModifier::json_default() | cross_def_modifier;

        match cardinality.1 {
            ValueCardinality::One => (
                cardinality.0,
                self.gen_addr(SerdeKey::Def(SerdeDef {
                    def_id: type_def_id,
                    modifier: default_modifier,
                }))
                .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.gen_addr(SerdeKey::Def(SerdeDef {
                    def_id: type_def_id,
                    modifier: default_modifier | SerdeModifier::ARRAY,
                }))
                .expect("no property operator"),
            ),
        }
    }

    fn alloc_addr(&mut self, def: &SerdeDef) -> SerdeOperatorAddr {
        self.alloc_addr_for_key(&SerdeKey::Def(*def))
    }

    fn alloc_addr_for_key(&mut self, key: &SerdeKey) -> SerdeOperatorAddr {
        let addr = SerdeOperatorAddr(self.operators_by_addr.len() as u32);
        // We just need a temporary placeholder for this operator,
        // this will be properly overwritten afterwards:
        self.operators_by_addr.push(SerdeOperator::Unit);
        self.operators_by_key.insert(key.clone(), addr);
        addr
    }

    fn alloc_serde_operator_from_key(&mut self, key: SerdeKey) -> Option<OperatorAllocation> {
        match &key {
            SerdeKey::Intersection(keys) => {
                debug!("create intersection: {keys:?}");

                let addr = self.alloc_addr_for_key(&key);
                let mut iterator = keys.iter();
                let first_id = self.gen_addr(iterator.next()?.clone())?;

                let mut intersected_map = self
                    .find_unambiguous_struct_operator(first_id)
                    .unwrap_or_else(|operator| {
                        panic!("Initial map not found for intersection: {operator:?}")
                    })
                    .clone();

                for next_key in iterator {
                    let next_id = self.gen_addr(next_key.clone()).unwrap();

                    if let Ok(next_map_type) = self.find_unambiguous_struct_operator(next_id) {
                        for (key, value) in &next_map_type.properties {
                            insert_property(
                                &mut intersected_map.properties,
                                key,
                                *value,
                                match next_key {
                                    SerdeKey::Def(def) => def.modifier,
                                    SerdeKey::Intersection(_) => panic!(),
                                },
                            );
                        }
                    }
                }

                Some(OperatorAllocation::Allocated(
                    addr,
                    SerdeOperator::Struct(intersected_map),
                ))
            }
            SerdeKey::Def(def)
                if def.modifier - SerdeModifier::cross_def_mask() == SerdeModifier::PRIMARY_ID =>
            {
                trace!("Gen primary id: {def:?}");

                let table = self
                    .relations
                    .properties_by_def_id
                    .get(&def.def_id)?
                    .table
                    .as_ref()?;

                let (property_id, _) = table.iter().find(|(_, property)| property.is_entity_id)?;

                let meta = self.defs.relationship_meta(property_id.relationship_id);

                let DefKind::TextLiteral(property_name) = *meta.relation_def_kind.value else {
                    return None;
                };

                let object_addr = self
                    .gen_addr(SerdeKey::Def(SerdeDef::new(
                        meta.relationship.object.0,
                        def.modifier.cross_def_flags(),
                    )))
                    .expect("No object operator for primary id property");

                let (ident, adaption) = make_property_name(property_name, def.modifier);
                if matches!(adaption, IdentAdaption::Verbatim)
                    && def.modifier != SerdeModifier::PRIMARY_ID
                {
                    // Deduplicate
                    Some(OperatorAllocation::Redirect(SerdeDef::new(
                        def.def_id,
                        SerdeModifier::PRIMARY_ID,
                    )))
                } else {
                    Some(OperatorAllocation::Allocated(
                        self.alloc_addr_for_key(&key),
                        SerdeOperator::IdSingletonStruct(ident, object_addr),
                    ))
                }
            }
            SerdeKey::Def(def) if def.modifier.contains(SerdeModifier::ARRAY) => {
                let item_addr =
                    self.gen_addr(SerdeKey::Def(def.remove_modifier(SerdeModifier::ARRAY)))?;

                Some(OperatorAllocation::Allocated(
                    self.alloc_addr_for_key(&key),
                    SerdeOperator::RelationSequence(RelationSequenceOperator {
                        ranges: [SequenceRange {
                            addr: item_addr,
                            finite_repetition: None,
                        }],
                        def: *def,
                    }),
                ))
            }
            SerdeKey::Def(def) => self.alloc_def_type_operator(*def),
        }
    }

    fn find_unambiguous_struct_operator(
        &self,
        id: SerdeOperatorAddr,
    ) -> Result<&StructOperator, &SerdeOperator> {
        let operator = &self.operators_by_addr[id.0 as usize];
        match operator {
            SerdeOperator::Struct(struct_op) => Ok(struct_op),
            SerdeOperator::Union(union_op) => {
                let mut map_count = 0;
                let mut result = Err(operator);

                for discriminator in union_op.unfiltered_variants() {
                    if let Ok(map_type) = self.find_unambiguous_struct_operator(discriminator.addr)
                    {
                        result = Ok(map_type);
                        map_count += 1;
                    } else {
                        let operator = &self.operators_by_addr[discriminator.addr.0 as usize];
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
                self.find_unambiguous_struct_operator(value_op.inner_addr)
            }
            _ => Err(operator),
        }
    }

    fn alloc_def_type_operator(&mut self, def: SerdeDef) -> Option<OperatorAllocation> {
        match self.get_def_type(def.def_id) {
            Some(Type::Domain(def_id) | Type::Anonymous(def_id)) => {
                let properties = self.relations.properties_by_def_id.get(def_id);
                let typename = match self.defs.def_kind(*def_id) {
                    DefKind::Type(TypeDef {
                        ident: Some(ident), ..
                    }) => ident,
                    DefKind::Type(TypeDef { ident: None, .. }) => "<anonymous>",
                    _ => "Unknown type",
                };
                self.alloc_domain_type_serde_operator(def.with_def(*def_id), typename, properties)
            }
            Some(type_ref) => match def.modifier {
                SerdeModifier::NONE => self.alloc_ontol_type_serde_operator(def, type_ref),
                _ => Some(OperatorAllocation::Redirect(SerdeDef::new(
                    def.def_id,
                    SerdeModifier::NONE,
                ))),
            },
            None => panic!(
                "no type available for {def:?} ({:?})",
                self.defs.def_kind(def.def_id)
            ),
        }
    }

    fn alloc_ontol_type_serde_operator(
        &mut self,
        def: SerdeDef,
        type_ref: TypeRef,
    ) -> Option<OperatorAllocation> {
        match type_ref {
            Type::Primitive(kind, _) => match kind {
                PrimitiveKind::Unit => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::Unit,
                )),
                PrimitiveKind::Boolean => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::Boolean(def.def_id),
                )),
                PrimitiveKind::False => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::False(def.def_id),
                )),
                PrimitiveKind::True => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::True(def.def_id),
                )),
                PrimitiveKind::Number => None,
                PrimitiveKind::Integer => None,
                PrimitiveKind::I64 => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::I64(def.def_id, None),
                )),
                PrimitiveKind::Float => None,
                // TODO: f32
                PrimitiveKind::F32 => None,
                PrimitiveKind::F64 => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::F64(def.def_id, None),
                )),
                PrimitiveKind::Text => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::String(def.def_id),
                )),
                PrimitiveKind::OpenDataRelationship => None,
            },
            Type::IntConstant(_) | Type::FloatConstant(_) => todo!(),
            Type::TextConstant(def_id) => {
                assert_eq!(def.def_id, *def_id);

                let literal = self.defs.get_string_representation(*def_id);

                Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::StringConstant(literal.into(), def.def_id),
                ))
            }
            Type::Regex(def_id) => {
                assert_eq!(def.def_id, *def_id);
                assert!(self.patterns.text_patterns.contains_key(&def.def_id));

                Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::TextPattern(*def_id),
                ))
            }
            Type::TextLike(_, _) => Some(OperatorAllocation::Allocated(
                self.alloc_addr(&def),
                SerdeOperator::TextPattern(def.def_id),
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
                panic!("crap: {:?}", self.get_def_type(def.def_id));
            }
        }
    }

    fn alloc_domain_type_serde_operator(
        &mut self,
        def: SerdeDef,
        typename: &str,
        properties: Option<&Properties>,
    ) -> Option<OperatorAllocation> {
        let repr = self.seal_ctx.repr_table.get(&def.def_id)?;

        let Some(properties) = properties else {
            return if matches!(
                def.modifier - SerdeModifier::cross_def_mask(),
                SerdeModifier::NONE
            ) {
                Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::Struct(StructOperator {
                        typename: typename.into(),
                        def,
                        flags: self.struct_flags_from_def_id(def.def_id),
                        properties: Default::default(),
                    }),
                ))
            } else {
                Some(OperatorAllocation::Redirect(SerdeDef::new(
                    def.def_id,
                    def.modifier.cross_def_flags(),
                )))
            };
        };

        match &repr.kind {
            ReprKind::Scalar(def_id, ReprScalarKind::I64(range), _span) => {
                Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    // Use the I32 operator for GraphQL purposes (if the integer range is within i32's range)
                    if *range.start() >= (i32::MIN as i64) && *range.end() <= (i32::MAX as i64) {
                        let range = RangeInclusive::new(*range.start() as i32, *range.end() as i32);
                        SerdeOperator::I32(
                            *def_id,
                            if range == (i32::MIN..=i32::MAX) {
                                None
                            } else {
                                Some(range)
                            },
                        )
                    } else {
                        SerdeOperator::I64(
                            *def_id,
                            if *range == (i64::MIN..=i64::MAX) {
                                None
                            } else {
                                Some(range.clone())
                            },
                        )
                    },
                ))
            }
            ReprKind::Scalar(def_id, ReprScalarKind::F64(range), _span) => {
                let f64_range = range.start().into_inner()..=range.end().into_inner();
                Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::F64(
                        *def_id,
                        if f64_range == (f64::MIN..=f64::MAX) {
                            None
                        } else {
                            Some(f64_range)
                        },
                    ),
                ))
            }
            ReprKind::Scalar(def_id, _kind, _span) => {
                if def_id == &def.def_id {
                    // If it's a "self-scalar" it must be a string fmt (for now).
                    if let Constructor::TextFmt(segment) = &properties.constructor {
                        return self.alloc_string_fmt_operator(def, segment);
                    }

                    panic!("Self-scalar without fmt: {def_id:?}");
                } else {
                    let (requirement, inner_addr) = self.get_property_operator(
                        *def_id,
                        (PropertyCardinality::Mandatory, ValueCardinality::One),
                        def.modifier.cross_def_flags(),
                    );

                    if !matches!(requirement, PropertyCardinality::Mandatory) {
                        panic!("Scalar cardinality must be mandatory, fix this during type check");
                    }

                    let addr = self.alloc_addr(&def);

                    Some(OperatorAllocation::Allocated(
                        addr,
                        SerdeOperator::Alias(AliasOperator {
                            typename: typename.into(),
                            def,
                            inner_addr,
                        }),
                    ))
                }
            }
            ReprKind::Unit | ReprKind::Struct => {
                self.alloc_struct_constructor_operator(def, typename, properties)
            }
            ReprKind::StructIntersection(members) => {
                if members.len() == 1 {
                    let (member_def_id, _) = members.first().unwrap();
                    if member_def_id == &def.def_id {
                        return self.alloc_struct_constructor_operator(def, typename, properties);
                    }
                } else if members.is_empty() {
                    return self.alloc_struct_constructor_operator(def, typename, properties);
                }

                self.alloc_struct_intersection_operator(
                    def,
                    typename,
                    properties,
                    members.as_slice(),
                )
            }
            ReprKind::Union(members) | ReprKind::StructUnion(members) => {
                let addr = self.alloc_addr(&def);
                Some(OperatorAllocation::Allocated(
                    addr,
                    if def.modifier.contains(SerdeModifier::UNION) {
                        self.create_union_operator(def, typename, properties)
                    } else {
                        // just the inherent properties are requested.
                        // Don't build a union
                        self.create_struct_operator(def, typename, properties, {
                            let mut flags = SerdeStructFlags::empty();
                            for (def_id, _) in members {
                                flags |= self.struct_flags_from_def_id(*def_id);
                            }
                            flags
                        })
                    },
                ))
            }
            ReprKind::Seq | ReprKind::Intersection(_) => match &properties.constructor {
                Constructor::Sequence(sequence) => {
                    let mut sequence_range_builder = SequenceRangeBuilder::default();

                    let mut element_iterator = sequence.elements().peekable();

                    while let Some((_, element)) = element_iterator.next() {
                        let addr = match element {
                            None => self
                                .gen_addr(SerdeKey::Def(SerdeDef::new(
                                    DefId::unit(),
                                    def.modifier.cross_def_flags(),
                                )))
                                .unwrap(),
                            Some(relationship_id) => {
                                let meta = self.defs.relationship_meta(relationship_id);

                                self.gen_addr(SerdeKey::Def(
                                    def.with_def(meta.relationship.object.0),
                                ))
                                .expect("no inner operator")
                            }
                        };

                        if element_iterator.peek().is_some() || !sequence.is_infinite() {
                            sequence_range_builder.push_required_operator(addr);
                        } else {
                            // last operator is infinite and accepts zero or more items
                            sequence_range_builder.push_infinite_operator(addr);
                        }
                    }

                    let ranges = sequence_range_builder.build();
                    trace!("sequence ranges: {:#?}", ranges);

                    let addr = self.alloc_addr(&def);
                    Some(OperatorAllocation::Allocated(
                        addr,
                        SerdeOperator::ConstructorSequence(ConstructorSequenceOperator {
                            ranges,
                            def,
                        }),
                    ))
                }
                constructor => {
                    unreachable!("{:?}: {constructor:?}: repr {repr:?}", def.def_id)
                }
            },
        }
    }

    fn alloc_struct_constructor_operator(
        &mut self,
        def: SerdeDef,
        typename: &str,
        properties: &Properties,
    ) -> Option<OperatorAllocation> {
        let union_mod = SerdeModifier::UNION | SerdeModifier::PRIMARY_ID;

        if def.modifier.contains(union_mod) {
            let Some(identifies_relationship_id) = properties.identified_by else {
                return Some(OperatorAllocation::Redirect(def.remove_modifier(union_mod)));
            };
            let Some(id_addr) = self.gen_addr(SerdeKey::Def(SerdeDef::new(
                def.def_id,
                SerdeModifier::PRIMARY_ID | def.modifier.cross_def_flags(),
            ))) else {
                // This type has no inherent id
                return Some(OperatorAllocation::Redirect(def.remove_modifier(union_mod)));
            };

            let struct_def = def.remove_modifier(union_mod);
            let identifies_meta = self.defs.relationship_meta(identifies_relationship_id);

            // prevent recursion
            let new_addr = self.alloc_addr(&def);

            // Create a union between { '_id' } and the map properties itself
            let struct_properties_addr = self
                .gen_addr(SerdeKey::Def(struct_def))
                .expect("No property struct operator");

            let (id_property_name, id_leaf_discriminant) =
                match self.operators_by_addr.get(id_addr.0 as usize).unwrap() {
                    SerdeOperator::IdSingletonStruct(id_property_name, inner_addr) => (
                        id_property_name.clone(),
                        if false {
                            operator_to_leaf_discriminant(self.get_operator(*inner_addr))
                        } else {
                            // The point of using IsAny is that as soon as the `ìd_property_name`
                            // matches, the variant has been found. The _value_ matcher
                            // is then decided, without further fallback.
                            // I.e. handled properly by its deserializer.
                            LeafDiscriminant::IsAny
                        },
                    ),
                    other => panic!("id operator was not an Id: {other:?}"),
                };

            Some(OperatorAllocation::Allocated(
                new_addr,
                SerdeOperator::Union(UnionOperator::new(
                    typename.into(),
                    def,
                    vec![
                        ValueUnionVariant {
                            discriminator: VariantDiscriminator {
                                discriminant: Discriminant::HasAttribute(
                                    identifies_relationship_id,
                                    id_property_name,
                                    id_leaf_discriminant,
                                ),
                                purpose: VariantPurpose::Identification {
                                    entity_id: def.def_id,
                                },
                                serde_def: SerdeDef::new(
                                    identifies_meta.relationship.subject.0,
                                    def.modifier.cross_def_flags(),
                                ),
                            },
                            addr: id_addr,
                        },
                        ValueUnionVariant {
                            discriminator: VariantDiscriminator {
                                discriminant: Discriminant::StructFallback,
                                purpose: VariantPurpose::Data,
                                serde_def: struct_def,
                            },
                            addr: struct_properties_addr,
                        },
                    ],
                )),
            ))
        } else {
            // prevent recursion
            let new_addr = self.alloc_addr(&def);
            let flags = self.struct_flags_from_def_id(def.def_id);

            Some(OperatorAllocation::Allocated(
                new_addr,
                self.create_struct_operator(def, typename, properties, flags),
            ))
        }
    }

    fn alloc_struct_intersection_operator(
        &mut self,
        def: SerdeDef,
        typename: &str,
        properties: &Properties,
        members: &[(DefId, SourceSpan)],
    ) -> Option<OperatorAllocation> {
        let modifier = &def.modifier;

        if members.is_empty() {
            Some(OperatorAllocation::Redirect(
                def.remove_modifier(SerdeModifier::INTERSECTION),
            ))
        } else if modifier.contains(SerdeModifier::INTERSECTION) {
            if members.len() == 1 && properties.table.is_none() {
                Some(OperatorAllocation::Redirect(def.remove_modifier(
                    SerdeModifier::INTERSECTION | SerdeModifier::INHERENT_PROPS,
                )))
            } else if members.len() == 1 {
                self.alloc_serde_operator_from_key(SerdeKey::Intersection(Box::new(
                    [
                        // Require intersected properties via [is] relation:
                        SerdeKey::Def(def.remove_modifier(
                            SerdeModifier::INTERSECTION | SerdeModifier::INHERENT_PROPS,
                        )),
                        // Require inherent propserties:
                        SerdeKey::Def(def.remove_modifier(SerdeModifier::INTERSECTION)),
                    ]
                    .into(),
                )))
            } else {
                let mut intersection_keys = BTreeSet::new();

                if properties.table.is_some() {
                    // inherent properties:
                    intersection_keys.insert(SerdeKey::Def(SerdeDef::new(
                        def.def_id,
                        def.remove_modifier(SerdeModifier::INTERSECTION).modifier,
                    )));
                }

                for (def_id, _span) in members {
                    intersection_keys
                        .insert(SerdeKey::Def(SerdeDef::new(*def_id, def.modifier.reset())));
                }

                self.alloc_serde_operator_from_key(SerdeKey::Intersection(Box::new(
                    intersection_keys,
                )))
            }
        } else if modifier.contains(SerdeModifier::INHERENT_PROPS) {
            let addr = self.alloc_addr(&def);
            let mut flags = SerdeStructFlags::empty();
            for (def_id, _) in members {
                flags |= self.struct_flags_from_def_id(*def_id);
            }

            Some(OperatorAllocation::Allocated(
                addr,
                self.create_struct_operator(def, typename, properties, flags),
            ))
        } else {
            let (value_def, _span) = members[0];

            let (requirement, inner_addr) = self.get_property_operator(
                value_def,
                (PropertyCardinality::Mandatory, ValueCardinality::One),
                def.modifier.cross_def_flags(),
            );

            if !matches!(requirement, PropertyCardinality::Mandatory) {
                panic!("Value properties must be mandatory, fix this during type check");
            }

            let addr = self.alloc_addr(&def);

            Some(OperatorAllocation::Allocated(
                addr,
                SerdeOperator::Alias(AliasOperator {
                    typename: typename.into(),
                    def,
                    inner_addr,
                }),
            ))
        }
    }

    fn alloc_string_fmt_operator(
        &mut self,
        def: SerdeDef,
        segment: &TextPatternSegment,
    ) -> Option<OperatorAllocation> {
        assert!(self.patterns.text_patterns.contains_key(&def.def_id));

        let operator = match segment {
            TextPatternSegment::AllStrings => SerdeOperator::String(def.def_id),
            _ => SerdeOperator::CapturingTextPattern(def.def_id),
        };

        Some(OperatorAllocation::Allocated(
            self.alloc_addr(&def),
            operator,
        ))
    }

    fn create_union_operator(
        &mut self,
        def: SerdeDef,
        typename: &str,
        properties: &Properties,
    ) -> SerdeOperator {
        let union_discriminator = self
            .relations
            .union_discriminators
            .get(&def.def_id)
            .expect("no union discriminator available. Should fail earlier");

        if union_discriminator.variants.is_empty() {
            panic!("No input variants");
        }

        let mut union_builder = UnionBuilder::new(def);
        let mut root_types: HashSet<DefId> = Default::default();

        for root_discriminator in &union_discriminator.variants {
            union_builder
                .add_root_discriminator(root_discriminator, self)
                .expect("Could not add root discriminator to union builder");

            root_types.insert(root_discriminator.serde_def.def_id);
        }

        let variants: Vec<_> = if properties.table.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_key = SerdeKey::Def(SerdeDef::new(
                def.def_id,
                SerdeModifier::INHERENT_PROPS | def.modifier.cross_def_flags(),
            ));

            union_builder
                .build(self, |this, addr, result_def| {
                    if result_def.modifier.contains(SerdeModifier::INHERENT_PROPS)
                        && root_types.contains(&result_def.def_id)
                    {
                        // Make the intersection:
                        this.gen_addr(SerdeKey::Intersection(Box::new(
                            [inherent_properties_key.clone(), SerdeKey::Def(result_def)].into(),
                        )))
                        .expect("No inner operator")
                    } else {
                        addr
                    }
                })
                .unwrap()
        } else {
            union_builder
                .build(self, |_this, addr, _result_type| addr)
                .unwrap()
        };

        if variants.is_empty() {
            panic!(
                "empty variant set for {def:?}. Input variants were {:?}",
                union_discriminator.variants
            );
        }

        SerdeOperator::Union(UnionOperator::new(typename.into(), def, variants))
    }

    /// TODO: It's probably possible to avoid duplicating structs
    /// for different interfaces if there are no rewrites compared to plain JSON.
    fn create_struct_operator(
        &mut self,
        def: SerdeDef,
        typename: &str,
        properties: &Properties,
        flags: SerdeStructFlags,
    ) -> SerdeOperator {
        let mut op = StructOperator {
            typename: typename.into(),
            def,
            flags,
            properties: Default::default(),
        };

        let Some(table) = &properties.table else {
            return SerdeOperator::Struct(op);
        };

        for (property_id, property) in table {
            let meta = self.defs.relationship_meta(property_id.relationship_id);
            let (type_def_id, ..) = meta.relationship.by(property_id.role.opposite());
            let prop_key = match property_id.role {
                Role::Subject => {
                    let DefKind::TextLiteral(prop_key) = meta.relation_def_kind.value else {
                        panic!("Subject property is not a string literal");
                    };

                    *prop_key
                }
                Role::Object => meta
                    .relationship
                    .object_prop
                    .expect("Object property has no name"),
            };

            let (property_cardinality, value_addr) = self.get_property_operator(
                type_def_id,
                property.cardinality,
                def.modifier.cross_def_flags(),
            );

            let rel_params_addr = match &meta.relationship.rel_params {
                RelParams::Type(def_id) => {
                    self.gen_addr(SerdeKey::Def(SerdeDef::new(*def_id, def.modifier.reset())))
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

            if let Some(target_properties) = self.relations.properties_by_def_id(type_def_id) {
                if target_properties.identified_by.is_some() && !property.is_entity_id {
                    flags |= SerdePropertyFlags::IN_ENTITY_GRAPH;
                }
            }

            insert_property(
                &mut op.properties,
                prop_key,
                SerdeProperty {
                    property_id: *property_id,
                    value_addr,
                    flags,
                    value_generator,
                    rel_params_addr,
                },
                def.modifier,
            );
        }

        SerdeOperator::Struct(op)
    }

    fn struct_flags_from_def_id(&self, def_id: DefId) -> SerdeStructFlags {
        let mut flags = SerdeStructFlags::empty();
        if let DefKind::Type(type_def) = self.defs.def_kind(def_id) {
            if type_def.open {
                flags |= SerdeStructFlags::OPEN_DATA;
            }
        }

        flags
    }
}

pub enum IdentAdaption {
    Verbatim,
    Adapted,
}

fn insert_property(
    properties: &mut IndexMap<String, SerdeProperty>,
    property_name: &str,
    property: SerdeProperty,
    modifier: SerdeModifier,
) -> IdentAdaption {
    if modifier.contains(SerdeModifier::GRAPHQL) {
        let (mut graphql_identifier, adaption) = make_property_name(property_name, modifier);
        let mut retries = 16;

        while properties.contains_key(&graphql_identifier) {
            retries -= 1;
            if retries == 0 {
                error!("GraphQL tried too many rewrites. Ignoring property `{property_name}`");
                return IdentAdaption::Adapted;
            }

            graphql_identifier = smart_format!("{graphql_identifier}_");
        }

        properties.insert(graphql_identifier, property);
        adaption
    } else {
        properties.insert(property_name.into(), property);
        IdentAdaption::Verbatim
    }
}

fn make_property_name(input: &str, modifier: SerdeModifier) -> (String, IdentAdaption) {
    if modifier.contains(SerdeModifier::GRAPHQL) {
        match adapt_graphql_identifier(input) {
            GqlAdaptedIdent::Valid(valid) => (valid.into(), IdentAdaption::Verbatim),
            GqlAdaptedIdent::Adapted(adapted) => (adapted, IdentAdaption::Adapted),
        }
    } else {
        (input.into(), IdentAdaption::Verbatim)
    }
}

pub(super) fn operator_to_leaf_discriminant(operator: &SerdeOperator) -> LeafDiscriminant {
    match operator {
        SerdeOperator::CapturingTextPattern(def_id) => {
            LeafDiscriminant::MatchesCapturingTextPattern(*def_id)
        }
        SerdeOperator::I64(..) | SerdeOperator::I32(..) => LeafDiscriminant::IsInt,
        SerdeOperator::DynamicSequence | SerdeOperator::RelationSequence(_) => {
            LeafDiscriminant::IsSequence
        }
        other => {
            warn!("Unable to match {other:?} yet");
            LeafDiscriminant::IsAny
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
