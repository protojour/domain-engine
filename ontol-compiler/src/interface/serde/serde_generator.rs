use std::{
    collections::{HashMap, VecDeque},
    ops::RangeInclusive,
};

use indexmap::IndexMap;
use ontol_runtime::{
    debug::NoFmt,
    interface::discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    interface::serde::operator::{
        AliasOperator, ConstructorSequenceOperator, RelationSequenceOperator, SequenceRange,
        SerdeOperator, SerdeOperatorAddr, SerdeProperty, SerdeUnionVariant, StructOperator,
        UnionOperator,
    },
    interface::{
        discriminator::LeafDiscriminant,
        serde::{operator::SerdeStructFlags, SerdeDef, SerdeModifier},
    },
    ontology::{Cardinality, PropertyCardinality, ValueCardinality},
    smart_format,
    text::TextConstant,
    DefId,
};
use smartstring::alias::String;
use tracing::{debug, debug_span, error, trace, warn};

use super::{sequence_range_builder::SequenceRangeBuilder, SerdeIntersection, SerdeKey};

use crate::{
    codegen::task::CodegenTasks,
    compiler_queries::GetDefType,
    def::{DefKind, Defs, LookupRelationshipMeta, TypeDef},
    interface::graphql::graphql_namespace::{adapt_graphql_identifier, GqlAdaptedIdent},
    primitive::{PrimitiveKind, Primitives},
    relation::{Constructor, Properties, Relations, UnionMemberCache},
    repr::repr_model::{ReprKind, ReprScalarKind},
    strings::Strings,
    text_patterns::{TextPatternSegment, TextPatterns},
    type_check::seal::SealCtx,
    types::{DefTypes, Type, TypeRef},
    SourceSpan,
};

pub struct SerdeGenerator<'c, 'm> {
    pub strings: &'c mut Strings<'m>,
    pub defs: &'c Defs<'m>,
    pub primitives: &'c Primitives,
    pub def_types: &'c DefTypes<'m>,
    pub relations: &'c Relations,
    pub seal_ctx: &'c SealCtx,
    pub patterns: &'c TextPatterns,
    pub codegen_tasks: &'c CodegenTasks<'m>,
    pub union_member_cache: &'c UnionMemberCache,

    pub(super) lazy_struct_op_tasks: VecDeque<(SerdeOperatorAddr, SerdeDef, &'c Properties)>,
    pub(super) lazy_struct_intersection_tasks: VecDeque<(SerdeOperatorAddr, SerdeIntersection)>,
    pub(super) lazy_union_repr_tasks:
        VecDeque<(SerdeOperatorAddr, SerdeDef, TextConstant, &'c Properties)>,
    pub(super) task_state: DebugTaskState,

    pub(super) operators_by_addr: Vec<SerdeOperator>,
    pub(super) operators_by_key: HashMap<SerdeKey, SerdeOperatorAddr>,
}

pub(super) enum DebugTaskState {
    Unlocked,
    Struct,
    Intersection,
    Union,
}

enum OperatorAllocation {
    Allocated(SerdeOperatorAddr, SerdeOperator),
    Redirect(SerdeDef),
}

impl<'c, 'm> SerdeGenerator<'c, 'm> {
    pub fn finish(mut self) -> (Vec<SerdeOperator>, HashMap<SerdeKey, SerdeOperatorAddr>) {
        self.execute_tasks();

        (self.operators_by_addr, self.operators_by_key)
    }

    fn execute_tasks(&mut self) {
        while self.has_pending_tasks() {
            self.task_state = DebugTaskState::Struct;

            while let Some((addr, def, properties)) = self.lazy_struct_op_tasks.pop_front() {
                self.populate_struct_operator(addr, def, properties);
            }

            self.task_state = DebugTaskState::Intersection;

            while let Some((addr, intersection)) = self.lazy_struct_intersection_tasks.pop_front() {
                assert!(self.lazy_struct_op_tasks.is_empty());
                self.populate_struct_intersection_operator(addr, intersection);
            }

            self.task_state = DebugTaskState::Union;

            while let Some((addr, def, typename, properties)) =
                self.lazy_union_repr_tasks.pop_front()
            {
                assert!(self.lazy_struct_intersection_tasks.is_empty());
                assert!(self.lazy_struct_op_tasks.is_empty());

                self.populate_union_repr_operator(addr, def, typename, properties);
            }
        }

        self.task_state = DebugTaskState::Unlocked;
    }

    fn has_pending_tasks(&self) -> bool {
        !self.lazy_struct_op_tasks.is_empty()
            || !self.lazy_struct_intersection_tasks.is_empty()
            || !self.lazy_union_repr_tasks.is_empty()
    }

    pub fn make_dynamic_sequence_addr(&mut self) -> SerdeOperatorAddr {
        let addr = SerdeOperatorAddr(self.operators_by_addr.len() as u32);
        self.operators_by_addr.push(SerdeOperator::DynamicSequence);
        addr
    }

    pub fn gen_addr_greedy(&mut self, key: SerdeKey) -> Option<SerdeOperatorAddr> {
        let addr = self.gen_addr_lazy(key);
        self.execute_tasks();
        addr
    }

    pub fn gen_addr_lazy(&mut self, mut key: SerdeKey) -> Option<SerdeOperatorAddr> {
        let mut discarded_keys = vec![];

        loop {
            if let Some(addr) = self.operators_by_key.get(&key) {
                return Some(*addr);
            }

            match self.alloc_serde_operator_from_key(key.clone()) {
                Some(OperatorAllocation::Allocated(addr, operator)) => {
                    trace!(
                        "CREATED {addr:?} {key:?} {operator:?}",
                        operator = NoFmt(&operator)
                    );
                    self.operators_by_addr[addr.0 as usize] = operator;

                    self.operators_by_key.insert(key, addr);

                    for discarded_key in discarded_keys {
                        self.operators_by_key.insert(discarded_key, addr);
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

    pub fn lookup_addr_by_key(&self, key: &SerdeKey) -> Option<&SerdeOperatorAddr> {
        self.operators_by_key.get(key)
    }

    pub fn get_operator(&self, addr: SerdeOperatorAddr) -> &SerdeOperator {
        &self.operators_by_addr[addr.0 as usize]
    }

    pub fn gen_operator_lazy(&mut self, key: SerdeKey) -> Option<&SerdeOperator> {
        let addr = self.gen_addr_lazy(key)?;
        Some(&self.operators_by_addr[addr.0 as usize])
    }

    pub(super) fn get_property_operator(
        &mut self,
        type_def_id: DefId,
        cardinality: Cardinality,
        cross_def_modifier: SerdeModifier,
    ) -> (PropertyCardinality, SerdeOperatorAddr) {
        let default_modifier = SerdeModifier::json_default() | cross_def_modifier;

        match cardinality.1 {
            ValueCardinality::One => (
                cardinality.0,
                self.gen_addr_lazy(SerdeKey::Def(SerdeDef {
                    def_id: type_def_id,
                    modifier: default_modifier,
                }))
                .expect("no property operator"),
            ),
            ValueCardinality::Many => (
                cardinality.0,
                self.gen_addr_lazy(SerdeKey::Def(SerdeDef {
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
        match key {
            SerdeKey::Intersection(intersection) => {
                // Generate dependencies
                if let Some(main) = intersection.main {
                    self.gen_addr_lazy(SerdeKey::Def(main));
                }

                for def in intersection.defs.iter() {
                    self.gen_addr_lazy(SerdeKey::Def(*def));
                }

                let addr = self.alloc_addr_for_key(&SerdeKey::Intersection(intersection.clone()));

                if matches!(self.task_state, DebugTaskState::Union) {
                    panic!();
                }

                // This will get computed later
                self.lazy_struct_intersection_tasks
                    .push_back((addr, *intersection));

                Some(OperatorAllocation::Allocated(addr, SerdeOperator::Unit))
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
                    .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
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
                        SerdeOperator::IdSingletonStruct(
                            def.def_id,
                            self.strings.intern_constant(&ident),
                            object_addr,
                        ),
                    ))
                }
            }
            SerdeKey::Def(def) if def.modifier.contains(SerdeModifier::ARRAY) => {
                let element_addr =
                    self.gen_addr_lazy(SerdeKey::Def(def.remove_modifier(SerdeModifier::ARRAY)))?;

                // FIXME: What about unions?
                let to_entity = self
                    .relations
                    .properties_by_def_id(def.def_id)
                    .map(|properties| properties.identified_by.is_some())
                    .unwrap_or(false);

                Some(OperatorAllocation::Allocated(
                    self.alloc_addr_for_key(&key),
                    SerdeOperator::RelationSequence(RelationSequenceOperator {
                        ranges: [SequenceRange {
                            addr: element_addr,
                            finite_repetition: None,
                        }],
                        def,
                        to_entity,
                    }),
                ))
            }
            SerdeKey::Def(def) => self.alloc_def_type_operator(def),
        }
    }

    fn alloc_def_type_operator(&mut self, def: SerdeDef) -> Option<OperatorAllocation> {
        match self.get_def_type(def.def_id) {
            Some(Type::Domain(def_id) | Type::Anonymous(def_id)) => {
                let properties = self.relations.properties_by_def_id.get(def_id);
                let typename = self.strings.intern_constant(self.get_typename(*def_id));
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
                PrimitiveKind::Serial => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::Serial(def.def_id),
                )),
                PrimitiveKind::Text => Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::String(def.def_id),
                )),
                PrimitiveKind::OpenDataRelationship => None,
            },
            Type::IntConstant(int) => Some(OperatorAllocation::Allocated(
                self.alloc_addr(&def),
                SerdeOperator::I64(def.def_id, Some(RangeInclusive::new(*int, *int))),
            )),
            Type::FloatConstant(float) => Some(OperatorAllocation::Allocated(
                self.alloc_addr(&def),
                SerdeOperator::F64(
                    def.def_id,
                    Some(RangeInclusive::new((*float).into(), (*float).into())),
                ),
            )),
            Type::TextConstant(def_id) => {
                assert_eq!(def.def_id, *def_id);

                let literal = self.defs.get_string_representation(*def_id);
                let constant = self.strings.intern_constant(literal);

                Some(OperatorAllocation::Allocated(
                    self.alloc_addr(&def),
                    SerdeOperator::StringConstant(constant, def.def_id),
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
        typename: TextConstant,
        properties: Option<&'c Properties>,
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
                        typename,
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
                            typename,
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
                if def.modifier.contains(SerdeModifier::UNION) {
                    Some(self.alloc_union_repr_operator(def, typename, properties))
                } else {
                    Some(self.alloc_struct_operator(def, typename, properties, {
                        let mut flags = SerdeStructFlags::empty();
                        for (def_id, _) in members {
                            flags |= self.struct_flags_from_def_id(*def_id);
                        }
                        flags
                    }))
                }
            }
            ReprKind::Seq | ReprKind::Intersection(_) => match &properties.constructor {
                Constructor::Sequence(sequence) => {
                    let mut sequence_range_builder = SequenceRangeBuilder::default();

                    let mut element_iterator = sequence.elements().peekable();

                    while let Some((_, element)) = element_iterator.next() {
                        let addr = match element {
                            None => self
                                .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                                    DefId::unit(),
                                    def.modifier.cross_def_flags(),
                                )))
                                .unwrap(),
                            Some(relationship_id) => {
                                let meta = self.defs.relationship_meta(relationship_id);

                                self.gen_addr_lazy(SerdeKey::Def(
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
                    trace!("sequence ranges: {:#?}", NoFmt(&ranges));

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
        typename: TextConstant,
        properties: &'c Properties,
    ) -> Option<OperatorAllocation> {
        let union_mod = SerdeModifier::UNION | SerdeModifier::PRIMARY_ID;

        if !def.modifier.contains(union_mod) {
            let flags = self.struct_flags_from_def_id(def.def_id);
            return Some(self.alloc_struct_operator(def, typename, properties, flags));
        }

        let Some(identifies_relationship_id) = properties.identified_by else {
            return Some(OperatorAllocation::Redirect(def.remove_modifier(union_mod)));
        };
        let Some(id_addr) = self.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
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
            .gen_addr_lazy(SerdeKey::Def(struct_def))
            .expect("No property struct operator");

        let (id_property_name, id_leaf_discriminant) =
            match self.operators_by_addr.get(id_addr.0 as usize).unwrap() {
                SerdeOperator::IdSingletonStruct(_entity_id, id_property_name, inner_addr) => (
                    *id_property_name,
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
                other => panic!("id operator was not an Id: {:?}", NoFmt(other)),
            };

        Some(OperatorAllocation::Allocated(
            new_addr,
            SerdeOperator::Union(UnionOperator::new(
                typename,
                def,
                vec![
                    SerdeUnionVariant {
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
                    SerdeUnionVariant {
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
    }

    fn alloc_struct_intersection_operator(
        &mut self,
        def: SerdeDef,
        typename: TextConstant,
        properties: &'c Properties,
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
            } else {
                let mut intersection_keys = vec![];
                let inherent_def = def.remove_modifier(SerdeModifier::INTERSECTION);

                if properties.table.is_some() {
                    // inherent properties:
                    self.gen_addr_lazy(SerdeKey::Def(inherent_def));
                    intersection_keys.push(inherent_def);
                }

                for (def_id, _span) in members {
                    let member_def = SerdeDef::new(*def_id, def.modifier.reset());
                    self.gen_addr_lazy(SerdeKey::Def(member_def));

                    intersection_keys.push(member_def);
                }

                self.alloc_serde_operator_from_key(SerdeKey::Intersection(Box::new(
                    SerdeIntersection {
                        main: Some(inherent_def),
                        defs: intersection_keys,
                    },
                )))
            }
        } else if modifier.contains(SerdeModifier::INHERENT_PROPS) {
            let mut flags = SerdeStructFlags::empty();
            for (def_id, _) in members {
                flags |= self.struct_flags_from_def_id(*def_id);
            }

            Some(self.alloc_struct_operator(def, typename, properties, flags))
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
                    typename,
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
            TextPatternSegment::AnyString => SerdeOperator::String(def.def_id),
            _ => SerdeOperator::CapturingTextPattern(def.def_id),
        };

        Some(OperatorAllocation::Allocated(
            self.alloc_addr(&def),
            operator,
        ))
    }

    fn alloc_union_repr_operator(
        &mut self,
        def: SerdeDef,
        typename: TextConstant,
        properties: &'c Properties,
    ) -> OperatorAllocation {
        let addr = self.alloc_addr(&def);
        let union_discriminator = self
            .relations
            .union_discriminators
            .get(&def.def_id)
            .expect("no union discriminator available. Should fail earlier");

        let _entered = debug_span!("alloc_union", def=?def.def_id).entered();

        if properties.table.is_some() {
            // Need to do an intersection of the union type's _inherent_
            // properties and each variant's properties
            let inherent_properties_def = SerdeDef::new(
                def.def_id,
                SerdeModifier::INHERENT_PROPS | def.modifier.cross_def_flags(),
            );

            for root_discriminator in &union_discriminator.variants {
                let variant_def = SerdeDef::new(
                    root_discriminator.serde_def.def_id,
                    SerdeModifier::INHERENT_PROPS | def.modifier.cross_def_flags(),
                );

                debug!("pre-alloc {variant_def:?}");

                // Intersection dependencies
                self.gen_addr_lazy(SerdeKey::Intersection(Box::new(SerdeIntersection {
                    main: None,
                    defs: [variant_def, inherent_properties_def].into(),
                })));
            }
        }

        self.lazy_union_repr_tasks
            .push_back((addr, def, typename, properties));

        OperatorAllocation::Allocated(addr, SerdeOperator::Unit)
    }

    /// TODO: It's probably possible to avoid duplicating structs
    /// for different interfaces if there are no rewrites compared to plain JSON.
    fn alloc_struct_operator(
        &mut self,
        def: SerdeDef,
        typename: TextConstant,
        properties: &'c Properties,
        flags: SerdeStructFlags,
    ) -> OperatorAllocation {
        let addr = self.alloc_addr(&def);

        self.lazy_struct_op_tasks.push_back((addr, def, properties));

        OperatorAllocation::Allocated(
            addr,
            SerdeOperator::Struct(StructOperator {
                typename,
                def,
                flags,
                properties: Default::default(),
            }),
        )
    }

    pub(super) fn struct_flags_from_def_id(&self, def_id: DefId) -> SerdeStructFlags {
        let mut flags = SerdeStructFlags::empty();
        if let DefKind::Type(type_def) = self.defs.def_kind(def_id) {
            if type_def.open {
                flags |= SerdeStructFlags::OPEN_DATA;
            }
        }

        flags
    }

    pub(super) fn get_typename(&self, def_id: DefId) -> &'c str {
        match self.defs.def_kind(def_id) {
            DefKind::Type(TypeDef {
                ident: Some(ident), ..
            }) => ident,
            DefKind::Type(TypeDef { ident: None, .. }) => "<anonymous>",
            _ => "Unknown type",
        }
    }
}

pub enum IdentAdaption {
    Verbatim,
    Adapted,
}

pub(super) fn insert_property(
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
            warn!("Unable to match {:?} yet", NoFmt(&other));
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
