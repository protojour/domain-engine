use std::{
    fmt::Debug,
    ops::{Range, RangeInclusive},
};

use ::serde::{Deserialize, Serialize};
use indexmap::IndexMap;
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    interface::discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    value::PropertyId,
    value_generator::ValueGenerator,
    DefId,
};

use super::{
    processor::{ProcessorLevel, ProcessorMode, ProcessorProfileFlags},
    SerdeDef,
};

/// SerdeOperatorAddr is an index into a vector of SerdeOperators.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SerdeOperatorAddr(pub u32);

impl ::std::fmt::Debug for SerdeOperatorAddr {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "SerdeOperatorAddr({:?})", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SerdeOperator {
    Unit,
    True(DefId),
    False(DefId),
    Boolean(DefId),
    I32(DefId, Option<RangeInclusive<i32>>),
    I64(DefId, Option<RangeInclusive<i64>>),
    F64(DefId, Option<RangeInclusive<f64>>),
    Serial(DefId),
    String(DefId),
    StringConstant(String, DefId),

    /// Always deserializes into text, ignores capture groups:
    TextPattern(DefId),

    /// Deserializes into a Struct if there are capture groups:
    CapturingTextPattern(DefId),

    /// Special operator for serialization, serializes each value
    /// in a raw manner based on its runtime type information.
    DynamicSequence,

    /// A sequence representing a relationship between one subject and many objects.
    /// This is simple and does not support any tuples.
    RelationSequence(RelationSequenceOperator),

    /// A sequence for constructing a value.
    /// This may include tuple-like sequences.
    ConstructorSequence(ConstructorSequenceOperator),

    /// A type with just one anonymous property
    Alias(AliasOperator),

    /// A type with multiple anonymous properties, equivalent to a union of types
    Union(UnionOperator),

    /// A type with many properties
    Struct(StructOperator),

    /// A map with one property: The ID of an entity.
    IdSingletonStruct(String, SerdeOperatorAddr),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelationSequenceOperator {
    // note: This is constant size array so that it can produce a dynamic slice
    pub ranges: [SequenceRange; 1],
    pub def: SerdeDef,
    pub to_entity: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConstructorSequenceOperator {
    pub ranges: SmallVec<[SequenceRange; 3]>,
    pub def: SerdeDef,
}

impl ConstructorSequenceOperator {
    pub fn length_range(&self) -> Range<Option<u16>> {
        let mut count = 0;
        let mut finite = true;
        for range in &self.ranges {
            if let Some(repetition) = range.finite_repetition {
                count += repetition;
            } else {
                finite = false;
            }
        }

        Range {
            start: if count > 0 { Some(count) } else { None },
            end: if finite { Some(count) } else { None },
        }
    }
}

/// A matcher for a range within a sequence
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequenceRange {
    /// Operator to use for this range
    pub addr: SerdeOperatorAddr,
    /// If this range is finite, this represents the number of repetitions.
    /// If None, this range is infinite and accepts 0 or more items.
    /// An infinite range must be the last range in the sequence to make any sense.
    pub finite_repetition: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AliasOperator {
    pub typename: String,
    pub def: SerdeDef,
    pub inner_addr: SerdeOperatorAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnionOperator {
    typename: String,
    union_def: SerdeDef,
    variants: Vec<SerdeUnionVariant>,
}

impl UnionOperator {
    /// Note: variants must be sorted according to their purpose (VariantPurpose)
    pub fn new(typename: String, union_def: SerdeDef, variants: Vec<SerdeUnionVariant>) -> Self {
        variants.iter().fold(
            VariantPurpose::Identification {
                entity_id: DefId::unit(),
            },
            |last_purpose, variant| {
                if variant.discriminator.purpose < last_purpose {
                    panic!("variants are not sorted: {variants:#?}");
                }
                variant.discriminator.purpose
            },
        );

        Self {
            typename,
            union_def,
            variants,
        }
    }

    pub fn typename(&self) -> &String {
        &self.typename
    }

    pub fn union_def(&self) -> SerdeDef {
        self.union_def
    }

    pub fn applied_variants(
        &self,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> AppliedVariants<'_> {
        if let Some(certain_addr) =
            PossibleVariantsIter::new(&self.variants, mode, level).find_unambiguous_addr()
        {
            AppliedVariants::Unambiguous(certain_addr)
        } else {
            AppliedVariants::OneOf(PossibleVariants {
                variants: &self.variants,
                mode,
                level,
            })
        }
    }

    pub fn unfiltered_variants(&self) -> &[SerdeUnionVariant] {
        &self.variants
    }
}

pub enum AppliedVariants<'on> {
    Unambiguous(SerdeOperatorAddr),
    OneOf(PossibleVariants<'on>),
}

#[derive(Clone, Copy)]
pub struct PossibleVariants<'on> {
    variants: &'on [SerdeUnionVariant],
    mode: ProcessorMode,
    level: ProcessorLevel,
}

impl<'on> PossibleVariants<'on> {
    pub fn iter(&self) -> PossibleVariantsIter<'on> {
        PossibleVariantsIter::new(self.variants, self.mode, self.level)
    }
}

pub struct PossibleVariantsIter<'on> {
    inner_iter: std::slice::Iter<'on, SerdeUnionVariant>,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

impl<'on> PossibleVariantsIter<'on> {
    fn new(variants: &'on [SerdeUnionVariant], mode: ProcessorMode, level: ProcessorLevel) -> Self {
        Self {
            inner_iter: variants.iter(),
            mode,
            level,
        }
    }

    fn find_unambiguous_addr(&mut self) -> Option<SerdeOperatorAddr> {
        let addr = self.next().map(|variant| variant.addr)?;
        if self.next().is_some() {
            None
        } else {
            Some(addr)
        }
    }

    fn apply_variant(
        variant: &'on SerdeUnionVariant,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> Option<PossibleVariant<'on>> {
        match (mode, variant.discriminator.purpose) {
            (ProcessorMode::Delete, VariantPurpose::Data) => None,
            (ProcessorMode::Raw, VariantPurpose::Identification { .. }) => None,
            (_, VariantPurpose::Identification { .. }) if level.is_global_root() => None,
            _ => Some(PossibleVariant {
                discriminant: &variant.discriminator.discriminant,
                purpose: variant.discriminator.purpose,
                addr: variant.addr,
                serde_def: variant.discriminator.serde_def,
            }),
        }
    }
}

impl<'on> Iterator for PossibleVariantsIter<'on> {
    type Item = PossibleVariant<'on>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(variant) = self.inner_iter.next() {
            if let Some(next) = Self::apply_variant(variant, self.mode, self.level) {
                return Some(next);
            }
        }

        None
    }
}

#[derive(Debug)]
pub struct PossibleVariant<'on> {
    pub discriminant: &'on Discriminant,
    pub purpose: VariantPurpose,
    pub addr: SerdeOperatorAddr,
    pub serde_def: SerdeDef,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SerdeUnionVariant {
    pub discriminator: VariantDiscriminator,
    pub addr: SerdeOperatorAddr,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructOperator {
    pub typename: String,
    pub def: SerdeDef,
    pub flags: SerdeStructFlags,
    pub properties: IndexMap<String, SerdeProperty>,
}

impl StructOperator {
    pub fn filter_properties(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropertyId>,
        profile_flags: ProcessorProfileFlags,
    ) -> impl Iterator<Item = (&String, &SerdeProperty)> {
        self.properties.iter().filter(move |(_, property)| {
            property
                .filter(mode, parent_property_id, profile_flags)
                .is_some()
        })
    }

    pub fn required_count(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropertyId>,
        profile_flags: ProcessorProfileFlags,
    ) -> usize {
        if profile_flags.contains(ProcessorProfileFlags::ALL_PROPS_OPTIONAL) {
            return 0;
        }

        self.filter_properties(mode, parent_property_id, profile_flags)
            .filter(|(_, property)| !property.is_optional_for(mode, &profile_flags))
            .count()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SerdeProperty {
    /// The ID of this property
    pub property_id: PropertyId,

    /// The operator addr for the value of this property
    pub value_addr: SerdeOperatorAddr,

    /// Various flags
    pub flags: SerdePropertyFlags,

    /// Value generator
    pub value_generator: Option<ValueGenerator>,

    pub rel_params_addr: Option<SerdeOperatorAddr>,
}

impl SerdeProperty {
    #[inline]
    pub fn is_optional(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::OPTIONAL)
    }

    #[inline]
    pub(crate) fn is_optional_for(
        &self,
        mode: ProcessorMode,
        profile_flags: &ProcessorProfileFlags,
    ) -> bool {
        self.is_optional()
            || (matches!(mode, ProcessorMode::Update | ProcessorMode::GraphqlUpdate)
                && !self.is_entity_id())
            || profile_flags.contains(ProcessorProfileFlags::ALL_PROPS_OPTIONAL)
    }

    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::READ_ONLY)
    }

    #[inline]
    pub fn is_entity_id(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::ENTITY_ID)
    }

    #[inline]
    pub fn filter(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropertyId>,
        profile_flags: ProcessorProfileFlags,
    ) -> Option<&Self> {
        match mode {
            ProcessorMode::Create => {
                if self.is_read_only() {
                    return None;
                }
            }
            ProcessorMode::Update | ProcessorMode::GraphqlUpdate => {
                if self.is_read_only() && !self.is_entity_id() {
                    return None;
                }
            }
            ProcessorMode::Read
            | ProcessorMode::Raw
            | ProcessorMode::RawTreeOnly
            | ProcessorMode::Delete => {}
        }

        if let Some(parent_property_id) = parent_property_id {
            // Filter out if this property is the mirrored property of the parent property
            if self.property_id.relationship_id == parent_property_id.relationship_id
                && self.property_id.role != parent_property_id.role
                && !profile_flags.contains(ProcessorProfileFlags::ALLOW_STRUCTURALLY_CIRCULAR_PROPS)
            {
                return None;
            }
        }

        if matches!(mode, ProcessorMode::RawTreeOnly)
            && self.flags.contains(SerdePropertyFlags::IN_ENTITY_GRAPH)
        {
            return None;
        }

        Some(self)
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct SerdePropertyFlags: u32 {
        const OPTIONAL        = 0b00000001;
        const READ_ONLY       = 0b00000010;
        const ENTITY_ID       = 0b00000100;
        /// Property is part of the entity graph
        const IN_ENTITY_GRAPH = 0b00001000;
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct SerdeStructFlags: u32 {
        /// This struct operator supports open/domainless properties
        const OPEN_DATA       = 0b00000001;
    }
}
