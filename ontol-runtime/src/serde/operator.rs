use std::{
    fmt::Debug,
    ops::{Range, RangeInclusive},
};

use ::serde::{Deserialize, Serialize};
use derive_debug_extras::DebugExtras;
use indexmap::IndexMap;
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    discriminator::{VariantDiscriminator, VariantPurpose},
    value::PropertyId,
    value_generator::ValueGenerator,
    DefId, DefVariant,
};

use super::processor::{ProcessorLevel, ProcessorMode};

/// SerdeOperatorId is an index into a vector of SerdeOperators.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize, DebugExtras)]
#[debug_single_tuple_inline]
pub struct SerdeOperatorId(pub u32);

#[derive(Debug, Serialize, Deserialize)]
pub enum SerdeOperator {
    Unit,
    True(DefId),
    False(DefId),
    Boolean(DefId),
    I64(DefId, Option<RangeInclusive<i64>>),
    F64(DefId, Option<RangeInclusive<f64>>),
    String(DefId),
    StringConstant(String, DefId),

    /// Always deserializes into a string, ignores capture groups:
    StringPattern(DefId),

    /// Deserializes into a Struct if there are capture groups:
    CapturingStringPattern(DefId),

    /// Special operator for serialization, can serialize a sequence of anything,
    /// by using dynamic lookup.
    /// Does not support deserialization.
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

    /// A map with one property: A primary id
    PrimaryId(String, SerdeOperatorId),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RelationSequenceOperator {
    // note: This is constant size array so that it can produce a dynamic slice
    pub ranges: [SequenceRange; 1],
    pub def_variant: DefVariant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConstructorSequenceOperator {
    pub ranges: SmallVec<[SequenceRange; 3]>,
    pub def_variant: DefVariant,
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
    pub operator_id: SerdeOperatorId,
    /// If this range is finite, this represents the number of repetitions.
    /// If None, this range is infinite and accepts 0 or more items.
    /// An infinite range must be the last range in the sequence to make any sense.
    pub finite_repetition: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AliasOperator {
    pub typename: String,
    pub def_variant: DefVariant,
    pub inner_operator_id: SerdeOperatorId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnionOperator {
    typename: String,
    union_def_variant: DefVariant,
    variants: Vec<ValueUnionVariant>,
}

impl UnionOperator {
    /// Note: variants must be sorted according to its purpose (VariantPurpose)
    pub fn new(
        typename: String,
        union_def_variant: DefVariant,
        variants: Vec<ValueUnionVariant>,
    ) -> Self {
        variants
            .iter()
            .fold(VariantPurpose::Identification, |last_purpose, variant| {
                if variant.discriminator.purpose < last_purpose {
                    panic!("variants are not sorted: {variants:#?}");
                }
                variant.discriminator.purpose
            });

        Self {
            typename,
            union_def_variant,
            variants,
        }
    }

    pub fn typename(&self) -> &String {
        &self.typename
    }

    pub fn union_def_variant(&self) -> DefVariant {
        self.union_def_variant
    }

    pub fn variants(&self, mode: ProcessorMode, level: ProcessorLevel) -> FilteredVariants<'_> {
        if matches!(mode, ProcessorMode::Inspect) || level.is_root() {
            let skip_id = self.variants.iter().enumerate().find(|(_, variant)| {
                variant.discriminator.purpose > VariantPurpose::Identification
            });

            if let Some((skip_index, _)) = skip_id {
                Self::filtered_variants(&self.variants[skip_index..])
            } else {
                Self::filtered_variants(&self.variants)
            }
        } else {
            Self::filtered_variants(&self.variants)
        }
    }

    fn filtered_variants(variants: &[ValueUnionVariant]) -> FilteredVariants<'_> {
        if variants.len() == 1 {
            FilteredVariants::Single(variants[0].operator_id)
        } else {
            FilteredVariants::Union(variants)
        }
    }

    pub fn unfiltered_variants(&self) -> &[ValueUnionVariant] {
        &self.variants
    }
}

pub enum FilteredVariants<'e> {
    Single(SerdeOperatorId),
    /// Should serialize one of the union members
    Union(&'e [ValueUnionVariant]),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValueUnionVariant {
    pub discriminator: VariantDiscriminator,
    pub operator_id: SerdeOperatorId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructOperator {
    pub typename: String,
    pub def_variant: DefVariant,
    pub properties: IndexMap<String, SerdeProperty>,
}

impl StructOperator {
    pub fn filter_properties(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropertyId>,
    ) -> impl Iterator<Item = (&String, &SerdeProperty)> {
        self.properties
            .iter()
            .filter(move |(_, property)| property.filter(mode, parent_property_id).is_some())
    }

    pub fn required_count(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropertyId>,
    ) -> usize {
        self.filter_properties(mode, parent_property_id)
            .filter(|(_, property)| !property.is_optional())
            .count()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SerdeProperty {
    /// The ID of this property
    pub property_id: PropertyId,

    /// The operator id for the value of this property
    pub value_operator_id: SerdeOperatorId,

    /// Various flags
    pub flags: SerdePropertyFlags,

    /// Value generator
    pub value_generator: Option<ValueGenerator>,

    pub rel_params_operator_id: Option<SerdeOperatorId>,
}

impl SerdeProperty {
    #[inline]
    pub fn is_optional(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::OPTIONAL)
    }

    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::READ_ONLY)
    }

    #[inline]
    pub fn filter(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropertyId>,
    ) -> Option<&Self> {
        if !matches!(mode, ProcessorMode::Read | ProcessorMode::Inspect) && self.is_read_only() {
            return None;
        }

        if let Some(parent_property_id) = parent_property_id {
            // Filter out if this property is the mirrored property of the parent property
            if self.property_id.relationship_id == parent_property_id.relationship_id
                && self.property_id.role != parent_property_id.role
            {
                return None;
            }
        }

        Some(self)
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct SerdePropertyFlags: u32 {
        const OPTIONAL       = 0b00000001;
        const READ_ONLY      = 0b00000010;
        const ENTITY_ID      = 0b00000100;
    }
}
