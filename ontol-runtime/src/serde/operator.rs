use std::{fmt::Debug, ops::Range};

use derive_debug_extras::DebugExtras;
use indexmap::IndexMap;
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{discriminator::VariantDiscriminator, value::PropertyId, DefId, DefVariant};

/// SerdeOperatorId is an index into a vector of SerdeOperators.
#[derive(Clone, Copy, Eq, PartialEq, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct SerdeOperatorId(pub u32);

#[derive(Debug)]
pub enum SerdeOperator {
    Unit,
    Int(DefId),
    Number(DefId),
    String(DefId),
    StringConstant(String, DefId),

    /// Always deserializes into a string, ignores capture groups:
    StringPattern(DefId),

    /// Deserializes into a Map if there are capture groups:
    CapturingStringPattern(DefId),

    /// A sequence representing a relationship between one subject and many objects.
    /// This is simple and does not support any tuples.
    RelationSequence(RelationSequenceOperator),

    /// A sequence for constructing a value.
    /// This may include tuple-like sequences.
    ConstructorSequence(ConstructorSequenceOperator),

    /// A type with just one anonymous property
    ValueType(ValueOperator),

    /// A type with multiple anonymous properties, equivalent to a union of types
    Union(UnionOperator),

    /// A map with one property, "_id"
    Id(SerdeOperatorId),

    /// A type with many properties
    Map(MapOperator),
}

#[derive(Debug)]
pub struct RelationSequenceOperator {
    // note: This is constant size array so that it can produce a dynamic slice
    pub ranges: [SequenceRange; 1],
    pub def_variant: DefVariant,
}

#[derive(Debug)]
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
#[derive(Clone, Debug)]
pub struct SequenceRange {
    /// Operator to use for this range
    pub operator_id: SerdeOperatorId,
    /// If this range is finite, this represents the number of repetitions.
    /// If None, this range is infinite and accepts 0 or more items.
    /// An infinite range must be the last range in the sequence to make any sense.
    pub finite_repetition: Option<u16>,
}

#[derive(Debug)]
pub struct ValueOperator {
    pub typename: String,
    pub def_variant: DefVariant,
    pub inner_operator_id: SerdeOperatorId,
}

#[derive(Debug)]
pub struct UnionOperator {
    typename: String,
    union_def_variant: DefVariant,
    variants: Vec<ValueUnionVariant>,
}

impl UnionOperator {
    pub fn new(
        typename: String,
        union_def_variant: DefVariant,
        variants: Vec<ValueUnionVariant>,
    ) -> Self {
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

    pub fn variants(&self) -> &[ValueUnionVariant] {
        &self.variants
    }
}

#[derive(Clone, Debug)]
pub struct ValueUnionVariant {
    pub discriminator: VariantDiscriminator,
    pub operator_id: SerdeOperatorId,
}

#[derive(Clone, Debug)]
pub struct MapOperator {
    pub typename: String,
    pub def_variant: DefVariant,
    pub properties: IndexMap<String, SerdeProperty>,
    pub n_mandatory_properties: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct SerdeProperty {
    pub property_id: PropertyId,
    pub value_operator_id: SerdeOperatorId,
    pub optional: bool,
    pub rel_params_operator_id: Option<SerdeOperatorId>,
}
