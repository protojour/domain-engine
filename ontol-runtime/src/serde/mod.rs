use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
    ops::Range,
};

use derive_debug_extras::DebugExtras;
use indexmap::IndexMap;
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    discriminator::VariantDiscriminator,
    env::Env,
    format_utils::{Backticks, CommaSeparated, DoubleQuote},
    value::PropertyId,
    DataModifier, DefId, DefVariant,
};

mod deserialize;
mod deserialize_matcher;
mod serialize;

const EDGE_PROPERTY: &str = "_edge";
const ID_PROPERTY: &str = "_id";

#[derive(Copy, Clone, Debug)]
pub enum ProcessorLevel {
    Root,
    Child,
}

/// SerdeProcessor is handle serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the compilerironment.
#[derive(Clone, Copy)]
pub struct SerdeProcessor<'e> {
    /// The operator used for (de)serializing this value
    pub(crate) value_operator: &'e SerdeOperator,

    /// The edge properties used for (de)serializing the _edge data_
    /// related to this value when it's associated with a "parent value" through a relation.
    ///
    /// Generally, non-unit edge data can only be represented on a relation between two map types.
    /// The parent (often the subject) map has an attribute that is another child map.
    /// The edge data would be injected in the child map as the `_edge` property.
    pub(crate) rel_params_operator_id: Option<SerdeOperatorId>,

    pub(crate) level: ProcessorLevel,

    /// The environment, via which new SerdeOperators can be created.
    pub(crate) env: &'e Env,
}

impl<'e> SerdeProcessor<'e> {
    /// Return a processor that helps to _narrow the value_ that this processor represents.
    pub fn narrow(&self, operator_id: SerdeOperatorId) -> Self {
        self.env
            .new_serde_processor(operator_id, self.rel_params_operator_id, self.level)
    }

    pub fn narrow_with_rel(
        &self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        self.env
            .new_serde_processor(operator_id, rel_params_operator_id, self.level)
    }

    /// Return a processor that processes a new value that is a child of value of this processor.
    pub fn new_child(&self, operator_id: SerdeOperatorId) -> Self {
        self.env
            .new_serde_processor(operator_id, None, ProcessorLevel::Child)
    }

    pub fn new_child_with_rel(
        &self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        self.env
            .new_serde_processor(operator_id, rel_params_operator_id, ProcessorLevel::Child)
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.search_property(prop, self.value_operator)
    }

    fn search_property(&self, prop: &str, operator: &'e SerdeOperator) -> Option<PropertyId> {
        match operator {
            SerdeOperator::ValueUnionType(union_type) => {
                for discriminator in &union_type.variants {
                    let next_operator = self.narrow(discriminator.operator_id);
                    if let Some(property_id) =
                        self.search_property(prop, next_operator.value_operator)
                    {
                        return Some(property_id);
                    }
                }

                None
            }
            SerdeOperator::MapType(map_type) => map_type
                .properties
                .get(prop)
                .map(|serde_property| serde_property.property_id),
            _ => None,
        }
    }
}

/// SerdeOperatorId is an index into a vector of SerdeOperators.
#[derive(Clone, Copy, Eq, PartialEq, Hash, DebugExtras)]
#[debug_single_tuple_inline]
pub struct SerdeOperatorId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SerdeKey {
    Def(DefVariant),
    Intersection(Box<BTreeSet<SerdeKey>>),
}

impl SerdeKey {
    pub const fn identity(def_id: DefId) -> Self {
        Self::Def(DefVariant::new(def_id, DataModifier::IDENTITY))
    }
}

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
    RelationSequence(RelationSequenceType),

    /// A sequence for constructing a value.
    /// This may include tuple-like sequences.
    ConstructorSequence(ConstructorSequenceType),

    /// A type with just one anonymous property
    ValueType(ValueType),

    /// A type with multiple anonymous properties, equivalent to a union of types
    ValueUnionType(ValueUnionType),

    /// A map with one property, "_id"
    Id(SerdeOperatorId),

    /// A type with many properties
    MapType(MapType),
}

impl SerdeOperator {
    pub fn type_def_id(&self) -> DefId {
        match self {
            Self::Unit => DefId::unit(),
            Self::Int(def_id) => *def_id,
            Self::Number(def_id) => *def_id,
            Self::String(def_id) => *def_id,
            Self::StringConstant(_, def_id) => *def_id,
            Self::StringPattern(def_id) => *def_id,
            Self::CapturingStringPattern(def_id) => *def_id,
            Self::RelationSequence(seq) => seq.def_variant.def_id,
            Self::ConstructorSequence(seq) => seq.def_variant.def_id,
            Self::ValueType(ty) => ty.def_variant.def_id,
            Self::ValueUnionType(ty) => ty.union_def_variant.def_id,
            Self::Id(_) => todo!("Should include DefId of the Id type"),
            Self::MapType(ty) => ty.def_variant.def_id,
        }
    }
}

#[derive(Debug)]
pub struct RelationSequenceType {
    // note: This is constant size array so that it can produce a dynamic slice
    pub ranges: [SequenceRange; 1],
    pub def_variant: DefVariant,
}

#[derive(Debug)]
pub struct ConstructorSequenceType {
    pub ranges: SmallVec<[SequenceRange; 3]>,
    pub def_variant: DefVariant,
}

impl ConstructorSequenceType {
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
pub struct ValueType {
    pub typename: String,
    pub def_variant: DefVariant,
    pub inner_operator_id: SerdeOperatorId,
}

#[derive(Debug)]
pub struct ValueUnionType {
    pub typename: String,
    pub union_def_variant: DefVariant,
    pub variants: Vec<ValueUnionVariant>,
}

#[derive(Clone, Debug)]
pub struct ValueUnionVariant {
    pub discriminator: VariantDiscriminator,
    pub operator_id: SerdeOperatorId,
}

#[derive(Clone, Debug)]
pub struct MapType {
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

impl<'e> Debug for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This structure might contain cycles (through operator id),
        // so just print the topmost level.
        f.debug_struct("SerdeProcessor")
            .field("operator", self.value_operator)
            .field("rel_params_operator_id", &self.rel_params_operator_id)
            .finish()
    }
}

impl<'e> Display for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value_operator {
            SerdeOperator::Unit => write!(f, "unit"),
            SerdeOperator::Int(_) => write!(f, "`int`"),
            SerdeOperator::Number(_) => write!(f, "`number`"),
            SerdeOperator::String(_) => write!(f, "`string`"),
            SerdeOperator::StringConstant(lit, _) => DoubleQuote(lit).fmt(f),
            SerdeOperator::StringPattern(_) | SerdeOperator::CapturingStringPattern(_) => {
                write!(f, "`string_pattern`")
            }
            SerdeOperator::RelationSequence(sequence_type) => {
                write!(
                    f,
                    "[{}..]",
                    self.narrow(sequence_type.ranges[0].operator_id)
                )
            }
            SerdeOperator::ConstructorSequence(sequence_type) => {
                let mut processors = vec![];
                let mut finite = true;
                for range in &sequence_type.ranges {
                    if let Some(finite_repetition) = range.finite_repetition {
                        for _ in 0..finite_repetition {
                            processors.push(self.narrow(range.operator_id));
                        }
                        finite = true;
                    } else {
                        finite = false;
                        processors.push(self.narrow(range.operator_id));
                    }
                }

                let dot_dot = if finite { "" } else { ".." };

                write!(
                    f,
                    "[{processors}{dot_dot}]",
                    processors = CommaSeparated(&processors)
                )
            }
            SerdeOperator::ValueType(value_type) => Backticks(&value_type.typename).fmt(f),
            SerdeOperator::ValueUnionType(_) => write!(f, "union"),
            SerdeOperator::Id(_) => write!(f, "id"),
            SerdeOperator::MapType(map_type) => Backticks(&map_type.typename).fmt(f),
        }
    }
}
