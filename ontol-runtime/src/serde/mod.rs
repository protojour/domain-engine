use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
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
    DataVariant, DefId, DefVariant,
};

mod deserialize;
mod deserialize_matcher;
mod serialize;

const EDGE_PROPERTY: &str = "_edge";
const ID_PROPERTY: &str = "_id";

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

    /// The environment, via which new SerdeOperators can be created.
    pub(crate) env: &'e Env,
}

impl<'e> SerdeProcessor<'e> {
    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.search_property(prop, self.value_operator)
    }

    fn search_property(&self, prop: &str, operator: &'e SerdeOperator) -> Option<PropertyId> {
        match operator {
            SerdeOperator::ValueUnionType(union_type) => {
                for discriminator in &union_type.discriminators {
                    let next_operator = self
                        .env
                        .new_serde_processor(discriminator.operator_id, None);
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
    Variant(DefVariant),
    Intersection(Box<BTreeSet<SerdeKey>>),
}

impl SerdeKey {
    pub const fn variant(def_id: DefId, variant: DataVariant) -> Self {
        Self::Variant(DefVariant(def_id, variant))
    }

    pub const fn identity(def_id: DefId) -> Self {
        Self::Variant(DefVariant(def_id, DataVariant::Identity))
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

    /// Any sequence
    Sequence(SmallVec<[SequenceRange; 3]>, DefVariant),

    /// A type with just one anonymous property
    ValueType(ValueType),

    /// A type with multiple anonymous properties, equivalent to a union of types
    ValueUnionType(ValueUnionType),

    /// A map with one property, "_id"
    Id(SerdeOperatorId),

    /// A type with many properties
    MapType(MapType),
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
    pub discriminators: Vec<ValueUnionDiscriminator>,
}

#[derive(Clone, Debug)]
pub struct ValueUnionDiscriminator {
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
            SerdeOperator::Sequence(ranges, _) => {
                let mut processors = vec![];
                let mut finite = true;
                for range in ranges {
                    if let Some(finite_repetition) = range.finite_repetition {
                        for _ in 0..finite_repetition {
                            processors.push(self.env.new_serde_processor(range.operator_id, None));
                        }
                        finite = true;
                    } else {
                        finite = false;
                        processors.push(self.env.new_serde_processor(range.operator_id, None));
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
