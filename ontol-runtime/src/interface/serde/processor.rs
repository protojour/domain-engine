use std::fmt::{Debug, Display};

use crate::{
    format_utils::{Backticks, CommaSeparated, DoubleQuote},
    ontology::Ontology,
    value::PropertyId,
};

use super::operator::{FilteredVariants, SerdeOperator, SerdeOperatorAddr, SerdePropertyFlags};

#[derive(Copy, Clone, Debug)]
pub enum ProcessorMode {
    Create,
    Read,
    Update,
    /// Used for unconditionally handling all fields.
    /// Should not be used for serialization or deserialization at the domain interface layer.
    Raw,
}

#[derive(Copy, Clone, Debug)]
pub struct ProcessorLevel {
    level: u16,
    recursion_limit: u16,
}

#[derive(Clone, Default)]
pub struct ProcessorProfile {
    pub overridden_id_property_key: Option<&'static str>,
    pub ignored_property_keys: &'static [&'static str],
    pub id_format: ScalarFormat,
}

#[derive(Clone, Copy, Default)]
pub enum ScalarFormat {
    /// Serialize scalars just like the domain says
    #[default]
    DomainTransparent,
    /// Serialize scalars as raw strings without any domain context
    /// Example: `fmt '' => 'prefix/' => uuid => id` becomes just an UUID-as-string.
    RawText,
}

/// The standard profile for domain serialization/deserialization
pub(crate) static DOMAIN_PROFILE: ProcessorProfile = ProcessorProfile {
    overridden_id_property_key: None,
    ignored_property_keys: &[],
    id_format: ScalarFormat::DomainTransparent,
};

/// Maximum number of nested/recursive operators.
const DEFAULT_RECURSION_LIMIT: u16 = 128;

impl ProcessorLevel {
    pub const fn new_root() -> Self {
        Self {
            level: 0,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
        }
    }

    pub const fn new_root_with_recursion_limit(limit: u16) -> Self {
        Self {
            level: 0,
            recursion_limit: limit,
        }
    }

    pub const fn new_child() -> Self {
        Self {
            level: 1,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
        }
    }

    pub const fn child(self) -> Result<Self, RecursionLimitError> {
        if self.level == self.recursion_limit {
            return Err(RecursionLimitError);
        }
        Ok(Self {
            level: self.level + 1,
            recursion_limit: self.recursion_limit,
        })
    }

    pub const fn is_root(&self) -> bool {
        self.level == 0
    }

    pub const fn current_level(&self) -> u16 {
        self.level
    }
}

pub struct RecursionLimitError;

impl RecursionLimitError {
    pub fn to_ser_error<E: serde::ser::Error>(self) -> E {
        E::custom("Recursion limit exceeded")
    }

    pub fn to_de_error<E: serde::de::Error>(self) -> E {
        E::custom("Recursion limit exceeded")
    }
}

#[derive(Clone, Copy, Default, Debug)]
pub struct SubProcessorContext {
    pub parent_property_id: Option<PropertyId>,
    pub parent_property_flags: SerdePropertyFlags,

    /// The edge properties used for (de)serializing the _edge data_
    /// related to this value when it's associated with a "parent value" through a relation.
    ///
    /// Generally, non-unit edge data can only be represented on a relation between two map types.
    /// The parent (often the subject) map has an attribute that is another child map.
    /// The edge data would be injected in the child map as the `_edge` property.
    pub rel_params_addr: Option<SerdeOperatorAddr>,
}

/// SerdeProcessor handles serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the runtime ontology.
#[derive(Clone, Copy)]
pub struct SerdeProcessor<'on, 'p> {
    /// The operator used for (de)serializing this value
    pub value_operator: &'on SerdeOperator,

    pub ctx: SubProcessorContext,

    /// The ontology, via which new SerdeOperators can be created.
    pub(crate) ontology: &'on Ontology,

    pub(crate) profile: &'p ProcessorProfile,

    pub(crate) mode: ProcessorMode,
    pub(crate) level: ProcessorLevel,
}

impl<'on, 'p> SerdeProcessor<'on, 'p> {
    /// Get the current processor level
    pub fn level(&self) -> ProcessorLevel {
        self.level
    }

    /// Set the processor profile to be used with this processor
    pub fn with_profile(self, profile: &'p ProcessorProfile) -> Self {
        Self { profile, ..self }
    }

    /// Set the processor level to be used with this processor
    pub fn with_level(self, level: ProcessorLevel) -> Self {
        Self { level, ..self }
    }

    /// Return a processor that helps to _narrow the value_ that this processor represents.
    pub fn narrow(&self, addr: SerdeOperatorAddr) -> Self {
        Self {
            value_operator: self.ontology.get_serde_operator(addr),
            ..*self
        }
    }

    /// Return a processor that helps to _narrow the value_ that this processor represents.
    pub fn narrow_with_context(&self, addr: SerdeOperatorAddr, ctx: SubProcessorContext) -> Self {
        Self {
            value_operator: self.ontology.get_serde_operator(addr),
            ctx,
            ..*self
        }
    }

    /// Return a processor that processes a new value that is a child value (i.e. increases the recursion level) of this processor.
    pub fn new_child(&self, addr: SerdeOperatorAddr) -> Result<Self, RecursionLimitError> {
        Ok(Self {
            value_operator: self.ontology.get_serde_operator(addr),
            ctx: Default::default(),
            level: self.level.child()?,
            ..*self
        })
    }

    pub fn new_child_with_context(
        &self,
        addr: SerdeOperatorAddr,
        ctx: SubProcessorContext,
    ) -> Result<Self, RecursionLimitError> {
        Ok(Self {
            value_operator: self.ontology.get_serde_operator(addr),
            ctx,
            level: self.level.child()?,
            ..*self
        })
    }

    pub fn scalar_format(&self) -> ScalarFormat {
        if self
            .ctx
            .parent_property_flags
            .contains(SerdePropertyFlags::ENTITY_ID)
        {
            self.profile.id_format
        } else {
            ScalarFormat::DomainTransparent
        }
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.search_property(prop, self.value_operator)
    }

    fn search_property(&self, prop: &str, operator: &'on SerdeOperator) -> Option<PropertyId> {
        match operator {
            SerdeOperator::Union(union_op) => match union_op.variants(self.mode, self.level) {
                FilteredVariants::Single(addr) => self.narrow(addr).find_property(prop),
                FilteredVariants::Union(variants) => {
                    for variant in variants {
                        if let Some(property_id) = self.narrow(variant.addr).find_property(prop) {
                            return Some(property_id);
                        }
                    }

                    None
                }
            },
            SerdeOperator::Struct(struct_op) => struct_op
                .properties
                .get(prop)
                .map(|serde_property| serde_property.property_id),
            _ => None,
        }
    }
}

impl<'on, 'p> Debug for SerdeProcessor<'on, 'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This structure might contain cycles (through operator addr),
        // so just print the topmost level.
        f.debug_struct("SerdeProcessor")
            .field("operator", self.value_operator)
            .field("rel_params_addr", &self.ctx.rel_params_addr)
            .finish()
    }
}

impl<'on, 'p> Display for SerdeProcessor<'on, 'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value_operator {
            SerdeOperator::Unit => write!(f, "unit"),
            SerdeOperator::False(_) => write!(f, "false"),
            SerdeOperator::True(_) => write!(f, "true"),
            SerdeOperator::Boolean(_) => write!(f, "bool"),
            SerdeOperator::I64(..) | SerdeOperator::I32(..) => write!(f, "`int`"),
            SerdeOperator::F64(..) => write!(f, "`float`"),
            SerdeOperator::String(_) => write!(f, "`string`"),
            SerdeOperator::StringConstant(lit, _) => DoubleQuote(lit).fmt(f),
            SerdeOperator::TextPattern(_) | SerdeOperator::CapturingTextPattern(_) => {
                write!(f, "`text_pattern`")
            }
            SerdeOperator::DynamicSequence => {
                write!(f, "[?..]")
            }
            SerdeOperator::RelationSequence(seq_op) => {
                write!(f, "[{}..]", self.narrow(seq_op.ranges[0].addr))
            }
            SerdeOperator::ConstructorSequence(seq_op) => {
                let mut processors = vec![];
                let mut finite = true;
                for range in &seq_op.ranges {
                    if let Some(finite_repetition) = range.finite_repetition {
                        for _ in 0..finite_repetition {
                            processors.push(self.narrow(range.addr));
                        }
                        finite = true;
                    } else {
                        finite = false;
                        processors.push(self.narrow(range.addr));
                    }
                }

                let dot_dot = if finite { "" } else { ".." };

                write!(
                    f,
                    "[{processors}{dot_dot}]",
                    processors = CommaSeparated(&processors)
                )
            }
            SerdeOperator::Alias(alias_op) => Backticks(&alias_op.typename).fmt(f),
            SerdeOperator::Union(_) => write!(f, "union"),
            SerdeOperator::IdSingletonStruct(..) => write!(f, "id"),
            SerdeOperator::Struct(struct_op) => Backticks(&struct_op.typename).fmt(f),
        }
    }
}
