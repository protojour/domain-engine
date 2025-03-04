use std::{
    fmt::Debug,
    ops::{Range, RangeInclusive},
};

use ::serde::{Deserialize, Serialize};
use bit_set::BitSet;
use ontol_macros::OntolDebug;
use thin_vec::ThinVec;

use crate::{
    DefId, PropId, impl_ontol_debug,
    interface::discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    ontology::{
        OntologyInit,
        ontol::{TextConstant, ValueGenerator},
    },
    phf::{PhfIndexMap, PhfKey},
};

use super::{
    SerdeDef,
    processor::{ProcessorLevel, ProcessorMode, ProcessorProfileFlags},
};

/// SerdeOperatorAddr is an index into a vector of SerdeOperators.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct SerdeOperatorAddr(pub u32);

impl ::std::fmt::Debug for SerdeOperatorAddr {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "SerdeOperatorAddr({:?})", self.0)
    }
}

impl_ontol_debug!(SerdeOperatorAddr);

#[derive(Serialize, Deserialize, OntolDebug)]
pub enum SerdeOperator {
    AnyPlaceholder,
    Unit,
    True(DefId),
    False(DefId),
    Boolean(DefId),
    I32(DefId, Option<RangeInclusive<i32>>),
    I64(DefId, Option<RangeInclusive<i64>>),
    F64(DefId, Option<RangeInclusive<f64>>),
    Serial(DefId),
    String(DefId),
    StringConstant(TextConstant, DefId),

    Octets(OctetsOperator),

    /// Always deserializes into text, ignores capture groups:
    TextPattern(DefId),

    /// Deserializes into a Struct if there are capture groups:
    CapturingTextPattern(DefId),

    /// Special operator for serialization, serializes each value
    /// in a raw manner based on its runtime type information.
    DynamicSequence,

    /// A sequence representing a relationship between one subject and many objects.
    /// This is simple and does not support any tuples.
    RelationList(RelationSequenceOperator),

    /// A sequence representing a relationship between one subject and many objects,
    /// where duplicates are disallowed.
    /// This is simple and does not support any tuples.
    RelationIndexSet(RelationSequenceOperator),

    /// A sequence for constructing a value.
    /// This may include tuple-like sequences.
    ConstructorSequence(ConstructorSequenceOperator),

    /// A type with just one anonymous property
    Alias(AliasOperator),

    /// A type with multiple anonymous properties, equivalent to a union of types
    Union(Box<UnionOperator>),

    /// A type with many properties
    Struct(Box<StructOperator>),

    /// A map with one property: The ID of an entity.
    IdSingletonStruct(DefId, TextConstant, SerdeOperatorAddr),
}

impl OntologyInit for SerdeOperator {
    fn ontology_init(&mut self, ontology: &crate::ontology::Ontology) {
        if let Self::Struct(struct_op) = self {
            struct_op.properties.ontology_init(ontology);
        }
    }
}

#[derive(Serialize, Deserialize, OntolDebug)]
pub struct OctetsOperator {
    pub target_def_id: DefId,
    pub format: DefId,
}

#[derive(Serialize, Deserialize, OntolDebug)]
pub struct RelationSequenceOperator {
    /// A singleton range
    pub range: SequenceRange,
    pub def: SerdeDef,
    pub to_entity: bool,
}

#[derive(Serialize, Deserialize, OntolDebug)]
pub struct ConstructorSequenceOperator {
    pub ranges: ThinVec<SequenceRange>,
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
#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct SequenceRange {
    /// Operator to use for this range
    pub addr: SerdeOperatorAddr,
    /// If this range is finite, this represents the number of repetitions.
    /// If None, this range is infinite and accepts 0 or more items.
    /// An infinite range must be the last range in the sequence to make any sense.
    pub finite_repetition: Option<u16>,
}

#[derive(Serialize, Deserialize, OntolDebug)]
pub struct AliasOperator {
    pub typename: TextConstant,
    pub def: SerdeDef,
    pub inner_addr: SerdeOperatorAddr,
}

#[derive(Serialize, Deserialize, OntolDebug)]
pub struct UnionOperator {
    typename: TextConstant,
    union_def: SerdeDef,
    variants: Vec<SerdeUnionVariant>,
}

impl UnionOperator {
    /// Note: variants must be sorted according to their purpose (VariantPurpose)
    pub fn new(
        typename: TextConstant,
        union_def: SerdeDef,
        variants: Vec<SerdeUnionVariant>,
    ) -> Self {
        Self {
            typename,
            union_def,
            variants,
        }
    }

    pub fn typename(&self) -> TextConstant {
        self.typename
    }

    pub fn union_def(&self) -> SerdeDef {
        self.union_def
    }

    /// Get the variant(s) that applies in the given context
    pub fn applied_deserialize_variants(
        &self,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> AppliedVariants<'_> {
        PossibleVariants::new(&self.variants, mode, level).applied_deserialize()
    }

    pub fn applied_serialize_variants(
        &self,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> AppliedVariants<'_> {
        PossibleVariants::new(&self.variants, mode, level).applied_deserialize()
    }

    pub fn unfiltered_variants(&self) -> &[SerdeUnionVariant] {
        &self.variants
    }

    pub fn unfiltered_discriminants(&self) -> impl Iterator<Item = &Discriminant> {
        self.variants
            .iter()
            .map(|variant| &variant.discriminator.discriminant)
    }
}

pub enum AppliedVariants<'on> {
    Unambiguous(SerdeOperatorAddr),
    OneOf(PossibleVariants<'on>),
}

#[derive(Clone, Copy, OntolDebug)]
pub struct PossibleVariants<'on> {
    all_variants: &'on [SerdeUnionVariant],
    mode: ProcessorMode,
    level: ProcessorLevel,
}

impl<'on> PossibleVariants<'on> {
    pub fn new(
        all_variants: &'on [SerdeUnionVariant],
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> Self {
        Self {
            all_variants,
            mode,
            level,
        }
    }

    pub fn applied_deserialize(self) -> AppliedVariants<'on> {
        if let Some(variant) = self.into_iter().find_unambiguous_next() {
            AppliedVariants::Unambiguous(variant.deserialize.addr)
        } else {
            AppliedVariants::OneOf(self)
        }
    }

    pub fn applied_serialize(self) -> AppliedVariants<'on> {
        if let Some(variant) = self.into_iter().find_unambiguous_next() {
            AppliedVariants::Unambiguous(variant.serialize.addr)
        } else {
            AppliedVariants::OneOf(self)
        }
    }
}

impl<'on> IntoIterator for PossibleVariants<'on> {
    type IntoIter = PossibleVariantsIter<'on>;
    type Item = &'on SerdeUnionVariant;

    fn into_iter(self) -> Self::IntoIter {
        PossibleVariantsIter {
            inner_iter: self.all_variants.iter(),
            mode: self.mode,
            level: self.level,
        }
    }
}

pub struct PossibleVariantsIter<'on> {
    inner_iter: std::slice::Iter<'on, SerdeUnionVariant>,
    mode: ProcessorMode,
    level: ProcessorLevel,
}

impl<'on> Iterator for PossibleVariantsIter<'on> {
    type Item = &'on SerdeUnionVariant;

    fn next(&mut self) -> Option<Self::Item> {
        // note: this continues iteration where the previous next() call left off
        for variant in self.inner_iter.by_ref() {
            if let Some(possible) = Self::filter_possible(variant, self.mode, self.level) {
                return Some(possible);
            }
        }

        None
    }
}

impl<'on> PossibleVariantsIter<'on> {
    fn find_unambiguous_next(&mut self) -> Option<&'on SerdeUnionVariant> {
        let variant = self.next()?;
        if self.next().is_some() {
            None
        } else {
            Some(variant)
        }
    }

    fn filter_possible(
        variant: &'on SerdeUnionVariant,
        mode: ProcessorMode,
        _level: ProcessorLevel,
    ) -> Option<&'on SerdeUnionVariant> {
        match (mode, variant.discriminator.purpose) {
            (ProcessorMode::Delete, VariantPurpose::Data) => None,
            _ => Some(variant),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct SerdeUnionVariant {
    pub discriminator: VariantDiscriminator,
    pub deserialize: SerdeDefAddr,
    pub serialize: SerdeDefAddr,
}

impl SerdeUnionVariant {
    pub fn discriminant(&self) -> &Discriminant {
        &self.discriminator.discriminant
    }

    pub fn purpose(&self) -> &VariantPurpose {
        &self.discriminator.purpose
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub struct SerdeDefAddr {
    pub def_id: DefId,
    pub addr: SerdeOperatorAddr,
}

impl SerdeDefAddr {
    pub fn new(def_id: DefId, addr: SerdeOperatorAddr) -> Self {
        Self { def_id, addr }
    }
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct StructOperator {
    pub typename: TextConstant,
    pub def: SerdeDef,
    pub flags: SerdeStructFlags,
    pub properties: PhfIndexMap<SerdeProperty>,
}

impl StructOperator {
    /// Are the struct properties empty (save the special _edge property)
    pub fn is_struct_properties_empty(&self) -> bool {
        self.struct_properties().next().is_none()
    }

    pub fn struct_properties(&self) -> impl Iterator<Item = (&PhfKey, &SerdeProperty)> {
        self.properties
            .iter()
            .filter(|(_, property)| !property.is_rel_params())
    }

    pub fn filter_properties(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropId>,
        profile_flags: ProcessorProfileFlags,
    ) -> impl Iterator<Item = (&PhfKey, &SerdeProperty)> {
        self.properties.iter().filter(move |(_, property)| {
            property
                .filter(mode, parent_property_id, profile_flags)
                .is_some()
        })
    }

    pub fn required_count(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropId>,
        profile_flags: ProcessorProfileFlags,
    ) -> usize {
        if profile_flags.contains(ProcessorProfileFlags::ALL_PROPS_OPTIONAL) {
            return 0;
        }

        self.filter_properties(mode, parent_property_id, profile_flags)
            .filter(|(_, property)| !property.is_optional_for(mode, &profile_flags))
            .count()
    }

    /// Get the bitset of required props
    /// NB: The bitset has the order of the _inner_ PhfMap, not the ordered PhfIndexMap
    pub fn required_props_bitset(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropId>,
        profile_flags: ProcessorProfileFlags,
    ) -> BitSet {
        if profile_flags.contains(ProcessorProfileFlags::ALL_PROPS_OPTIONAL) {
            BitSet::default()
        } else {
            BitSet::from_bit_vec(
                self.properties
                    .raw_map()
                    .iter()
                    .map(|(_, property)| {
                        if let Some(property) =
                            property.filter(mode, parent_property_id, profile_flags)
                        {
                            !property.is_optional_for(mode, &profile_flags)
                        } else {
                            false
                        }
                    })
                    .collect(),
            )
        }
    }
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct SerdeProperty {
    /// The ID of this property
    pub id: PropId,

    /// The operator addr for the value of this property
    pub value_addr: SerdeOperatorAddr,

    /// Various flags
    pub flags: SerdePropertyFlags,

    /// Value generator
    pub value_generator: Option<ValueGenerator>,

    pub kind: SerdePropertyKind,
}

impl SerdeProperty {
    #[inline]
    pub fn is_optional(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::OPTIONAL)
    }

    /// Is it the special rel_params / "_edge" property?
    #[inline]
    pub fn is_rel_params(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::REL_PARAMS)
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
            || self.is_generator()
            || profile_flags.contains(ProcessorProfileFlags::ALL_PROPS_OPTIONAL)
    }

    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::READ_ONLY)
    }

    #[inline]
    pub fn is_write_once(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::WRITE_ONCE)
    }

    #[inline]
    pub fn is_generator(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::GENERATOR)
    }

    #[inline]
    pub fn is_entity_id(&self) -> bool {
        self.flags.contains(SerdePropertyFlags::ENTITY_ID)
    }

    #[inline]
    pub fn filter(
        &self,
        mode: ProcessorMode,
        parent_property_id: Option<PropId>,
        profile_flags: ProcessorProfileFlags,
    ) -> Option<&Self> {
        match mode {
            ProcessorMode::Create => {
                if self.is_read_only() && !self.is_entity_id() {
                    return None;
                }
            }
            ProcessorMode::Update | ProcessorMode::GraphqlUpdate => {
                if (self.is_read_only() && !self.is_entity_id()) || self.is_write_once() {
                    return None;
                }
            }
            ProcessorMode::Read
            | ProcessorMode::Raw
            | ProcessorMode::RawTreeOnly
            | ProcessorMode::Delete => {}
        }

        if false {
            if let Some(parent_property_id) = parent_property_id {
                // Filter out if this property is the mirrored property of the parent property
                if self.id == parent_property_id
                // && self.property_id.role != parent_property_id.role
                && !profile_flags.contains(ProcessorProfileFlags::ALLOW_STRUCTURALLY_CIRCULAR_PROPS)
                {
                    return None;
                }
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

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum SerdePropertyKind {
    Plain {
        rel_params_addr: Option<SerdeOperatorAddr>,
    },
    /// The property is the entry point to a flattened union.
    /// The property itself is the flattened union's discriminating property.
    /// Interacting with the inner structure of the type requires resolving the union variant.
    /// The union_addr must be a UnionOperator address.
    FlatUnionDiscriminator { union_addr: SerdeOperatorAddr },
    /// A property that belongs to some flattened union variant.
    /// The full set of properties that belong to at least one flattened variant
    /// must be explicitly inlucded in the parent structure using this kind.
    FlatUnionData,
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize, Debug)]
    pub struct SerdePropertyFlags: u8 {
        const OPTIONAL         = 0b00000001;
        /// Property represents parameters to a relation: e.g. "_edge"
        const REL_PARAMS       = 0b00000010;
        const READ_ONLY        = 0b00000100;
        /// Only (directly) writable when constructing resource for the first time:
        const WRITE_ONCE       = 0b00001000;
        const GENERATOR        = 0b00010000;
        /// The property is an identifier of the entity that owns the property
        const ENTITY_ID        = 0b00100000;
        /// The property is an identifier of any entity (self or foreign)
        const ANY_ID           = 0b01000000;
        /// Property is part of the entity graph
        const IN_ENTITY_GRAPH  = 0b10000000;
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize, Debug)]
    pub struct SerdeStructFlags: u8 {
        /// This struct operator supports open/domainless properties
        const OPEN_DATA          = 0b00000001;
        /// A "proper entity" struct is a struct that has at least two properties:
        /// at least one ID property and at least one non-ID/data property.
        /// The consequence for non-proper entities that has only an ID and no other data,
        /// is that they are always deserialized in their struct form.
        const PROPER_ENTITY      = 0b00000010;
        const ENTITY_ID_OPTIONAL = 0b00000100;
    }
}

impl_ontol_debug!(SerdePropertyFlags);
impl_ontol_debug!(SerdeStructFlags);
