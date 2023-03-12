use smartstring::alias::String;

use crate::{DefId, DefVariant, RelationId};

#[derive(Debug)]
pub struct UnionDiscriminator {
    pub variants: Vec<VariantDiscriminator>,
}

#[derive(Clone, Debug)]
pub struct VariantDiscriminator {
    pub discriminant: Discriminant,
    pub purpose: VariantPurpose,
    pub def_variant: DefVariant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum VariantPurpose {
    Identification,
    Data,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Discriminant {
    IsUnit,
    IsInt,
    IsString,
    IsStringLiteral(String),
    IsSequence,
    MatchesCapturingStringPattern(DefId),
    /// Matches any map
    MapFallback,
    /// Matches a map that has a single property, and only that property will be used for information
    IsSingletonProperty(RelationId, String),
    HasStringAttribute(RelationId, String, String),
    HasAttributeMatchingStringPattern(RelationId, String, DefId),
}
