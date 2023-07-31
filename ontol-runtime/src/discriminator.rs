use ::serde::{Deserialize, Serialize};
use smartstring::alias::String;

use crate::{DefId, DefVariant, RelationshipId};

#[derive(Debug, Serialize, Deserialize)]
pub struct UnionDiscriminator {
    pub variants: Vec<VariantDiscriminator>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VariantDiscriminator {
    pub discriminant: Discriminant,
    pub purpose: VariantPurpose,
    pub def_variant: DefVariant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum VariantPurpose {
    Identification,
    Data,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
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
    IsSingletonProperty(RelationshipId, String),
    HasStringAttribute(RelationshipId, String, String),
    HasAttributeMatchingStringPattern(RelationshipId, String, DefId),
}
