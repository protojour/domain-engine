use smartstring::alias::String;

use crate::{DefId, RelationId};

#[derive(Debug)]
pub struct UnionDiscriminator {
    pub variants: Vec<VariantDiscriminator>,
}

#[derive(Clone, Debug)]
pub struct VariantDiscriminator {
    pub discriminant: Discriminant,
    pub result_type: DefId,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Discriminant {
    IsUnit,
    IsInt,
    IsString,
    IsStringLiteral(String),
    IsSequence,
    MatchesCapturingStringPattern,
    /// Matches any map
    MapFallback,
    /// Matches a map that has a single property, and only that property will be used for information
    IsSingletonProperty(RelationId, String),
    HasStringAttribute(RelationId, String, String),
    HasAttributeMatchingStringPattern(RelationId, String, DefId),
}
