use ::serde::{Deserialize, Serialize};
use smartstring::alias::String;

use crate::{interface::serde::SerdeDef, DefId, RelationshipId};

#[derive(Debug, Serialize, Deserialize)]
pub struct UnionDiscriminator {
    pub variants: Vec<VariantDiscriminator>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VariantDiscriminator {
    pub discriminant: Discriminant,
    pub purpose: VariantPurpose,
    pub serde_def: SerdeDef,
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
    IsText,
    IsTextLiteral(String),
    IsSequence,
    MatchesCapturingTextPattern(DefId),
    /// Matches any struct
    StructFallback,
    /// Matches a struct that has a single property, and only that property will be used for informationk
    IsSingletonProperty(RelationshipId, String),
    HasTextAttribute(RelationshipId, String, String),
    HasAttributeMatchingTextPattern(RelationshipId, String, DefId),
}
