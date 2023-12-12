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
    Identification { entity_id: DefId },
    Data,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum Discriminant {
    MatchesLeaf(LeafDiscriminant),
    HasAttribute(RelationshipId, String, LeafDiscriminant),
    /// Matches any struct
    StructFallback,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum LeafDiscriminant {
    IsAny,
    IsUnit,
    IsInt,
    IsText,
    IsTextLiteral(String),
    IsSequence,
    MatchesCapturingTextPattern(DefId),
}
