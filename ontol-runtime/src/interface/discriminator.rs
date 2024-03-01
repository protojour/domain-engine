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
    /// The purpose is explusively for identifying some entity
    Identification { entity_id: DefId },
    /// The purpose is providing _data_ for the data type, NOT entity identification
    Data,
    /// A combination of the two above, used in Raw deserialization:
    /// Must dynamically support both the full data AND IdSingletonStruct if only the primary id is given.
    RawDynamicEntity,
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
    IsIntLiteral(i64),
    IsText,
    IsTextLiteral(String),
    IsSequence,
    MatchesCapturingTextPattern(DefId),
}
