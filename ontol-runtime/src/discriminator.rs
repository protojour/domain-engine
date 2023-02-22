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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Discriminant {
    IsUnit,
    IsInt,
    IsString,
    IsStringLiteral(String),
    IsSequence,
    MapFallback,
    MatchesCapturingStringPattern,
    HasProperty(RelationId, String),
    HasStringAttribute(RelationId, String, String),
}
