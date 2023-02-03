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
    IsInt,
    IsString,
    IsStringLiteral(String),
    HasProperty(RelationId, String),
    HasStringAttribute(RelationId, String, String),
}
