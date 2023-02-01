use smartstring::alias::String;

use crate::{DefId, PropertyId};

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
    IsNumber,
    IsString,
    HasProperty(PropertyId, String),
}
