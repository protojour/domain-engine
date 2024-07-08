use ::serde::{Deserialize, Serialize};
use ontol_macros::OntolDebug;

use crate::{ontology::ontol::TextConstant, DefId, RelationshipId};

#[derive(Clone, Serialize, Deserialize, OntolDebug, Debug)]
pub struct VariantDiscriminator {
    pub discriminant: Discriminant,
    pub purpose: VariantPurpose,
}

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug,
)]
pub enum VariantPurpose {
    /// The purpose is providing _data_ for the data type, NOT entity identification
    Data,
    /// The purpose is to identify ID fields
    Identification,
    /// A combination of the two above, used in Raw deserialization:
    /// Must dynamically support both the full data AND IdSingletonStruct if only the primary id is given.
    RawDynamicEntity,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug)]
pub enum Discriminant {
    MatchesLeaf(LeafDiscriminant),
    /// Has _any_ attribute that matches discriminant
    HasAttribute(RelationshipId, TextConstant, LeafDiscriminant),
    /// Matches any struct
    StructFallback,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug)]
pub enum LeafDiscriminant {
    IsAny,
    IsUnit,
    IsInt,
    IsIntLiteral(i64),
    IsText,
    IsTextLiteral(TextConstant),
    IsSequence,
    MatchesCapturingTextPattern(DefId),
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct LeafDiscriminantScalarUnion: u8 {
        const UNIT     = 0b00000001;
        const INT      = 0b00000010;
        const TEXT     = 0b00000100;
        const SEQUENCE = 0b00001000;
    }
}

/// Returns the type-set of scalar matchers for HasAttribute discriminants
pub fn leaf_discriminant_scalar_union_for_has_attribute<'a>(
    discriminant_iterator: impl Iterator<Item = &'a Discriminant>,
) -> LeafDiscriminantScalarUnion {
    let mut union = LeafDiscriminantScalarUnion::empty();

    for discriminant in discriminant_iterator {
        let Discriminant::HasAttribute(.., leaf_discriminant) = discriminant else {
            continue;
        };
        match leaf_discriminant {
            LeafDiscriminant::IsAny => {}
            LeafDiscriminant::IsUnit => {
                union |= LeafDiscriminantScalarUnion::UNIT;
            }
            LeafDiscriminant::IsInt | LeafDiscriminant::IsIntLiteral(_) => {
                union |= LeafDiscriminantScalarUnion::INT;
            }
            LeafDiscriminant::IsText
            | LeafDiscriminant::IsTextLiteral(_)
            | LeafDiscriminant::MatchesCapturingTextPattern(_) => {
                union |= LeafDiscriminantScalarUnion::TEXT;
            }
            LeafDiscriminant::IsSequence => {
                union |= LeafDiscriminantScalarUnion::SEQUENCE;
            }
        }
    }

    union
}
