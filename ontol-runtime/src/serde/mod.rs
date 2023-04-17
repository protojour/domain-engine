use std::collections::BTreeSet;

use crate::{DataModifier, DefId, DefVariant};

use self::operator::{SerdeOperatorId, StructOperator};

mod deserialize;
mod deserialize_matcher;
mod serialize;

pub mod operator;
pub mod processor;

const EDGE_PROPERTY: &str = "_edge";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SerdeKey {
    Def(DefVariant),
    Intersection(Box<BTreeSet<SerdeKey>>),
}

impl SerdeKey {
    pub const fn no_modifier(def_id: DefId) -> Self {
        Self::Def(DefVariant::new(def_id, DataModifier::NONE))
    }
}
