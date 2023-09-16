use std::collections::BTreeSet;

use ::serde::{Deserialize, Serialize};

use crate::DefVariant;

use self::operator::{SerdeOperatorId, StructOperator};

mod deserialize;
mod deserialize_matcher;
mod serialize;

pub mod operator;
pub mod processor;

const EDGE_PROPERTY: &str = "_edge";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SerdeKey {
    Def(DefVariant),
    Intersection(Box<BTreeSet<SerdeKey>>),
}
