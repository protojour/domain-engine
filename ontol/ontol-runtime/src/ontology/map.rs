//! Data models related to ONTOL definition mapping

use std::ops::Range;

use ontol_macros::OntolDebug;
use serde::{Deserialize, Serialize};

use crate::{DefId, MapDirection, PropId, property::Cardinality, var::Var, vm::proc::Procedure};

use super::ontol::TextConstant;

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct MapMeta {
    pub procedure: Procedure,
    pub propflow_range: Option<Range<u32>>,
    pub direction: MapDirection,
    pub lossiness: MapLossiness,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, OntolDebug)]
pub enum MapLossiness {
    Complete,
    Lossy,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug)]
pub struct PropertyFlow {
    pub id: PropId,
    pub data: PropertyFlowData,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug)]
pub enum PropertyFlowData {
    UnitType(DefId),
    TupleType(u8, DefId),
    Match(Var),
    Cardinality(Cardinality),
    ChildOf(PropId),
    DependentOn(PropId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Extern {
    HttpJson { url: TextConstant },
}
