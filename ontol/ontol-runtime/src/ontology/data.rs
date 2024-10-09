use ::serde::{Deserialize, Serialize};
use arcstr::ArcStr;
use fnv::FnvHashMap;

use crate::{
    interface::{
        serde::operator::{SerdeOperator, SerdeOperatorAddr},
        DomainInterface,
    },
    query::condition::Condition,
    vec_map::VecMap,
    vm::proc::{Lib, Procedure},
    DefId, DefIdSet, DomainIndex, MapKey, PropId,
};

use super::{
    config::DomainConfig,
    domain::{Domain, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{text_pattern::TextPattern, OntolDomainMeta, TextConstant, TextLikeType},
};

/// All of the information that makes an Ontology.
#[derive(Serialize, Deserialize)]
pub struct Data {
    pub(crate) const_proc_table: FnvHashMap<DefId, Procedure>,
    pub(crate) map_meta_table: FnvHashMap<MapKey, MapMeta>,
    pub(crate) static_conditions: FnvHashMap<MapKey, Condition>,
    pub(crate) named_downmaps: FnvHashMap<(DomainIndex, TextConstant), MapKey>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
    pub(crate) text_patterns: FnvHashMap<DefId, TextPattern>,
    pub(crate) extern_table: FnvHashMap<DefId, Extern>,
    pub(crate) lib: Lib,

    /// The text constants are stored using ArcStr because it's only one word wide,
    /// (length is stored on the heap) and which makes the vector as dense as possible:
    pub(crate) text_constants: Vec<ArcStr>,

    pub(crate) domains: VecMap<DomainIndex, Domain>,
    pub(crate) extended_entity_table: FnvHashMap<DefId, ExtendedEntityInfo>,
    pub(crate) ontol_domain_meta: OntolDomainMeta,
    pub(crate) union_variants: FnvHashMap<DefId, DefIdSet>,
    pub(crate) domain_interfaces: FnvHashMap<DomainIndex, Vec<DomainInterface>>,
    pub(crate) domain_config_table: FnvHashMap<DomainIndex, DomainConfig>,
    pub(crate) def_docs: FnvHashMap<DefId, ArcStr>,
    pub(crate) prop_docs: FnvHashMap<PropId, ArcStr>,
    pub(crate) serde_operators: Vec<SerdeOperator>,
    pub(crate) dynamic_sequence_operator_addr: SerdeOperatorAddr,
    pub(crate) property_flows: Vec<PropertyFlow>,
}
