use arcstr::ArcStr;
use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize)]
pub struct DomainAspect {
    pub(crate) domains: VecMap<DomainIndex, Domain>,
    pub(crate) text_constants: Vec<ArcStr>,
    pub(crate) extended_entity_table: FnvHashMap<DefId, ExtendedEntityInfo>,
    pub(crate) ontol_domain_meta: OntolDomainMeta,
    pub(crate) union_variants: FnvHashMap<DefId, DefIdSet>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
    pub(crate) text_patterns: FnvHashMap<DefId, TextPattern>,
}

#[derive(Serialize, Deserialize)]
pub struct SerdeAspect {
    pub(crate) operators: Vec<SerdeOperator>,
    pub(crate) dynamic_sequence_operator_addr: SerdeOperatorAddr,
}

#[derive(Serialize, Deserialize)]
pub struct DocumentationAspect {
    pub(crate) def_docs: FnvHashMap<DefId, ArcStr>,
    pub(crate) prop_docs: FnvHashMap<PropId, ArcStr>,
}

#[derive(Serialize, Deserialize)]
pub struct InterfaceAspect {
    pub(crate) interfaces: FnvHashMap<DomainIndex, Vec<DomainInterface>>,
}

#[derive(Serialize, Deserialize)]
pub struct MappingAspect {
    pub(crate) const_proc_table: FnvHashMap<DefId, Procedure>,
    pub(crate) map_meta_table: FnvHashMap<MapKey, MapMeta>,
    pub(crate) static_conditions: FnvHashMap<MapKey, Condition>,
    pub(crate) named_downmaps: FnvHashMap<(DomainIndex, TextConstant), MapKey>,
    pub(crate) extern_table: FnvHashMap<DefId, Extern>,
    pub(crate) lib: Lib,
    pub(crate) property_flows: Vec<PropertyFlow>,
}

#[derive(Serialize, Deserialize)]
pub struct ConfigAspect {
    pub(crate) domain_config_table: FnvHashMap<DomainIndex, DomainConfig>,
}
