use std::ops::Index;

use arcstr::ArcStr;
use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    debug::OntolFormatter,
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
    domain::{Def, Domain, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{text_pattern::TextPattern, OntolDomainMeta, TextConstant, TextLikeType},
};

/// get aspect, turbofish style
pub fn get_aspect<T>(source: &impl AsRef<T>) -> &T {
    source.as_ref()
}

#[derive(Serialize, Deserialize)]
pub struct DefsAspect {
    pub(crate) domains: VecMap<DomainIndex, Domain>,
    pub(crate) text_constants: Vec<ArcStr>,
    pub(crate) extended_entity_table: FnvHashMap<DefId, ExtendedEntityInfo>,
    pub(crate) ontol_domain_meta: OntolDomainMeta,
    pub(crate) union_variants: FnvHashMap<DefId, DefIdSet>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
    pub(crate) text_patterns: FnvHashMap<DefId, TextPattern>,
}

impl DefsAspect {
    pub fn domain_by_index(&self, index: DomainIndex) -> Option<&Domain> {
        self.domains.get(&index)
    }

    pub fn def(&self, def_id: DefId) -> &Def {
        match self.domain_by_index(def_id.0) {
            Some(domain) => domain.def(def_id),
            None => {
                panic!("No domain for {:?}", def_id.0)
            }
        }
    }

    pub fn get_def(&self, def_id: DefId) -> Option<&Def> {
        match self.domain_by_index(def_id.0) {
            Some(domain) => domain.def_option(def_id),
            None => None,
        }
    }
}

impl Index<TextConstant> for DefsAspect {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        &self.text_constants[index.0 as usize]
    }
}

impl OntolFormatter for DefsAspect {
    fn fmt_text_constant(
        &self,
        constant: TextConstant,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{str:?}", str = &self[constant])
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerdeAspect {
    pub(crate) operators: Vec<SerdeOperator>,
    pub(crate) dynamic_sequence_operator_addr: SerdeOperatorAddr,
}

impl Index<SerdeOperatorAddr> for SerdeAspect {
    type Output = SerdeOperator;

    fn index(&self, index: SerdeOperatorAddr) -> &Self::Output {
        &self.operators[index.0 as usize]
    }
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
pub struct ExecutionAspect {
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
