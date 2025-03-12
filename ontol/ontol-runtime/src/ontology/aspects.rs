use std::ops::Index;

use arcstr::ArcStr;
use fnv::FnvHashMap;
use ontol_core::debug::OntolFormatter;
use serde::{Deserialize, Serialize};

use crate::{
    DefId, DefIdSet, DomainIndex, MapKey, PropId,
    interface::{
        DomainInterface,
        serde::operator::{SerdeOperator, SerdeOperatorAddr},
    },
    query::condition::Condition,
    vec_map::VecMap,
    vm::proc::{Lib, Procedure},
};

use super::{
    config::DomainConfig,
    domain::{Def, Domain, EdgeInfo, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{OntolDomainMeta, TextConstant, TextLikeType, text_pattern::TextPattern},
};

/// fetch aspect, turbofish style
pub fn aspect<T>(source: &impl AsRef<T>) -> &T {
    source.as_ref()
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize, Debug)]
    pub struct OntologyAspects: u8 {
        const DEFS          = 0b00000001;
        const SERDE         = 0b00000010;
        const DOCUMENTATION = 0b00000100;
        const INTERFACE     = 0b00001000;
        const EXECTUTION    = 0b00010000;
        const CONFIG        = 0b00100000;
    }
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

impl AsRef<DefsAspect> for DefsAspect {
    fn as_ref(&self) -> &DefsAspect {
        self
    }
}

impl DefsAspect {
    pub fn domain_by_index(&self, index: DomainIndex) -> Option<&Domain> {
        self.domains.get(&index)
    }

    pub fn domains(&self) -> impl Iterator<Item = (DomainIndex, &Domain)> {
        self.domains
            .iter()
            .map(|(idx, domain)| (DomainIndex(idx as u16), domain))
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

    pub fn find_edge(&self, id: DefId) -> Option<&EdgeInfo> {
        let domain = self.domain_by_index(id.0)?;
        domain.find_edge(id)
    }

    /// Get the members of a given union.
    /// Returns an empty slice if it's not a union.
    pub fn union_variants(&self, union_def_id: DefId) -> &[DefId] {
        self.union_variants
            .get(&union_def_id)
            .map(|set| set.as_slice())
            .unwrap_or(&[])
    }

    pub fn get_text_pattern(&self, def_id: DefId) -> Option<&TextPattern> {
        self.text_patterns.get(&def_id)
    }

    pub fn get_text_like_type(&self, def_id: DefId) -> Option<TextLikeType> {
        self.text_like_types.get(&def_id).cloned()
    }

    pub(crate) fn empty() -> Self {
        Self {
            domains: Default::default(),
            extended_entity_table: Default::default(),
            ontol_domain_meta: Default::default(),
            text_constants: vec![],
            text_like_types: Default::default(),
            text_patterns: Default::default(),
            union_variants: Default::default(),
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
        constant: u32,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{str:?}", str = &self[TextConstant(constant)])
    }
}

#[derive(Serialize, Deserialize)]
pub struct SerdeAspect {
    pub(crate) operators: Vec<SerdeOperator>,
    pub(crate) dynamic_sequence_operator_addr: SerdeOperatorAddr,
}

impl SerdeAspect {
    pub(crate) fn empty() -> Self {
        Self {
            operators: Default::default(),
            dynamic_sequence_operator_addr: SerdeOperatorAddr(u32::MAX),
        }
    }
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

impl DocumentationAspect {
    pub(crate) fn empty() -> Self {
        Self {
            def_docs: Default::default(),
            prop_docs: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct InterfaceAspect {
    pub(crate) interfaces: FnvHashMap<DomainIndex, Vec<DomainInterface>>,
}

impl InterfaceAspect {
    pub(crate) fn empty() -> Self {
        Self {
            interfaces: Default::default(),
        }
    }
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

impl ExecutionAspect {
    pub(crate) fn empty() -> Self {
        Self {
            const_proc_table: Default::default(),
            map_meta_table: Default::default(),
            static_conditions: Default::default(),
            lib: Lib::default(),
            property_flows: Default::default(),
            named_downmaps: Default::default(),
            extern_table: Default::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ConfigAspect {
    pub(crate) domain_config_table: FnvHashMap<DomainIndex, DomainConfig>,
}

impl ConfigAspect {
    pub(crate) fn empty() -> Self {
        Self {
            domain_config_table: Default::default(),
        }
    }
}
