use std::sync::Arc;

use arcstr::ArcStr;
use fnv::FnvHashMap;

use crate::{
    interface::{
        serde::operator::{SerdeOperator, SerdeOperatorAddr},
        DomainInterface,
    },
    query::condition::Condition,
    vm::proc::{Lib, Procedure},
    DefId, DefIdSet, DomainIndex, MapKey, PropId,
};

use super::{
    aspects::{
        ConfigAspect, DefsAspect, DocumentationAspect, ExecutionAspect, InterfaceAspect,
        SerdeAspect,
    },
    config::DomainConfig,
    domain::{Domain, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{text_pattern::TextPattern, OntolDomainMeta, TextConstant, TextLikeType},
    Data, Ontology,
};

pub struct OntologyBuilder {
    defs: DefsAspect,
    serde: SerdeAspect,
    documentation: DocumentationAspect,
    interface: InterfaceAspect,
    execution: ExecutionAspect,
    config: ConfigAspect,
}

impl OntologyBuilder {
    pub fn add_domain(&mut self, index: DomainIndex, domain: Domain) {
        self.defs.domains.insert(index, domain);
    }

    pub fn domain_mut(&mut self, index: DomainIndex) -> &mut Domain {
        self.defs.domains.get_mut(&index).unwrap()
    }

    pub fn add_domain_config(&mut self, index: DomainIndex, config: DomainConfig) {
        self.config.domain_config_table.insert(index, config);
    }

    pub fn union_variants(mut self, table: FnvHashMap<DefId, DefIdSet>) -> Self {
        self.defs.union_variants = table;
        self
    }

    pub fn extended_entity_info(mut self, table: FnvHashMap<DefId, ExtendedEntityInfo>) -> Self {
        self.defs.extended_entity_table = table;
        self
    }

    pub fn text_constants(mut self, text_constants: Vec<ArcStr>) -> Self {
        self.defs.text_constants = text_constants;
        self
    }

    pub fn ontol_domain_meta(mut self, meta: OntolDomainMeta) -> Self {
        self.defs.ontol_domain_meta = meta;
        self
    }

    pub fn text_like_types(mut self, types: FnvHashMap<DefId, TextLikeType>) -> Self {
        self.defs.text_like_types = types;
        self
    }

    pub fn text_patterns(mut self, patterns: FnvHashMap<DefId, TextPattern>) -> Self {
        self.defs.text_patterns = patterns;
        self
    }

    pub fn domain_interfaces(
        mut self,
        interfaces: FnvHashMap<DomainIndex, Vec<DomainInterface>>,
    ) -> Self {
        self.interface.interfaces = interfaces;
        self
    }

    pub fn def_docs(mut self, docs: FnvHashMap<DefId, ArcStr>) -> Self {
        self.documentation.def_docs = docs;
        self
    }

    pub fn prop_docs(mut self, docs: FnvHashMap<PropId, ArcStr>) -> Self {
        self.documentation.prop_docs = docs;
        self
    }

    pub fn serde_operators(mut self, operators: Vec<SerdeOperator>) -> Self {
        self.serde.operators = operators;
        self
    }

    pub fn dynamic_sequence_operator_addr(mut self, addr: SerdeOperatorAddr) -> Self {
        self.serde.dynamic_sequence_operator_addr = addr;
        self
    }

    pub fn lib(mut self, lib: Lib) -> Self {
        self.execution.lib = lib;
        self
    }

    pub fn const_procs(mut self, const_procs: FnvHashMap<DefId, Procedure>) -> Self {
        self.execution.const_proc_table = const_procs;
        self
    }

    pub fn map_meta_table(mut self, map_meta_table: FnvHashMap<MapKey, MapMeta>) -> Self {
        self.execution.map_meta_table = map_meta_table;
        self
    }

    pub fn static_conditions(mut self, static_conditions: FnvHashMap<MapKey, Condition>) -> Self {
        self.execution.static_conditions = static_conditions;
        self
    }

    pub fn named_forward_maps(
        mut self,
        named_forward_maps: FnvHashMap<(DomainIndex, TextConstant), MapKey>,
    ) -> Self {
        self.execution.named_downmaps = named_forward_maps;
        self
    }

    pub fn property_flows(mut self, flows: Vec<PropertyFlow>) -> Self {
        self.execution.property_flows = flows;
        self
    }

    pub fn externs(mut self, externs: FnvHashMap<DefId, Extern>) -> Self {
        self.execution.extern_table = externs;
        self
    }

    /// Access to partially built defs aspect
    pub fn defs_aspect(&self) -> &DefsAspect {
        &self.defs
    }

    pub fn build(self) -> Ontology {
        Ontology {
            data: Data {
                defs: Arc::new(self.defs),
                serde: Arc::new(self.serde),
                documentation: Arc::new(self.documentation),
                interface: Arc::new(self.interface),
                execution: Arc::new(self.execution),
                config: Arc::new(self.config),
            },
        }
    }
}

pub(super) fn new_builder() -> OntologyBuilder {
    OntologyBuilder {
        defs: DefsAspect {
            domains: Default::default(),
            extended_entity_table: Default::default(),
            ontol_domain_meta: Default::default(),
            text_constants: vec![],
            text_like_types: Default::default(),
            text_patterns: Default::default(),
            union_variants: Default::default(),
        },
        serde: SerdeAspect {
            operators: Default::default(),
            dynamic_sequence_operator_addr: SerdeOperatorAddr(u32::MAX),
        },
        documentation: DocumentationAspect {
            def_docs: Default::default(),
            prop_docs: Default::default(),
        },
        interface: InterfaceAspect {
            interfaces: Default::default(),
        },
        execution: ExecutionAspect {
            const_proc_table: Default::default(),
            map_meta_table: Default::default(),
            static_conditions: Default::default(),
            lib: Lib::default(),
            property_flows: Default::default(),
            named_downmaps: Default::default(),
            extern_table: Default::default(),
        },
        config: ConfigAspect {
            domain_config_table: Default::default(),
        },
    }
}
