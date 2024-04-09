use arcstr::ArcStr;
use fnv::FnvHashMap;

use crate::{
    interface::{
        serde::operator::{SerdeOperator, SerdeOperatorAddr},
        DomainInterface,
    },
    vm::proc::{Lib, Procedure},
    DefId, MapKey, PackageId, RelationshipId,
};

use super::{
    config::PackageConfig,
    domain::{Domain, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{
        text_pattern::TextPattern, OntolDomainMeta, TextConstant, TextLikeType, ValueGenerator,
    },
    Data, Ontology,
};

pub struct OntologyBuilder {
    ontology: Ontology,
}

impl OntologyBuilder {
    /// Access to the partial ontology being built
    pub fn partial_ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn add_domain(&mut self, package_id: PackageId, domain: Domain) {
        self.ontology.data.domain_table.insert(package_id, domain);
    }

    pub fn add_package_config(&mut self, package_id: PackageId, config: PackageConfig) {
        self.ontology
            .data
            .package_config_table
            .insert(package_id, config);
    }

    pub fn union_variants(mut self, table: FnvHashMap<DefId, Box<[DefId]>>) -> Self {
        self.data().union_variants = table;
        self
    }

    pub fn extended_entity_info(mut self, table: FnvHashMap<DefId, ExtendedEntityInfo>) -> Self {
        self.data().extended_entity_table = table;
        self
    }

    pub fn text_constants(mut self, text_constants: Vec<ArcStr>) -> Self {
        self.data().text_constants = text_constants;
        self
    }

    pub fn ontol_domain_meta(mut self, meta: OntolDomainMeta) -> Self {
        self.data().ontol_domain_meta = meta;
        self
    }

    pub fn domain_interfaces(
        mut self,
        interfaces: FnvHashMap<PackageId, Vec<DomainInterface>>,
    ) -> Self {
        self.data().domain_interfaces = interfaces;
        self
    }

    pub fn docs(mut self, docs: FnvHashMap<DefId, TextConstant>) -> Self {
        self.data().docs = docs;
        self
    }

    pub fn lib(mut self, lib: Lib) -> Self {
        self.data().lib = lib;
        self
    }

    pub fn const_procs(mut self, const_procs: FnvHashMap<DefId, Procedure>) -> Self {
        self.data().const_proc_table = const_procs;
        self
    }

    pub fn map_meta_table(mut self, map_meta_table: FnvHashMap<MapKey, MapMeta>) -> Self {
        self.data().map_meta_table = map_meta_table;
        self
    }

    pub fn named_forward_maps(
        mut self,
        named_forward_maps: FnvHashMap<(PackageId, TextConstant), MapKey>,
    ) -> Self {
        self.data().named_forward_maps = named_forward_maps;
        self
    }

    pub fn serde_operators(mut self, operators: Vec<SerdeOperator>) -> Self {
        self.data().serde_operators = operators;
        self
    }

    pub fn dynamic_sequence_operator_addr(mut self, addr: SerdeOperatorAddr) -> Self {
        self.data().dynamic_sequence_operator_addr = addr;
        self
    }

    pub fn property_flows(mut self, flows: Vec<PropertyFlow>) -> Self {
        self.data().property_flows = flows;
        self
    }

    pub fn string_like_types(mut self, types: FnvHashMap<DefId, TextLikeType>) -> Self {
        self.data().text_like_types = types;
        self
    }

    pub fn text_patterns(mut self, patterns: FnvHashMap<DefId, TextPattern>) -> Self {
        self.data().text_patterns = patterns;
        self
    }

    pub fn externs(mut self, externs: FnvHashMap<DefId, Extern>) -> Self {
        self.data().extern_table = externs;
        self
    }

    pub fn value_generators(
        mut self,
        generators: FnvHashMap<RelationshipId, ValueGenerator>,
    ) -> Self {
        self.data().value_generators = generators;
        self
    }

    fn data(&mut self) -> &mut Data {
        &mut self.ontology.data
    }

    pub fn build(self) -> Ontology {
        self.ontology
    }
}

pub(super) fn new_builder() -> OntologyBuilder {
    OntologyBuilder {
        ontology: Ontology {
            data: Data {
                text_constants: vec![],
                const_proc_table: Default::default(),
                map_meta_table: Default::default(),
                named_forward_maps: Default::default(),
                text_like_types: Default::default(),
                text_patterns: Default::default(),
                extern_table: Default::default(),
                ontol_domain_meta: Default::default(),
                domain_table: Default::default(),
                extended_entity_table: Default::default(),
                union_variants: Default::default(),
                domain_interfaces: Default::default(),
                package_config_table: Default::default(),
                docs: Default::default(),
                lib: Lib::default(),
                serde_operators: Default::default(),
                dynamic_sequence_operator_addr: SerdeOperatorAddr(u32::MAX),
                value_generators: Default::default(),
                property_flows: Default::default(),
            },
        },
    }
}
