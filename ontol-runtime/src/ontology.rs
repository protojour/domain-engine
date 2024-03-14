//! The ONTOL ontology.

use std::ops::Index;

use ::serde::{Deserialize, Serialize};
use arcstr::ArcStr;
use fnv::FnvHashMap;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    debug::{self, OntolFormatter},
    interface::{
        serde::{
            operator::{SerdeOperator, SerdeOperatorAddr},
            processor::{ProcessorLevel, ProcessorMode, SerdeProcessor, DOMAIN_PROFILE},
        },
        DomainInterface,
    },
    vm::{
        ontol_vm::OntolVm,
        proc::{Lib, Procedure},
    },
    DefId, MapKey, PackageId, RelationshipId,
};

use self::{
    builder::OntologyBuilder,
    config::PackageConfig,
    domain::{Domain, TypeInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{
        text_pattern::TextPattern, OntolDomainMeta, TextConstant, TextLikeType, ValueGenerator,
    },
};

pub mod builder;
pub mod config;
pub mod domain;
pub mod map;
pub mod ontol;

/// The Ontology is the model of an ONTOL runtime environment.
#[derive(Serialize, Deserialize)]
pub struct Ontology {
    pub(crate) const_proc_table: FnvHashMap<DefId, Procedure>,
    pub(crate) map_meta_table: FnvHashMap<MapKey, MapMeta>,
    pub(crate) named_forward_maps: FnvHashMap<(PackageId, TextConstant), MapKey>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
    pub(crate) text_patterns: FnvHashMap<DefId, TextPattern>,
    pub(crate) extern_table: FnvHashMap<DefId, Extern>,
    pub(crate) lib: Lib,
    pub(crate) ontol_domain_meta: OntolDomainMeta,

    /// The text constants are stored using ArcStr because it's only one word wide,
    /// (length is stored on the heap) and which makes the vector as dense as possible:
    text_constants: Vec<ArcStr>,

    domain_table: FnvHashMap<PackageId, Domain>,
    union_variants: FnvHashMap<DefId, Box<[DefId]>>,
    domain_interfaces: FnvHashMap<PackageId, Vec<DomainInterface>>,
    package_config_table: FnvHashMap<PackageId, PackageConfig>,
    docs: FnvHashMap<DefId, Vec<String>>,
    serde_operators: Vec<SerdeOperator>,
    dynamic_sequence_operator_addr: SerdeOperatorAddr,
    value_generators: FnvHashMap<RelationshipId, ValueGenerator>,
    property_flows: Vec<PropertyFlow>,
}

impl Ontology {
    pub fn builder() -> OntologyBuilder {
        builder::new_builder()
    }

    pub fn try_from_bincode(reader: impl std::io::Read) -> Result<Self, bincode::Error> {
        bincode::deserialize_from(reader)
    }

    pub fn try_serialize_to_bincode(
        &self,
        writer: impl std::io::Write,
    ) -> Result<(), bincode::Error> {
        bincode::serialize_into(writer, self)
    }

    pub fn debug<'a, T: ?Sized>(&'a self, value: &'a T) -> debug::Fmt<'a, &'a T> {
        debug::Fmt(self, value)
    }

    pub fn new_vm(&self, proc: Procedure) -> OntolVm<'_> {
        OntolVm::new(self, proc)
    }

    pub fn get_type_info(&self, def_id: DefId) -> &TypeInfo {
        match self.find_domain(def_id.0) {
            Some(domain) => domain.type_info(def_id),
            None => {
                panic!("No domain for {:?}", def_id.0)
            }
        }
    }

    pub fn get_docs(&self, def_id: DefId) -> Option<std::string::String> {
        let docs = self.docs.get(&def_id)?;
        if docs.is_empty() {
            None
        } else {
            Some(docs.join("\n"))
        }
    }

    pub fn get_text_pattern(&self, def_id: DefId) -> Option<&TextPattern> {
        self.text_patterns.get(&def_id)
    }

    pub fn get_text_like_type(&self, def_id: DefId) -> Option<TextLikeType> {
        self.text_like_types.get(&def_id).cloned()
    }

    pub fn domains(&self) -> impl Iterator<Item = (&PackageId, &Domain)> {
        self.domain_table.iter()
    }

    pub fn ontol_domain_meta(&self) -> &OntolDomainMeta {
        &self.ontol_domain_meta
    }

    pub fn find_domain(&self, package_id: PackageId) -> Option<&Domain> {
        self.domain_table.get(&package_id)
    }

    /// Get the members of a given union.
    /// Returns an empty slice if it's not a union.
    pub fn union_variants(&self, union_def_id: DefId) -> &[DefId] {
        self.union_variants
            .get(&union_def_id)
            .map(|slice| slice.as_ref())
            .unwrap_or(&[])
    }

    pub fn get_package_config(&self, package_id: PackageId) -> Option<&PackageConfig> {
        self.package_config_table.get(&package_id)
    }

    pub fn domain_interfaces(&self, package_id: PackageId) -> &[DomainInterface] {
        self.domain_interfaces
            .get(&package_id)
            .map(|interfaces| interfaces.as_slice())
            .unwrap_or(&[])
    }

    pub fn get_const_proc(&self, const_id: DefId) -> Option<Procedure> {
        self.const_proc_table.get(&const_id).cloned()
    }

    pub fn iter_map_meta(&self) -> impl Iterator<Item = (MapKey, &MapMeta)> + '_ {
        self.map_meta_table.iter().map(|(key, proc)| (*key, proc))
    }

    pub fn get_map_meta(&self, key: &MapKey) -> Option<&MapMeta> {
        self.map_meta_table.get(key)
    }

    pub fn get_prop_flow_slice(&self, map_meta: &MapMeta) -> Option<&[PropertyFlow]> {
        let range = map_meta.propflow_range.as_ref()?;
        Some(&self.property_flows[range.start as usize..range.end as usize])
    }

    pub fn get_mapper_proc(&self, key: &MapKey) -> Option<Procedure> {
        self.map_meta_table.get(key).map(|map_info| {
            debug!(
                "get_mapper_proc ({:?}) => {:?}",
                key.def_ids(),
                self.debug(&map_info.procedure)
            );
            map_info.procedure
        })
    }

    pub fn new_serde_processor(
        &self,
        value_addr: SerdeOperatorAddr,
        mode: ProcessorMode,
    ) -> SerdeProcessor {
        SerdeProcessor {
            value_operator: &self.serde_operators[value_addr.0 as usize],
            ctx: Default::default(),
            level: ProcessorLevel::new_root(),
            ontology: self,
            profile: &DOMAIN_PROFILE,
            mode,
        }
    }

    pub fn dynamic_sequence_operator_addr(&self) -> SerdeOperatorAddr {
        self.dynamic_sequence_operator_addr
    }

    pub fn get_value_generator(&self, relationship_id: RelationshipId) -> Option<&ValueGenerator> {
        self.value_generators.get(&relationship_id)
    }

    pub fn get_extern(&self, def_id: DefId) -> Option<&Extern> {
        self.extern_table.get(&def_id)
    }

    /// Find a text constant given its string representation.
    /// NOTE: This intentionally has linear search complexity.
    /// It's only use case should be testing.
    pub fn find_text_constant(&self, str: &str) -> Option<TextConstant> {
        self.text_constants
            .iter()
            .enumerate()
            .find(|(_, arcstr)| arcstr.as_str() == str)
            .map(|(index, _)| TextConstant(index as u32))
    }

    /// This primarily exists for testing only.
    pub fn find_named_forward_map_meta(&self, package_id: PackageId, name: &str) -> Option<MapKey> {
        let text_constant = self.find_text_constant(name)?;
        self.named_forward_maps
            .get(&(package_id, text_constant))
            .cloned()
    }
}

impl Index<TextConstant> for Ontology {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        &self.text_constants[index.0 as usize]
    }
}

impl Index<SerdeOperatorAddr> for Ontology {
    type Output = SerdeOperator;

    fn index(&self, index: SerdeOperatorAddr) -> &Self::Output {
        &self.serde_operators[index.0 as usize]
    }
}

impl OntolFormatter for Ontology {
    fn fmt_text_constant(
        &self,
        constant: TextConstant,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{str:?}", str = &self[constant])
    }
}
