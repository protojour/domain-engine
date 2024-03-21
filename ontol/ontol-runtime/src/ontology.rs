//! The ONTOL ontology.

use std::ops::Index;

use ::serde::{Deserialize, Serialize};
use arcstr::ArcStr;
use bincode::Options;
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
    domain::{Domain, ExtendedEntityInfo, TypeInfo},
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

/// The Ontology is the model of a single ONTOL runtime environment.
pub struct Ontology {
    pub(crate) data: Data,
}

/// All of the information that makes an Ontology.
#[derive(Serialize, Deserialize)]
pub struct Data {
    pub(crate) const_proc_table: FnvHashMap<DefId, Procedure>,
    pub(crate) map_meta_table: FnvHashMap<MapKey, MapMeta>,
    pub(crate) named_forward_maps: FnvHashMap<(PackageId, TextConstant), MapKey>,
    pub(crate) text_like_types: FnvHashMap<DefId, TextLikeType>,
    pub(crate) text_patterns: FnvHashMap<DefId, TextPattern>,
    pub(crate) extern_table: FnvHashMap<DefId, Extern>,
    pub(crate) lib: Lib,

    /// The text constants are stored using ArcStr because it's only one word wide,
    /// (length is stored on the heap) and which makes the vector as dense as possible:
    text_constants: Vec<ArcStr>,

    domain_table: FnvHashMap<PackageId, Domain>,
    extended_entity_table: FnvHashMap<DefId, ExtendedEntityInfo>,
    ontol_domain_meta: OntolDomainMeta,
    union_variants: FnvHashMap<DefId, Box<[DefId]>>,
    domain_interfaces: FnvHashMap<PackageId, Vec<DomainInterface>>,
    package_config_table: FnvHashMap<PackageId, PackageConfig>,
    docs: FnvHashMap<DefId, Vec<String>>,
    serde_operators: Vec<SerdeOperator>,
    dynamic_sequence_operator_addr: SerdeOperatorAddr,
    value_generators: FnvHashMap<RelationshipId, ValueGenerator>,
    property_flows: Vec<PropertyFlow>,
}

fn bincode_config() -> impl bincode::Options {
    bincode::options()
        .with_little_endian()
        .with_varint_encoding()
        .reject_trailing_bytes()
}

impl Ontology {
    /// Make a builder for building an Ontology from scratch.
    pub fn builder() -> OntologyBuilder {
        builder::new_builder()
    }

    /// Deserialize an Ontology using the bincode format.
    pub fn try_from_bincode(reader: impl std::io::Read) -> Result<Self, bincode::Error> {
        let data: Data = bincode_config().deserialize_from(reader)?;
        Ok(Self::from(data))
    }

    /// Serialize an ontology to bincode.
    pub fn try_serialize_to_bincode(
        &self,
        writer: impl std::io::Write,
    ) -> Result<(), bincode::Error> {
        bincode_config().serialize_into(writer, &self.data)
    }

    /// Access the ontology's [Data].
    /// The data is a serializable value.
    pub fn data(&self) -> &Data {
        &self.data
    }

    /// Make a value debuggable using OntolDebug.
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
        let docs = self.data.docs.get(&def_id)?;
        if docs.is_empty() {
            None
        } else {
            Some(docs.join("\n"))
        }
    }

    pub fn get_text_pattern(&self, def_id: DefId) -> Option<&TextPattern> {
        self.data.text_patterns.get(&def_id)
    }

    pub fn get_text_like_type(&self, def_id: DefId) -> Option<TextLikeType> {
        self.data.text_like_types.get(&def_id).cloned()
    }

    pub fn domains(&self) -> impl Iterator<Item = (&PackageId, &Domain)> {
        self.data.domain_table.iter()
    }

    pub fn ontol_domain_meta(&self) -> &OntolDomainMeta {
        &self.data.ontol_domain_meta
    }

    pub fn find_domain(&self, package_id: PackageId) -> Option<&Domain> {
        self.data.domain_table.get(&package_id)
    }

    /// Get the members of a given union.
    /// Returns an empty slice if it's not a union.
    pub fn union_variants(&self, union_def_id: DefId) -> &[DefId] {
        self.data
            .union_variants
            .get(&union_def_id)
            .map(|slice| slice.as_ref())
            .unwrap_or(&[])
    }

    pub fn extended_entity_info(&self, def_id: DefId) -> Option<&ExtendedEntityInfo> {
        self.data.extended_entity_table.get(&def_id)
    }

    pub fn get_package_config(&self, package_id: PackageId) -> Option<&PackageConfig> {
        self.data.package_config_table.get(&package_id)
    }

    pub fn domain_interfaces(&self, package_id: PackageId) -> &[DomainInterface] {
        self.data
            .domain_interfaces
            .get(&package_id)
            .map(|interfaces| interfaces.as_slice())
            .unwrap_or(&[])
    }

    pub fn get_const_proc(&self, const_id: DefId) -> Option<Procedure> {
        self.data.const_proc_table.get(&const_id).cloned()
    }

    pub fn iter_map_meta(&self) -> impl Iterator<Item = (MapKey, &MapMeta)> + '_ {
        self.data
            .map_meta_table
            .iter()
            .map(|(key, proc)| (*key, proc))
    }

    pub fn get_map_meta(&self, key: &MapKey) -> Option<&MapMeta> {
        self.data.map_meta_table.get(key)
    }

    pub fn get_prop_flow_slice(&self, map_meta: &MapMeta) -> Option<&[PropertyFlow]> {
        let range = map_meta.propflow_range.as_ref()?;
        Some(&self.data.property_flows[range.start as usize..range.end as usize])
    }

    pub fn get_mapper_proc(&self, key: &MapKey) -> Option<Procedure> {
        self.data.map_meta_table.get(key).map(|map_info| {
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
            value_operator: &self.data.serde_operators[value_addr.0 as usize],
            ctx: Default::default(),
            level: ProcessorLevel::new_root(),
            ontology: self,
            profile: &DOMAIN_PROFILE,
            mode,
        }
    }

    pub fn dynamic_sequence_operator_addr(&self) -> SerdeOperatorAddr {
        self.data.dynamic_sequence_operator_addr
    }

    pub fn get_value_generator(&self, relationship_id: RelationshipId) -> Option<&ValueGenerator> {
        self.data.value_generators.get(&relationship_id)
    }

    pub fn get_extern(&self, def_id: DefId) -> Option<&Extern> {
        self.data.extern_table.get(&def_id)
    }

    pub fn get_text_constant(&self, constant: TextConstant) -> &ArcStr {
        &self.data.text_constants[constant.0 as usize]
    }

    /// Find a text constant given its string representation.
    /// NOTE: This intentionally has linear search complexity.
    /// It's only use case should be testing.
    pub fn find_text_constant(&self, str: &str) -> Option<TextConstant> {
        self.data
            .text_constants
            .iter()
            .enumerate()
            .find(|(_, arcstr)| arcstr.as_str() == str)
            .map(|(index, _)| TextConstant(index as u32))
    }

    /// This primarily exists for testing only.
    pub fn find_named_forward_map_meta(&self, package_id: PackageId, name: &str) -> Option<MapKey> {
        let text_constant = self.find_text_constant(name)?;
        self.data
            .named_forward_maps
            .get(&(package_id, text_constant))
            .cloned()
    }
}

impl From<Data> for Ontology {
    fn from(value: Data) -> Self {
        let mut ontology = Ontology { data: value };

        let mut serde_operators = std::mem::take(&mut ontology.data.serde_operators);
        let mut interfaces = std::mem::take(&mut ontology.data.domain_interfaces);

        for operator in serde_operators.iter_mut() {
            operator.ontology_init(&ontology);
        }

        for (_, interfaces) in interfaces.iter_mut() {
            for interface in interfaces {
                interface.ontology_init(&ontology);
            }
        }

        ontology.data.serde_operators = serde_operators;
        ontology.data.domain_interfaces = interfaces;

        ontology
    }
}

/// Things that must be done after deserialization before the ontology can be used
pub trait OntologyInit {
    fn ontology_init(&mut self, ontology: &Ontology);
}

impl Index<TextConstant> for Ontology {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        &self.data.text_constants[index.0 as usize]
    }
}

impl Index<SerdeOperatorAddr> for Ontology {
    type Output = SerdeOperator;

    fn index(&self, index: SerdeOperatorAddr) -> &Self::Output {
        &self.data.serde_operators[index.0 as usize]
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
