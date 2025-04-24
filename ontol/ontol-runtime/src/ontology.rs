//! The ONTOL ontology.

use std::{ops::Index, sync::Arc};

use arcstr::ArcStr;
use aspects::{
    ConfigAspect, DefsAspect, DocumentationAspect, ExecutionAspect, InterfaceAspect,
    OntologyAspects, SerdeAspect,
};
use domain::EdgeInfo;
use ontol_core::{DomainId, debug::OntolFormatter, tag::DomainIndex};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    DefId, MapKey, PropId,
    interface::{
        DomainInterface,
        serde::{
            operator::{SerdeOperator, SerdeOperatorAddr},
            processor::{ProcessorMode, SerdeProcessor},
        },
    },
    query::condition::Condition,
    vm::{ontol_vm::OntolVm, proc::Procedure},
};

use self::{
    builder::OntologyBuilder,
    config::DomainConfig,
    domain::{Def, Domain, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{OntolDomainMeta, TextConstant, TextLikeType, text_pattern::TextPattern},
};

pub mod aspects;
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
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Data {
    pub(crate) defs: Arc<DefsAspect>,
    pub(crate) serde: Arc<SerdeAspect>,
    pub(crate) documentation: Arc<DocumentationAspect>,
    pub(crate) interface: Arc<InterfaceAspect>,
    pub(crate) execution: Arc<ExecutionAspect>,
    pub(crate) config: Arc<ConfigAspect>,
}

impl Ontology {
    /// Make a builder for building an Ontology from scratch.
    pub fn builder() -> OntologyBuilder {
        builder::new_builder()
    }

    /// Deserialize an Ontology using the postcard format.
    pub fn try_from_postcard(bytes: &[u8]) -> Result<Self, postcard::Error> {
        let data: Data = postcard::from_bytes(bytes)?;
        Ok(Self::from(data))
    }

    /// Serialize an ontology to postcard.
    pub fn try_serialize_to_postcard(
        &self,
        writer: impl std::io::Write,
    ) -> Result<(), postcard::Error> {
        postcard::to_io(&self.data, writer)?;
        Ok(())
    }

    pub fn new_vm(&self, proc: Procedure) -> OntolVm<'_> {
        OntolVm::new(proc, self)
    }

    pub fn def(&self, def_id: DefId) -> &Def {
        self.data.defs.def(def_id)
    }

    pub fn get_def(&self, def_id: DefId) -> Option<&Def> {
        match self.domain_by_index(def_id.0) {
            Some(domain) => domain.def_option(def_id),
            None => None,
        }
    }

    pub fn get_def_docs(&self, def_id: DefId) -> Option<&ArcStr> {
        self.data.documentation.def_docs.get(&def_id)
    }

    pub fn get_prop_docs(&self, prop_id: PropId) -> Option<&ArcStr> {
        self.data.documentation.prop_docs.get(&prop_id)
    }

    pub fn get_text_pattern(&self, def_id: DefId) -> Option<&TextPattern> {
        self.data.defs.get_text_pattern(def_id)
    }

    pub fn get_text_like_type(&self, def_id: DefId) -> Option<TextLikeType> {
        self.data.defs.get_text_like_type(def_id)
    }

    pub fn domains(&self) -> impl Iterator<Item = (DomainIndex, &Domain)> {
        self.data.defs.domains()
    }

    pub fn ontol_domain_meta(&self) -> &OntolDomainMeta {
        &self.data.defs.ontol_domain_meta
    }

    pub fn domain_by_index(&self, index: DomainIndex) -> Option<&Domain> {
        self.data.defs.domain_by_index(index)
    }

    pub fn domain_by_id(&self, id: DomainId) -> Option<&Domain> {
        self.data
            .defs
            .domains
            .iter()
            .find(|(_, domain)| domain.domain_id().id == id)
            .map(|(_, domain)| domain)
    }

    /// Get the members of a given union.
    /// Returns an empty slice if it's not a union.
    pub fn union_variants(&self, union_def_id: DefId) -> &[DefId] {
        self.data.defs.union_variants(union_def_id)
    }

    pub fn find_edge(&self, id: DefId) -> Option<&EdgeInfo> {
        self.data.defs.find_edge(id)
    }

    pub fn extended_entity_info(&self, def_id: DefId) -> Option<&ExtendedEntityInfo> {
        self.data.defs.extended_entity_table.get(&def_id)
    }

    pub fn get_domain_config(&self, index: DomainIndex) -> Option<&DomainConfig> {
        self.data.config.domain_config_table.get(&index)
    }

    pub fn domain_interfaces(&self, index: DomainIndex) -> &[DomainInterface] {
        self.data
            .interface
            .interfaces
            .get(&index)
            .map(|interfaces| interfaces.as_slice())
            .unwrap_or(&[])
    }

    pub fn get_const_proc(&self, const_id: DefId) -> Option<Procedure> {
        self.data.execution.const_proc_table.get(&const_id).cloned()
    }

    pub fn iter_map_meta(&self) -> impl Iterator<Item = (MapKey, &MapMeta)> + use<'_> {
        self.data
            .execution
            .map_meta_table
            .iter()
            .map(|(key, proc)| (*key, proc))
    }

    pub fn iter_named_downmaps(
        &self,
    ) -> impl Iterator<Item = (DomainIndex, TextConstant, MapKey)> + use<'_> {
        self.data
            .execution
            .named_downmaps
            .iter()
            .map(|((index, text_constant), value)| (*index, *text_constant, *value))
    }

    pub fn get_map_meta(&self, key: &MapKey) -> Option<&MapMeta> {
        self.data.execution.map_meta_table.get(key)
    }

    pub fn get_prop_flow_slice(&self, map_meta: &MapMeta) -> Option<&[PropertyFlow]> {
        let range = map_meta.propflow_range.as_ref()?;
        Some(&self.data.execution.property_flows[range.start as usize..range.end as usize])
    }

    pub fn get_static_condition(&self, key: &MapKey) -> Option<&Condition> {
        self.data.execution.static_conditions.get(key)
    }

    pub fn get_mapper_proc(&self, key: &MapKey) -> Option<Procedure> {
        self.data.execution.map_meta_table.get(key).map(|map_info| {
            debug!(
                i = ?key.input.def_id,
                o = ?key.output.def_id,
                addr = ?map_info.procedure.address,
                "get_mapper_proc",
            );
            map_info.procedure
        })
    }

    pub fn new_serde_processor(
        &self,
        value_addr: SerdeOperatorAddr,
        mode: ProcessorMode,
    ) -> SerdeProcessor {
        SerdeProcessor::new(value_addr, mode, self)
    }

    pub fn dynamic_sequence_operator_addr(&self) -> SerdeOperatorAddr {
        self.data.serde.dynamic_sequence_operator_addr
    }

    pub fn get_extern(&self, def_id: DefId) -> Option<&Extern> {
        self.data.execution.extern_table.get(&def_id)
    }

    pub fn get_text_constant(&self, constant: TextConstant) -> &ArcStr {
        &self.data.defs.text_constants[constant.0 as usize]
    }

    /// Find a text constant given its string representation.
    /// NOTE: This intentionally has linear search complexity.
    /// It's only use case should be testing.
    pub fn find_text_constant(&self, str: &str) -> Option<TextConstant> {
        self.data
            .defs
            .text_constants
            .iter()
            .enumerate()
            .find(|(_, arcstr)| arcstr.as_str() == str)
            .map(|(index, _)| TextConstant(index as u32))
    }

    /// This primarily exists for testing only.
    pub fn find_named_downmap_meta(&self, index: DomainIndex, name: &str) -> Option<MapKey> {
        let text_constant = self.find_text_constant(name)?;
        self.data
            .execution
            .named_downmaps
            .get(&(index, text_constant))
            .cloned()
    }

    /// Create a possibly stripped-down version of the Ontology.
    ///
    /// This is useful for serialization over the wire to subsystems
    /// that only needs certain aspects of the ontology, e.g. data stores,
    /// modules that only needs to read domain definitions, etc.
    pub fn aspect_subset(&self, aspects: OntologyAspects) -> Self {
        let mut data = self.data.clone();
        if !aspects.contains(OntologyAspects::DEFS) {
            data.defs = Arc::new(DefsAspect::empty());
        }
        if !aspects.contains(OntologyAspects::SERDE) {
            data.serde = Arc::new(SerdeAspect::empty());
        }
        if !aspects.contains(OntologyAspects::DOCUMENTATION) {
            data.documentation = Arc::new(DocumentationAspect::empty());
        }
        if !aspects.contains(OntologyAspects::INTERFACE) {
            data.interface = Arc::new(InterfaceAspect::empty());
        }
        if !aspects.contains(OntologyAspects::EXECTUTION) {
            data.execution = Arc::new(ExecutionAspect::empty());
        }
        if !aspects.contains(OntologyAspects::CONFIG) {
            data.config = Arc::new(ConfigAspect::empty());
        }

        Self { data }
    }
}

impl From<Data> for Ontology {
    fn from(data: Data) -> Self {
        let mut ontology = Ontology { data };

        let mut serde_operators =
            std::mem::take(&mut Arc::get_mut(&mut ontology.data.serde).unwrap().operators);
        let mut interfaces = std::mem::take(
            &mut Arc::get_mut(&mut ontology.data.interface)
                .unwrap()
                .interfaces,
        );

        for operator in serde_operators.iter_mut() {
            operator.ontology_init(&ontology);
        }

        for (_, interfaces) in interfaces.iter_mut() {
            for interface in interfaces {
                interface.ontology_init(&ontology);
            }
        }

        Arc::get_mut(&mut ontology.data.serde).unwrap().operators = serde_operators;
        Arc::get_mut(&mut ontology.data.interface)
            .unwrap()
            .interfaces = interfaces;

        ontology
    }
}

impl AsRef<DefsAspect> for Ontology {
    fn as_ref(&self) -> &DefsAspect {
        &self.data.defs
    }
}

impl AsRef<SerdeAspect> for Ontology {
    fn as_ref(&self) -> &SerdeAspect {
        &self.data.serde
    }
}

impl AsRef<ExecutionAspect> for Ontology {
    fn as_ref(&self) -> &ExecutionAspect {
        &self.data.execution
    }
}

/// Things that must be done after deserialization before the ontology can be used
pub trait OntologyInit {
    fn ontology_init(&mut self, ontology: &Ontology);
}

impl Index<TextConstant> for Ontology {
    type Output = str;

    fn index(&self, index: TextConstant) -> &Self::Output {
        &self.data.defs.text_constants[index.0 as usize]
    }
}

impl Index<SerdeOperatorAddr> for Ontology {
    type Output = SerdeOperator;

    fn index(&self, index: SerdeOperatorAddr) -> &Self::Output {
        &self.data.serde.operators[index.0 as usize]
    }
}

impl OntolFormatter for Ontology {
    fn fmt_text_constant(
        &self,
        constant: u32,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{str:?}", str = &self[TextConstant(constant)])
    }
}
