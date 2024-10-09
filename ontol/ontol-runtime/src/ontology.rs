//! The ONTOL ontology.

use std::ops::Index;

use arcstr::ArcStr;
use data::Data;
use domain::{DefRepr, EdgeInfo};
use tracing::debug;
use ulid::Ulid;

use crate::{
    attr::AttrRef,
    debug::{OntolDebug, OntolFormatter},
    interface::{
        serde::{
            operator::{SerdeOperator, SerdeOperatorAddr},
            processor::{ProcessorLevel, ProcessorMode, SerdeProcessor, DOMAIN_PROFILE},
        },
        DomainInterface,
    },
    query::condition::Condition,
    value::Value,
    vm::{ontol_vm::OntolVm, proc::Procedure},
    DefId, DomainIndex, MapKey, PropId,
};

use self::{
    builder::OntologyBuilder,
    config::DomainConfig,
    domain::{Def, Domain, ExtendedEntityInfo},
    map::{Extern, MapMeta, PropertyFlow},
    ontol::{text_pattern::TextPattern, OntolDomainMeta, TextConstant, TextLikeType},
};

pub mod aspects;
pub mod builder;
pub mod config;
pub mod domain;
pub mod map;
pub mod ontol;

mod data;

/// The Ontology is the model of a single ONTOL runtime environment.
pub struct Ontology {
    pub(crate) data: Data,
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

    /// Access the ontology's [Data].
    /// The data is a serializable value.
    pub fn data(&self) -> &Data {
        &self.data
    }

    pub fn new_vm(&self, proc: Procedure) -> OntolVm<'_> {
        OntolVm::new(self, proc)
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

    pub fn get_def_docs(&self, def_id: DefId) -> Option<&ArcStr> {
        self.data.documentation.def_docs.get(&def_id)
    }

    pub fn get_prop_docs(&self, prop_id: PropId) -> Option<&ArcStr> {
        self.data.documentation.prop_docs.get(&prop_id)
    }

    pub fn get_text_pattern(&self, def_id: DefId) -> Option<&TextPattern> {
        self.data.domain.text_patterns.get(&def_id)
    }

    pub fn get_text_like_type(&self, def_id: DefId) -> Option<TextLikeType> {
        self.data.domain.text_like_types.get(&def_id).cloned()
    }

    pub fn domains(&self) -> impl Iterator<Item = (DomainIndex, &Domain)> {
        self.data
            .domain
            .domains
            .iter()
            .map(|(idx, domain)| (DomainIndex(idx as u16), domain))
    }

    pub fn ontol_domain_meta(&self) -> &OntolDomainMeta {
        &self.data.domain.ontol_domain_meta
    }

    pub fn domain_by_index(&self, index: DomainIndex) -> Option<&Domain> {
        self.data.domain.domains.get(&index)
    }

    pub fn domain_by_id(&self, domain_id: Ulid) -> Option<&Domain> {
        self.data
            .domain
            .domains
            .iter()
            .find(|(_, domain)| domain.domain_id().ulid == domain_id)
            .map(|(_, domain)| domain)
    }

    /// Get the members of a given union.
    /// Returns an empty slice if it's not a union.
    pub fn union_variants(&self, union_def_id: DefId) -> &[DefId] {
        self.data
            .domain
            .union_variants
            .get(&union_def_id)
            .map(|set| set.as_slice())
            .unwrap_or(&[])
    }

    pub fn find_edge(&self, id: DefId) -> Option<&EdgeInfo> {
        let domain = self.domain_by_index(id.0)?;
        domain.find_edge(id)
    }

    pub fn extended_entity_info(&self, def_id: DefId) -> Option<&ExtendedEntityInfo> {
        self.data.domain.extended_entity_table.get(&def_id)
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
        self.data.mapping.const_proc_table.get(&const_id).cloned()
    }

    pub fn iter_map_meta(&self) -> impl Iterator<Item = (MapKey, &MapMeta)> + '_ {
        self.data
            .mapping
            .map_meta_table
            .iter()
            .map(|(key, proc)| (*key, proc))
    }

    pub fn iter_named_downmaps(
        &self,
    ) -> impl Iterator<Item = (DomainIndex, TextConstant, MapKey)> + '_ {
        self.data
            .mapping
            .named_downmaps
            .iter()
            .map(|((index, text_constant), value)| (*index, *text_constant, *value))
    }

    pub fn get_map_meta(&self, key: &MapKey) -> Option<&MapMeta> {
        self.data.mapping.map_meta_table.get(key)
    }

    pub fn get_prop_flow_slice(&self, map_meta: &MapMeta) -> Option<&[PropertyFlow]> {
        let range = map_meta.propflow_range.as_ref()?;
        Some(&self.data.mapping.property_flows[range.start as usize..range.end as usize])
    }

    pub fn get_static_condition(&self, key: &MapKey) -> Option<&Condition> {
        self.data.mapping.static_conditions.get(key)
    }

    pub fn get_mapper_proc(&self, key: &MapKey) -> Option<Procedure> {
        self.data.mapping.map_meta_table.get(key).map(|map_info| {
            debug!(
                "get_mapper_proc ({:?}) => {:?}",
                key.def_ids(),
                map_info.procedure.debug(self)
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
            value_operator: &self.data.serde.operators[value_addr.0 as usize],
            ctx: Default::default(),
            level: ProcessorLevel::new_root(),
            ontology: self,
            profile: &DOMAIN_PROFILE,
            mode,
        }
    }

    pub fn dynamic_sequence_operator_addr(&self) -> SerdeOperatorAddr {
        self.data.serde.dynamic_sequence_operator_addr
    }

    pub fn get_extern(&self, def_id: DefId) -> Option<&Extern> {
        self.data.mapping.extern_table.get(&def_id)
    }

    pub fn get_text_constant(&self, constant: TextConstant) -> &ArcStr {
        &self.data.domain.text_constants[constant.0 as usize]
    }

    /// Find a text constant given its string representation.
    /// NOTE: This intentionally has linear search complexity.
    /// It's only use case should be testing.
    pub fn find_text_constant(&self, str: &str) -> Option<TextConstant> {
        self.data
            .domain
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
            .mapping
            .named_downmaps
            .get(&(index, text_constant))
            .cloned()
    }

    /// best-effort formatting of a value
    pub fn format_value(&self, value: &Value) -> String {
        let def = self.def(value.type_def_id());
        if let Some(operator_addr) = def.operator_addr {
            // TODO: Easier way to report values in "human readable"/JSON format

            let processor = self.new_serde_processor(operator_addr, ProcessorMode::Read);

            let mut buf: Vec<u8> = vec![];
            processor
                .serialize_attr(
                    AttrRef::Unit(value),
                    &mut serde_json::Serializer::new(&mut buf),
                )
                .unwrap();
            String::from(std::str::from_utf8(&buf).unwrap())
        } else {
            "N/A".to_string()
        }
    }

    pub fn try_produce_constant(&self, def_id: DefId) -> Option<Value> {
        let def = self.def(def_id);
        match def.repr() {
            Some(DefRepr::TextConstant(constant)) => {
                Some(Value::Text(self[*constant].into(), def_id.into()))
            }
            _ => {
                let Some(operator_addr) = def.operator_addr else {
                    debug!("{def_id:?} has no operator addr, no constant can be produced");
                    return None;
                };
                self.try_produce_constant_from_operator(operator_addr)
            }
        }
    }

    fn try_produce_constant_from_operator(&self, addr: SerdeOperatorAddr) -> Option<Value> {
        let operator = &self.data.serde.operators[addr.0 as usize];
        match operator {
            SerdeOperator::AnyPlaceholder => None,
            SerdeOperator::Unit => Some(Value::unit()),
            SerdeOperator::True(_) => Some(Value::boolean(true)),
            SerdeOperator::False(_) => Some(Value::boolean(false)),
            SerdeOperator::Boolean(_)
            | SerdeOperator::I32(..)
            | SerdeOperator::I64(..)
            | SerdeOperator::F64(..)
            | SerdeOperator::Serial(_)
            | SerdeOperator::Octets(_)
            | SerdeOperator::String(_) => None,
            SerdeOperator::StringConstant(text_constant, def_id) => {
                Some(Value::Text(self[*text_constant].into(), (*def_id).into()))
            }
            SerdeOperator::TextPattern(_)
            | SerdeOperator::CapturingTextPattern(_)
            | SerdeOperator::DynamicSequence
            | SerdeOperator::RelationList(_)
            | SerdeOperator::RelationIndexSet(_)
            | SerdeOperator::ConstructorSequence(_) => None,
            SerdeOperator::Alias(alias_op) => {
                self.try_produce_constant_from_operator(alias_op.inner_addr)
            }
            SerdeOperator::Union(_) | SerdeOperator::Struct(_) => None,
            SerdeOperator::IdSingletonStruct(_, _, _) => None,
        }
    }
}

impl From<Data> for Ontology {
    fn from(value: Data) -> Self {
        let mut ontology = Ontology { data: value };

        let mut serde_operators = std::mem::take(&mut ontology.data.serde.operators);
        let mut interfaces = std::mem::take(&mut ontology.data.interface.interfaces);

        for operator in serde_operators.iter_mut() {
            operator.ontology_init(&ontology);
        }

        for (_, interfaces) in interfaces.iter_mut() {
            for interface in interfaces {
                interface.ontology_init(&ontology);
            }
        }

        ontology.data.serde.operators = serde_operators;
        ontology.data.interface.interfaces = interfaces;

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
        &self.data.domain.text_constants[index.0 as usize]
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
        constant: TextConstant,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{str:?}", str = &self[constant])
    }
}
