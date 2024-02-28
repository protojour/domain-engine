use std::{
    fmt::Debug,
    ops::{Index, Range},
};

use ::serde::{Deserialize, Serialize};
use arcstr::ArcStr;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_macros::OntolDebug;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    config::PackageConfig,
    debug::{self, OntolFormatter},
    interface::{
        serde::{
            operator::{SerdeOperator, SerdeOperatorAddr},
            processor::{ProcessorLevel, ProcessorMode, SerdeProcessor, DOMAIN_PROFILE},
        },
        DomainInterface,
    },
    text::TextConstant,
    text_like_types::TextLikeType,
    text_pattern::TextPattern,
    value::PropertyId,
    value_generator::ValueGenerator,
    var::Var,
    vm::{
        ontol_vm::OntolVm,
        proc::{Lib, Procedure},
    },
    DefId, MapKey, PackageId, RelationshipId, Role,
};

/// Ontology is the ONTOL runtime environment
///
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
        OntologyBuilder {
            ontology: Self {
                text_constants: vec![],
                const_proc_table: Default::default(),
                map_meta_table: Default::default(),
                named_forward_maps: Default::default(),
                text_like_types: Default::default(),
                text_patterns: Default::default(),
                extern_table: Default::default(),
                domain_table: Default::default(),
                ontol_domain_meta: Default::default(),
                domain_interfaces: Default::default(),
                package_config_table: Default::default(),
                docs: Default::default(),
                lib: Lib::default(),
                serde_operators: Default::default(),
                dynamic_sequence_operator_addr: SerdeOperatorAddr(u32::MAX),
                value_generators: Default::default(),
                property_flows: Default::default(),
            },
        }
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
        constant: crate::text::TextConstant,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "{str:?}", str = &self[constant])
    }
}

#[derive(Serialize, Deserialize)]
pub struct OntolDomainMeta {
    pub bool: DefId,
    pub i64: DefId,
    pub f64: DefId,
    pub text: DefId,
    pub open_data_relationship: DefId,
}

impl OntolDomainMeta {
    pub fn open_data_property_id(&self) -> PropertyId {
        PropertyId {
            role: Role::Subject,
            relationship_id: RelationshipId(self.open_data_relationship),
        }
    }
}

impl Default for OntolDomainMeta {
    fn default() -> Self {
        Self {
            bool: DefId::unit(),
            i64: DefId::unit(),
            f64: DefId::unit(),
            text: DefId::unit(),
            open_data_relationship: DefId::unit(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Domain {
    unique_name: TextConstant,

    /// Types by DefId.1 (the type's index within the domain)
    info: Vec<TypeInfo>,
}

impl Domain {
    pub fn new(unique_name: TextConstant) -> Self {
        Self {
            unique_name,
            info: Default::default(),
        }
    }

    pub fn unique_name(&self) -> TextConstant {
        self.unique_name
    }

    pub fn type_count(&self) -> usize {
        self.info.len()
    }

    pub fn type_info(&self, def_id: DefId) -> &TypeInfo {
        &self.info[def_id.1 as usize]
    }

    pub fn type_infos(&self) -> impl Iterator<Item = &TypeInfo> {
        self.info.iter()
    }

    pub fn find_type_by_name(&self, name: TextConstant) -> Option<&TypeInfo> {
        self.info
            .iter()
            .find(|type_info| type_info.name() == Some(name))
    }

    pub fn add_type(&mut self, type_info: TypeInfo) {
        self.register_type_info(type_info);
    }

    fn register_type_info(&mut self, type_info: TypeInfo) {
        let index = type_info.def_id.1 as usize;

        // pad the vector
        let new_size = std::cmp::max(self.info.len(), index + 1);
        self.info.resize_with(new_size, || TypeInfo {
            def_id: DefId(type_info.def_id.0, 0),
            public: false,
            kind: TypeKind::Data(BasicTypeInfo { name: None }),
            operator_addr: None,
            data_relationships: Default::default(),
        });

        self.info[index] = type_info;
    }

    pub fn find_type_info_by_name(&self, name: TextConstant) -> Option<&TypeInfo> {
        self.info
            .iter()
            .find(|type_info| type_info.name() == Some(name))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TypeInfo {
    pub def_id: DefId,
    pub public: bool,
    pub kind: TypeKind,
    /// The SerdeOperatorAddr used for JSON.
    /// FIXME: This should really be connected to a DomainInterface.
    pub operator_addr: Option<SerdeOperatorAddr>,

    pub data_relationships: IndexMap<PropertyId, DataRelationshipInfo>,
}

impl TypeInfo {
    pub fn name(&self) -> Option<TextConstant> {
        match &self.kind {
            TypeKind::Entity(info) => Some(info.name),
            TypeKind::Data(info)
            | TypeKind::Relationship(info)
            | TypeKind::Function(info)
            | TypeKind::Domain(info)
            | TypeKind::Generator(info) => info.name,
        }
    }

    pub fn entity_info(&self) -> Option<&EntityInfo> {
        match &self.kind {
            TypeKind::Entity(entity_info) => Some(entity_info),
            _ => None,
        }
    }

    pub fn entity_relationships(
        &self,
    ) -> impl Iterator<Item = (&PropertyId, &DataRelationshipInfo)> {
        self.data_relationships
            .iter()
            .filter(|(_, info)| matches!(info.kind, DataRelationshipKind::EntityGraph { .. }))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum TypeKind {
    Entity(EntityInfo),
    Data(BasicTypeInfo),
    Relationship(BasicTypeInfo),
    Function(BasicTypeInfo),
    Domain(BasicTypeInfo),
    Generator(BasicTypeInfo),
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct BasicTypeInfo {
    pub name: Option<TextConstant>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EntityInfo {
    pub name: TextConstant,
    pub id_relationship_id: RelationshipId,
    pub id_value_def_id: DefId,
    pub id_operator_addr: SerdeOperatorAddr,
    /// Whether all inherent fields are part of the primary id of this entity.
    /// In other words: The entity has only one field, its ID.
    pub is_self_identifying: bool,
    pub id_value_generator: Option<ValueGenerator>,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct DataRelationshipInfo {
    pub kind: DataRelationshipKind,
    pub subject_cardinality: Cardinality,
    pub object_cardinality: Cardinality,
    pub subject_name: TextConstant,
    pub object_name: Option<TextConstant>,
    pub source: DataRelationshipSource,
    pub target: DataRelationshipTarget,
}

impl DataRelationshipInfo {
    pub fn cardinality_by_role(&self, role: Role) -> Cardinality {
        match role {
            Role::Subject => self.subject_cardinality,
            Role::Object => self.object_cardinality,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipKind {
    /// The relationship is between an entity and its identifier
    Id,
    /// A Tree data relationship that can never be circular.
    /// It expresses a simple composition of a composite (the parent) and the component (the child).
    Tree,
    /// Graph data relationships can be circular and involves entities.
    /// The Graph relationship kind must go from one entity to another entity.
    EntityGraph {
        /// EntityGraph data relationships are allowed to be parametric.
        /// i.e. the relation itself has parameters.
        rel_params: Option<DefId>,
    },
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipSource {
    Inherent,
    ByUnionProxy,
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub enum DataRelationshipTarget {
    Unambiguous(DefId),
    Union {
        union_def_id: DefId,
        // TODO: Move to one place in the ontology.
        // It's a lookup from the union DefId to its members.
        variants: Box<[DefId]>,
    },
}

#[derive(Clone, Serialize, Deserialize, OntolDebug)]
pub struct MapMeta {
    pub procedure: Procedure,
    pub propflow_range: Option<Range<u32>>,
    pub lossiness: MapLossiness,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, OntolDebug)]
pub enum MapLossiness {
    Complete,
    Lossy,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug)]
pub struct PropertyFlow {
    pub id: PropertyId,
    pub data: PropertyFlowData,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, OntolDebug, Debug)]
pub enum PropertyFlowData {
    Type(DefId),
    Match(Var),
    Cardinality(Cardinality),
    ChildOf(PropertyId),
    DependentOn(PropertyId),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Extern {
    HttpJson { url: TextConstant },
}

pub struct OntologyBuilder {
    ontology: Ontology,
}

impl OntologyBuilder {
    /// Access to the partial ontology being built
    pub fn partial_ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn add_domain(&mut self, package_id: PackageId, domain: Domain) {
        self.ontology.domain_table.insert(package_id, domain);
    }

    pub fn add_package_config(&mut self, package_id: PackageId, config: PackageConfig) {
        self.ontology
            .package_config_table
            .insert(package_id, config);
    }

    pub fn text_constants(mut self, text_constants: Vec<ArcStr>) -> Self {
        self.ontology.text_constants = text_constants;
        self
    }

    pub fn ontol_domain_meta(mut self, meta: OntolDomainMeta) -> Self {
        self.ontology.ontol_domain_meta = meta;
        self
    }

    pub fn domain_interfaces(
        mut self,
        interfaces: FnvHashMap<PackageId, Vec<DomainInterface>>,
    ) -> Self {
        self.ontology.domain_interfaces = interfaces;
        self
    }

    pub fn docs(mut self, docs: FnvHashMap<DefId, Vec<String>>) -> Self {
        self.ontology.docs = docs;
        self
    }

    pub fn lib(mut self, lib: Lib) -> Self {
        self.ontology.lib = lib;
        self
    }

    pub fn const_procs(mut self, const_procs: FnvHashMap<DefId, Procedure>) -> Self {
        self.ontology.const_proc_table = const_procs;
        self
    }

    pub fn map_meta_table(mut self, map_meta_table: FnvHashMap<MapKey, MapMeta>) -> Self {
        self.ontology.map_meta_table = map_meta_table;
        self
    }

    pub fn named_forward_maps(
        mut self,
        named_forward_maps: FnvHashMap<(PackageId, TextConstant), MapKey>,
    ) -> Self {
        self.ontology.named_forward_maps = named_forward_maps;
        self
    }

    pub fn serde_operators(mut self, operators: Vec<SerdeOperator>) -> Self {
        self.ontology.serde_operators = operators;
        self
    }

    pub fn dynamic_sequence_operator_addr(mut self, addr: SerdeOperatorAddr) -> Self {
        self.ontology.dynamic_sequence_operator_addr = addr;
        self
    }

    pub fn property_flows(mut self, flows: Vec<PropertyFlow>) -> Self {
        self.ontology.property_flows = flows;
        self
    }

    pub fn string_like_types(mut self, types: FnvHashMap<DefId, TextLikeType>) -> Self {
        self.ontology.text_like_types = types;
        self
    }

    pub fn text_patterns(mut self, patterns: FnvHashMap<DefId, TextPattern>) -> Self {
        self.ontology.text_patterns = patterns;
        self
    }

    pub fn externs(mut self, externs: FnvHashMap<DefId, Extern>) -> Self {
        self.ontology.extern_table = externs;
        self
    }

    pub fn value_generators(
        mut self,
        generators: FnvHashMap<RelationshipId, ValueGenerator>,
    ) -> Self {
        self.ontology.value_generators = generators;
        self
    }

    pub fn build(self) -> Ontology {
        self.ontology
    }
}

pub type Cardinality = (PropertyCardinality, ValueCardinality);

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug, OntolDebug,
)]
pub enum PropertyCardinality {
    Optional,
    Mandatory,
}

impl PropertyCardinality {
    pub fn is_mandatory(&self) -> bool {
        matches!(self, Self::Mandatory)
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional)
    }
}

#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Debug, OntolDebug,
)]
pub enum ValueCardinality {
    One,
    Many,
}
