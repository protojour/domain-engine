use std::collections::HashMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    config::PackageConfig,
    serde::{
        operator::{SerdeOperator, SerdeOperatorId},
        processor::{ProcessorLevel, ProcessorMode, SerdeProcessor},
        SerdeKey,
    },
    string_pattern::StringPattern,
    string_types::StringLikeType,
    value::PropertyId,
    value_generator::ValueGenerator,
    vm::{
        ontol_vm::OntolVm,
        proc::{Lib, Procedure},
        property_probe::PropertyProbe,
    },
    DefId, MapKey, PackageId, RelationshipId,
};

/// Runtime environment
pub struct Env {
    pub(crate) const_proc_table: FnvHashMap<DefId, Procedure>,
    pub(crate) map_meta_table: FnvHashMap<(MapKey, MapKey), MapMeta>,
    pub(crate) string_like_types: FnvHashMap<DefId, StringLikeType>,
    pub(crate) string_patterns: FnvHashMap<DefId, StringPattern>,

    domain_table: FnvHashMap<PackageId, Domain>,
    package_config_table: FnvHashMap<PackageId, PackageConfig>,
    docs: FnvHashMap<DefId, Vec<String>>,
    lib: Lib,
    serde_operators_per_def: HashMap<SerdeKey, SerdeOperatorId>,
    serde_operators: Vec<SerdeOperator>,
    dynamic_sequence_operator_id: SerdeOperatorId,
}

impl Env {
    pub fn builder() -> EnvBuilder {
        EnvBuilder {
            env: Self {
                const_proc_table: Default::default(),
                map_meta_table: Default::default(),
                string_like_types: Default::default(),
                string_patterns: Default::default(),
                domain_table: Default::default(),
                package_config_table: Default::default(),
                docs: Default::default(),
                lib: Lib::default(),
                serde_operators_per_def: Default::default(),
                serde_operators: Default::default(),
                dynamic_sequence_operator_id: SerdeOperatorId(u32::MAX),
            },
        }
    }

    pub fn new_vm(&self) -> OntolVm<'_> {
        OntolVm::new(&self.lib)
    }

    pub fn new_property_probe(&self) -> PropertyProbe<'_> {
        PropertyProbe::new(&self.lib)
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

    pub fn get_string_pattern(&self, def_id: DefId) -> Option<&StringPattern> {
        self.string_patterns.get(&def_id)
    }

    pub fn get_string_like_type(&self, def_id: DefId) -> Option<StringLikeType> {
        self.string_like_types.get(&def_id).cloned()
    }

    pub fn domains(&self) -> impl Iterator<Item = (&PackageId, &Domain)> {
        self.domain_table.iter()
    }

    pub fn find_domain(&self, package_id: PackageId) -> Option<&Domain> {
        self.domain_table.get(&package_id)
    }

    pub fn get_package_config(&self, package_id: PackageId) -> Option<&PackageConfig> {
        self.package_config_table.get(&package_id)
    }

    pub fn get_const_proc(&self, const_id: DefId) -> Option<Procedure> {
        self.const_proc_table.get(&const_id).cloned()
    }

    pub fn iter_map_info(&self) -> impl Iterator<Item = ((MapKey, MapKey), &MapMeta)> + '_ {
        self.map_meta_table.iter().map(|(key, proc)| (*key, proc))
    }

    pub fn get_mapper_proc(&self, from: MapKey, to: MapKey) -> Option<Procedure> {
        self.map_meta_table
            .get(&(from, to))
            .map(|map_info| map_info.procedure)
    }

    pub fn new_serde_processor(
        &self,
        value_operator_id: SerdeOperatorId,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> SerdeProcessor {
        SerdeProcessor {
            value_operator: &self.serde_operators[value_operator_id.0 as usize],
            ctx: Default::default(),
            level,
            env: self,
            mode,
        }
    }

    pub fn get_serde_operator(&self, operator_id: SerdeOperatorId) -> &SerdeOperator {
        &self.serde_operators[operator_id.0 as usize]
    }

    pub fn dynamic_sequence_operator_id(&self) -> SerdeOperatorId {
        self.dynamic_sequence_operator_id
    }
}

#[derive(Default)]
pub struct Domain {
    /// Map that stores types in insertion/definition order
    pub type_names: IndexMap<String, DefId>,

    /// Types by DefId.1 (the type's index within the domain)
    info: Vec<TypeInfo>,
}

impl Domain {
    pub fn type_info(&self, def_id: DefId) -> &TypeInfo {
        &self.info[def_id.1 as usize]
    }

    pub fn type_infos(&self) -> impl Iterator<Item = &TypeInfo> {
        self.info.iter()
    }

    pub fn add_type(&mut self, type_info: TypeInfo) {
        let def_id = type_info.def_id;
        if let Some(type_name) = type_info.name.as_ref() {
            self.type_names.insert(type_name.clone(), def_id);
        }
        self.register_type_info(type_info);
    }

    fn register_type_info(&mut self, type_info: TypeInfo) {
        let index = type_info.def_id.1 as usize;

        // pad the vector
        let new_size = std::cmp::max(self.info.len(), index + 1);
        self.info.resize_with(new_size, || TypeInfo {
            def_id: DefId(type_info.def_id.0, 0),
            public: false,
            name: None,
            entity_info: None,
            operator_id: None,
        });

        self.info[index] = type_info;
    }
}

#[derive(Clone, Debug)]
pub struct TypeInfo {
    pub def_id: DefId,
    pub public: bool,
    pub name: Option<String>,
    /// Some if this type is an entity
    pub entity_info: Option<EntityInfo>,
    pub operator_id: Option<SerdeOperatorId>,
}

#[derive(Clone, Debug)]
pub struct EntityInfo {
    pub id_relationship_id: RelationshipId,
    pub id_value_def_id: DefId,
    pub id_operator_id: SerdeOperatorId,
    pub id_value_generator: Option<ValueGenerator>,
    pub entity_relationships: IndexMap<PropertyId, EntityRelationship>,
}

#[derive(Clone, Debug)]
pub struct EntityRelationship {
    pub cardinality: Cardinality,
    pub target: DefId,
}

#[derive(Clone, Debug)]
pub struct MapMeta {
    pub procedure: Procedure,
    pub data_flow: DataFlow,
}

#[derive(Clone, Default, Eq, PartialEq, Debug)]
pub struct DataFlow {
    pub properties: Vec<PropertyFlow>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct PropertyFlow {
    pub property: PropertyId,
    pub relationship: PropertyFlowRelationship,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum PropertyFlowRelationship {
    None,
    ChildOf(PropertyId),
    DependentOn(PropertyId),
}

pub struct EnvBuilder {
    env: Env,
}

impl EnvBuilder {
    pub fn add_domain(&mut self, package_id: PackageId, domain: Domain) {
        self.env.domain_table.insert(package_id, domain);
    }

    pub fn add_package_config(&mut self, package_id: PackageId, config: PackageConfig) {
        self.env.package_config_table.insert(package_id, config);
    }

    pub fn docs(mut self, docs: FnvHashMap<DefId, Vec<String>>) -> Self {
        self.env.docs = docs;
        self
    }

    pub fn lib(mut self, lib: Lib) -> Self {
        self.env.lib = lib;
        self
    }

    pub fn const_procs(mut self, const_procs: FnvHashMap<DefId, Procedure>) -> Self {
        self.env.const_proc_table = const_procs;
        self
    }

    pub fn map_meta_table(mut self, map_meta_table: FnvHashMap<(MapKey, MapKey), MapMeta>) -> Self {
        self.env.map_meta_table = map_meta_table;
        self
    }

    pub fn serde_operators(
        mut self,
        operators: Vec<SerdeOperator>,
        per_def: HashMap<SerdeKey, SerdeOperatorId>,
    ) -> Self {
        self.env.serde_operators = operators;
        self.env.serde_operators_per_def = per_def;
        self
    }

    pub fn dynamic_sequence_operator_id(
        mut self,
        dynamic_sequence_operator_id: SerdeOperatorId,
    ) -> Self {
        self.env.dynamic_sequence_operator_id = dynamic_sequence_operator_id;
        self
    }

    pub fn string_like_types(mut self, types: FnvHashMap<DefId, StringLikeType>) -> Self {
        self.env.string_like_types = types;
        self
    }

    pub fn string_patterns(mut self, patterns: FnvHashMap<DefId, StringPattern>) -> Self {
        self.env.string_patterns = patterns;
        self
    }

    pub fn build(self) -> Env {
        self.env
    }
}

pub type Cardinality = (PropertyCardinality, ValueCardinality);

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
pub enum ValueCardinality {
    One,
    Many,
    // ManyInRange(Option<u16>, Option<u16>),
}
