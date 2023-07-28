use std::{collections::HashMap, ops::Range};

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
    },
    DefId, MapKey, PackageId, RelationshipId,
};

/// Ontology is the ONTOL runtime environment
pub struct Ontology {
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
    value_generators: FnvHashMap<RelationshipId, ValueGenerator>,
    property_flows: Vec<PropertyFlow>,
}

impl Ontology {
    pub fn builder() -> OntologyBuilder {
        OntologyBuilder {
            ontology: Self {
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
                value_generators: Default::default(),
                property_flows: Default::default(),
            },
        }
    }

    pub fn new_vm(&self) -> OntolVm<'_> {
        OntolVm::new(&self.lib)
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

    pub fn iter_map_meta(&self) -> impl Iterator<Item = ((MapKey, MapKey), &MapMeta)> + '_ {
        self.map_meta_table.iter().map(|(key, proc)| (*key, proc))
    }

    pub fn get_map_meta(&self, from: MapKey, to: MapKey) -> Option<&MapMeta> {
        self.map_meta_table.get(&(from, to))
    }

    pub fn get_prop_flow_slice(&self, map_meta: &MapMeta) -> &[PropertyFlow] {
        let range = &map_meta.propflow_range;
        &self.property_flows[range.start as usize..range.end as usize]
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
            ontology: self,
            mode,
        }
    }

    pub fn get_serde_operator(&self, operator_id: SerdeOperatorId) -> &SerdeOperator {
        &self.serde_operators[operator_id.0 as usize]
    }

    pub fn dynamic_sequence_operator_id(&self) -> SerdeOperatorId {
        self.dynamic_sequence_operator_id
    }

    pub fn get_value_generator(&self, relationship_id: RelationshipId) -> Option<&ValueGenerator> {
        self.value_generators.get(&relationship_id)
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
    /// Whether all inherent fields are part of the primary id of this entity.
    /// In other words: The entity has only one field, its ID.
    pub is_self_identifying: bool,
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
    pub propflow_range: Range<u32>,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct PropertyFlow {
    pub id: PropertyId,
    pub data: PropertyFlowData,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum PropertyFlowData {
    Type(DefId),
    Cardinality(Cardinality),
    ChildOf(PropertyId),
    DependentOn(PropertyId),
}

pub struct OntologyBuilder {
    ontology: Ontology,
}

impl OntologyBuilder {
    pub fn add_domain(&mut self, package_id: PackageId, domain: Domain) {
        self.ontology.domain_table.insert(package_id, domain);
    }

    pub fn add_package_config(&mut self, package_id: PackageId, config: PackageConfig) {
        self.ontology
            .package_config_table
            .insert(package_id, config);
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

    pub fn map_meta_table(mut self, map_meta_table: FnvHashMap<(MapKey, MapKey), MapMeta>) -> Self {
        self.ontology.map_meta_table = map_meta_table;
        self
    }

    pub fn serde_operators(
        mut self,
        operators: Vec<SerdeOperator>,
        per_def: HashMap<SerdeKey, SerdeOperatorId>,
    ) -> Self {
        self.ontology.serde_operators = operators;
        self.ontology.serde_operators_per_def = per_def;
        self
    }

    pub fn dynamic_sequence_operator_id(
        mut self,
        dynamic_sequence_operator_id: SerdeOperatorId,
    ) -> Self {
        self.ontology.dynamic_sequence_operator_id = dynamic_sequence_operator_id;
        self
    }

    pub fn property_flows(mut self, flows: Vec<PropertyFlow>) -> Self {
        self.ontology.property_flows = flows;
        self
    }

    pub fn string_like_types(mut self, types: FnvHashMap<DefId, StringLikeType>) -> Self {
        self.ontology.string_like_types = types;
        self
    }

    pub fn string_patterns(mut self, patterns: FnvHashMap<DefId, StringPattern>) -> Self {
        self.ontology.string_patterns = patterns;
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
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

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum ValueCardinality {
    One,
    Many,
    // ManyInRange(Option<u16>, Option<u16>),
}
