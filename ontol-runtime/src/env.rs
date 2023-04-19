use std::collections::HashMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    mapping_vm::MappingVm,
    proc::{Lib, Procedure},
    property_probe::PropertyProbe,
    serde::{
        operator::{SerdeOperator, SerdeOperatorId},
        processor::{ProcessorLevel, ProcessorMode, SerdeProcessor},
        SerdeKey,
    },
    string_pattern::StringPattern,
    string_types::StringLikeType,
    DefId, PackageId, RelationId,
};

/// Runtime environment
pub struct Env {
    pub(crate) mapper_proc_table: FnvHashMap<(DefId, DefId), Procedure>,
    pub(crate) string_like_types: FnvHashMap<DefId, StringLikeType>,
    pub(crate) string_patterns: FnvHashMap<DefId, StringPattern>,

    domains: FnvHashMap<PackageId, Domain>,
    lib: Lib,
    serde_operators_per_def: HashMap<SerdeKey, SerdeOperatorId>,
    serde_operators: Vec<SerdeOperator>,
}

impl Env {
    pub fn builder() -> EnvBuilder {
        EnvBuilder {
            env: Self {
                mapper_proc_table: Default::default(),
                string_like_types: Default::default(),
                string_patterns: Default::default(),
                domains: Default::default(),
                lib: Lib::default(),
                serde_operators_per_def: Default::default(),
                serde_operators: Default::default(),
            },
        }
    }

    pub fn new_mapper(&self) -> MappingVm<'_> {
        MappingVm::new(&self.lib)
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

    pub fn get_string_pattern(&self, def_id: DefId) -> Option<&StringPattern> {
        self.string_patterns.get(&def_id)
    }

    pub fn domains(&self) -> impl Iterator<Item = (&PackageId, &Domain)> {
        self.domains.iter()
    }

    pub fn find_domain(&self, package_id: PackageId) -> Option<&Domain> {
        self.domains.get(&package_id)
    }

    pub fn mapper_procs(&self) -> impl Iterator<Item = ((DefId, DefId), Procedure)> + '_ {
        self.mapper_proc_table
            .iter()
            .map(|(key, value)| (*key, value.clone()))
    }

    pub fn get_mapper_proc(&self, from: DefId, to: DefId) -> Option<Procedure> {
        self.mapper_proc_table.get(&(from, to)).cloned()
    }

    pub fn new_serde_processor(
        &self,
        value_operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) -> SerdeProcessor {
        SerdeProcessor {
            value_operator: &self.serde_operators[value_operator_id.0 as usize],
            rel_params_operator_id,
            level,
            env: self,
            mode,
        }
    }

    pub fn get_serde_operator(&self, operator_id: SerdeOperatorId) -> &SerdeOperator {
        &self.serde_operators[operator_id.0 as usize]
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
        while self.info.len() < index + 1 {
            self.info.push(TypeInfo {
                def_id: DefId(type_info.def_id.0, self.info.len() as u16),
                public: false,
                name: None,
                entity_info: None,
                operator_id: None,
            });
        }

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
    pub id_relation_id: RelationId,
    pub id_value_def_id: DefId,
    pub id_operator_id: SerdeOperatorId,
    pub id_inherent_property_name: Option<String>,
}

pub struct EnvBuilder {
    env: Env,
}

impl EnvBuilder {
    pub fn add_domain(&mut self, package_id: PackageId, domain: Domain) {
        self.env.domains.insert(package_id, domain);
    }

    pub fn lib(mut self, lib: Lib) -> Self {
        self.env.lib = lib;
        self
    }

    pub fn mapping_procs(mut self, mapping_procs: FnvHashMap<(DefId, DefId), Procedure>) -> Self {
        self.env.mapper_proc_table = mapping_procs;
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
