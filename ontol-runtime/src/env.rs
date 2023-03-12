use std::collections::HashMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    proc::{Lib, Procedure},
    property_probe::PropertyProbe,
    serde::{
        operator::{SerdeOperator, SerdeOperatorId},
        processor::{ProcessorLevel, ProcessorMode, SerdeProcessor},
        SerdeKey,
    },
    string_pattern::StringPattern,
    string_types::StringLikeType,
    translate::Translator,
    value::{Data, Value},
    DefId, PackageId,
};

/// Runtime environment
pub struct Env {
    pub(crate) translations: FnvHashMap<(DefId, DefId), Procedure>,
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
                translations: Default::default(),
                string_like_types: Default::default(),
                string_patterns: Default::default(),
                domains: Default::default(),
                lib: Lib::default(),
                serde_operators_per_def: Default::default(),
                serde_operators: Default::default(),
            },
        }
    }

    pub fn new_translator(&self) -> Translator<'_> {
        Translator::new(&self.lib)
    }

    pub fn new_property_probe(&self) -> PropertyProbe<'_> {
        PropertyProbe::new(&self.lib)
    }

    pub fn unit_value(&self) -> Value {
        Value {
            type_def_id: DefId::unit(),
            data: Data::Unit,
        }
    }

    pub fn get_type_info(&self, def_id: DefId) -> &TypeInfo {
        match self.find_domain(&def_id.0) {
            Some(domain) => domain.type_info(def_id),
            None => {
                panic!("No domain for {:?}", def_id.0)
            }
        }
    }

    pub fn domain_count(&self) -> usize {
        self.domains.len()
    }

    pub fn find_domain(&self, package_id: &PackageId) -> Option<&Domain> {
        self.domains.get(package_id)
    }

    pub fn get_translator(&self, from: DefId, to: DefId) -> Option<Procedure> {
        self.translations.get(&(from, to)).cloned()
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

    pub fn add_type(&mut self, type_info: TypeInfo) {
        let def_id = type_info.def_id;
        let type_name = type_info.name.clone();
        self.register_type_info(type_info);
        self.type_names.insert(type_name, def_id);
    }

    fn register_type_info(&mut self, type_info: TypeInfo) {
        let index = type_info.def_id.1 as usize;

        // pad the vector
        while self.info.len() < index + 1 {
            self.info.push(TypeInfo {
                def_id: DefId(type_info.def_id.0, self.info.len() as u16),
                name: "".into(),
                entity_id: None,
                generic_operator_id: None,
                create_operator_id: None,
                selection_operator_id: None,
            });
        }

        self.info[index] = type_info;
    }
}

#[derive(Clone, Debug)]
pub struct TypeInfo {
    pub def_id: DefId,

    pub name: String,

    /// If this type is an entity, this is the type of its ID
    pub entity_id: Option<DefId>,

    pub generic_operator_id: Option<SerdeOperatorId>,

    pub create_operator_id: Option<SerdeOperatorId>,

    pub selection_operator_id: Option<SerdeOperatorId>,
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

    pub fn translations(mut self, translations: FnvHashMap<(DefId, DefId), Procedure>) -> Self {
        self.env.translations = translations;
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
