use std::collections::HashMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    proc::{Lib, Procedure},
    serde::{SerdeKey, SerdeOperator, SerdeOperatorId, SerdeProcessor},
    string_pattern::StringPattern,
    string_types::StringLikeType,
    value::{Data, Value},
    DefId, PackageId,
};

/// Runtime environment
pub struct Env {
    pub domains: FnvHashMap<PackageId, Domain>,
    pub lib: Lib,
    pub translations: FnvHashMap<(DefId, DefId), Procedure>,
    pub serde_operators_per_def: HashMap<SerdeKey, SerdeOperatorId>,
    pub serde_operators: Vec<SerdeOperator>,
    pub string_like_types: FnvHashMap<DefId, StringLikeType>,
    pub string_patterns: FnvHashMap<DefId, StringPattern>,
}

impl Env {
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
    ) -> SerdeProcessor {
        SerdeProcessor {
            value_operator: &self.serde_operators[value_operator_id.0 as usize],
            rel_params_operator_id,
            env: self,
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

    pub fn add_type(&mut self, type_name: String, type_info: TypeInfo) {
        let def_id = type_info.def_id;
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
                serde_operator_id: None,
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

    pub serde_operator_id: Option<SerdeOperatorId>,
}
