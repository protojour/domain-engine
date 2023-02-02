use std::collections::HashMap;

use smartstring::alias::String;

use crate::{
    proc::{Lib, Procedure},
    serde::{SerdeOperator, SerdeOperatorId, SerdeProcessor, SerdeRegistry},
    DefId, PackageId,
};

/// Runtime environment
pub struct Env {
    pub domains: HashMap<PackageId, Domain>,
    pub lib: Lib,
    pub translations: HashMap<(DefId, DefId), Procedure>,
    pub serde_operators_per_def: HashMap<DefId, SerdeOperatorId>,
    pub serde_operators: Vec<SerdeOperator>,
}

impl Env {
    pub fn get_domain(&self, package_id: &PackageId) -> Option<&Domain> {
        self.domains.get(&package_id)
    }

    pub fn get_translator(&self, from: DefId, to: DefId) -> Option<Procedure> {
        self.translations.get(&(from, to)).cloned()
    }

    pub fn new_serde_processor(&self, serde_operator_id: SerdeOperatorId) -> SerdeProcessor {
        SerdeRegistry::new(&self.serde_operators).make_processor(serde_operator_id)
    }
}

pub struct Domain {
    pub types: HashMap<String, TypeInfo>,
}

pub struct TypeInfo {
    pub def_id: DefId,
    pub serde_operator_id: Option<SerdeOperatorId>,
}

impl Domain {
    pub fn get_def_id(&self, type_name: &str) -> Option<DefId> {
        let type_info = self.types.get(type_name)?;
        Some(type_info.def_id)
    }

    pub fn get_serde_operator_id(&self, type_name: &str) -> Option<SerdeOperatorId> {
        let type_info = self.types.get(type_name)?;
        type_info.serde_operator_id
    }
}
