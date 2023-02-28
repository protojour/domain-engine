use std::collections::HashMap;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{
    proc::{Lib, Procedure},
    serde::{SerdeOperator, SerdeOperatorId, SerdeOperatorKey, SerdeProcessor},
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
    pub serde_operators_per_def: HashMap<SerdeOperatorKey, SerdeOperatorId>,
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

    pub fn get_domain(&self, package_id: &PackageId) -> Option<&Domain> {
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

    pub(crate) fn get_serde_operator(&self, operator_id: SerdeOperatorId) -> &SerdeOperator {
        &self.serde_operators[operator_id.0 as usize]
    }
}

pub struct Domain {
    pub types: IndexMap<String, TypeInfo>,
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

#[derive(Clone)]
pub struct TypeInfo {
    pub def_id: DefId,

    // If this type is an entity, this is the type of its ID
    pub entity_id: Option<DefId>,

    pub serde_operator_id: Option<SerdeOperatorId>,
}
