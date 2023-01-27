use std::collections::HashMap;

use smartstring::alias::String;

use crate::{
    def::DefId,
    serde::{SerdeExecutor, SerdeOperator, SerdeOperatorId},
    PackageId,
};

/// Runtime environment
pub struct Env {
    pub(crate) serde_operators: Vec<SerdeOperator>,
    pub(crate) domains: HashMap<PackageId, Domain>,
}

impl Env {
    pub fn get_domain(&self, package_id: &PackageId) -> Option<&Domain> {
        self.domains.get(&package_id)
    }

    pub fn get_serde_executor(&self, serde_operator_id: SerdeOperatorId) -> SerdeExecutor {
        SerdeExecutor {
            current: &self.serde_operators[serde_operator_id.0 as usize],
            all_operators: &self.serde_operators,
        }
    }
}

pub struct Domain {
    pub(crate) types: HashMap<String, TypeInfo>,
}

pub(crate) struct TypeInfo {
    pub(crate) serde_operator_id: Option<SerdeOperatorId>,
}

impl Domain {
    pub fn get_serde_operator_id(&self, type_name: &str) -> Option<SerdeOperatorId> {
        let type_info = self.types.get(type_name)?;
        type_info.serde_operator_id
    }
}
