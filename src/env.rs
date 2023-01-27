use std::collections::HashMap;

use smartstring::alias::String;

use crate::{
    serde::{SerdeOperator, SerdeOperatorId, SerdeProcessor, SerdeRegistry},
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

    pub fn new_serde_processor(&self, serde_operator_id: SerdeOperatorId) -> SerdeProcessor {
        SerdeRegistry::new(&self.serde_operators).make_processor(serde_operator_id)
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
