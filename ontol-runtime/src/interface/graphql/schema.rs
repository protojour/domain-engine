use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    interface::serde::{
        operator::SerdeOperatorAddr,
        processor::{ProcessorLevel, ProcessorMode},
    },
    DefId, PackageId,
};

use super::data::{TypeAddr, TypeData};

#[derive(Serialize, Deserialize)]
pub struct GraphqlSchema {
    pub package_id: PackageId,
    pub query: TypeAddr,
    pub mutation: TypeAddr,
    pub page_info: TypeAddr,
    pub json_scalar: TypeAddr,
    pub i64_custom_scalar: Option<TypeAddr>,
    pub types: Vec<TypeData>,
    pub type_addr_by_def: FnvHashMap<(DefId, QueryLevel), TypeAddr>,
}

impl GraphqlSchema {
    pub fn type_data(&self, type_addr: TypeAddr) -> &TypeData {
        &self.types[type_addr.0 as usize]
    }

    pub fn type_data_by_key(&self, key: (DefId, QueryLevel)) -> Option<&TypeData> {
        let type_addr = self.type_addr_by_def.get(&key)?;
        Some(self.type_data(*type_addr))
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum TypingPurpose {
    Selection,
    Input,
    PartialInput,
    InputOrReference,
}

impl TypingPurpose {
    pub fn child(self) -> Self {
        match self {
            Self::Selection => Self::Selection,
            Self::Input => Self::Input,
            Self::PartialInput => Self::Input,
            Self::InputOrReference => Self::Input,
        }
    }

    pub const fn mode_and_level(self) -> (ProcessorMode, ProcessorLevel) {
        match self {
            TypingPurpose::Selection => (ProcessorMode::Raw, ProcessorLevel::new_root()),
            TypingPurpose::Input => (ProcessorMode::Create, ProcessorLevel::new_root()),
            TypingPurpose::PartialInput => (ProcessorMode::Update, ProcessorLevel::new_root()),
            TypingPurpose::InputOrReference => (ProcessorMode::Create, ProcessorLevel::new_child()),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum QueryLevel {
    Node,
    Edge {
        rel_params: Option<SerdeOperatorAddr>,
    },
    Connection {
        rel_params: Option<SerdeOperatorAddr>,
    },
}
