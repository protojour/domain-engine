use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    interface::serde::{
        operator::SerdeOperatorId,
        processor::{ProcessorLevel, ProcessorMode},
    },
    DefId, PackageId,
};

use super::data::{TypeData, TypeIndex};

#[derive(Serialize, Deserialize)]
pub struct GraphqlSchema {
    pub package_id: PackageId,
    pub query: TypeIndex,
    pub mutation: TypeIndex,
    pub i64_custom_scalar: Option<TypeIndex>,
    pub types: Vec<TypeData>,
    pub type_index_by_def: FnvHashMap<(DefId, QueryLevel), TypeIndex>,
}

impl GraphqlSchema {
    pub fn type_data(&self, type_index: TypeIndex) -> &TypeData {
        &self.types[type_index.0 as usize]
    }

    pub fn type_data_by_key(&self, key: (DefId, QueryLevel)) -> Option<&TypeData> {
        let type_index = self.type_index_by_def.get(&key)?;
        Some(self.type_data(*type_index))
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum TypingPurpose {
    Selection,
    Input,
    PartialInput,
    ReferenceInput,
}

impl TypingPurpose {
    pub fn child(self) -> Self {
        match self {
            Self::Selection => Self::Selection,
            Self::Input => Self::Input,
            Self::PartialInput => Self::Input,
            Self::ReferenceInput => Self::Input,
        }
    }

    pub const fn mode_and_level(self) -> (ProcessorMode, ProcessorLevel) {
        match self {
            TypingPurpose::Selection => (ProcessorMode::Inspect, ProcessorLevel::new_root()),
            TypingPurpose::Input => (ProcessorMode::Create, ProcessorLevel::new_root()),
            TypingPurpose::PartialInput => (ProcessorMode::Update, ProcessorLevel::new_root()),
            TypingPurpose::ReferenceInput => (ProcessorMode::Create, ProcessorLevel::new_child()),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum QueryLevel {
    Node,
    Edge { rel_params: Option<SerdeOperatorId> },
    Connection { rel_params: Option<SerdeOperatorId> },
}
