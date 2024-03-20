use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    interface::serde::{
        operator::SerdeOperatorAddr,
        processor::{ProcessorLevel, ProcessorMode},
    },
    ontology::OntologyInit,
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

    pub(crate) fn empty() -> Self {
        Self {
            package_id: PackageId(0),
            query: TypeAddr(0),
            mutation: TypeAddr(0),
            page_info: TypeAddr(0),
            json_scalar: TypeAddr(0),
            i64_custom_scalar: None,
            types: vec![],
            type_addr_by_def: Default::default(),
        }
    }
}

impl OntologyInit for GraphqlSchema {
    fn ontology_init(&mut self, ontology: &crate::ontology::Ontology) {
        for type_data in self.types.iter_mut() {
            type_data.ontology_init(ontology);
        }
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
            TypingPurpose::PartialInput => {
                (ProcessorMode::GraphqlUpdate, ProcessorLevel::new_root())
            }
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
    MutationResult,
}
