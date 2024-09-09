use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    interface::serde::{
        operator::SerdeOperatorAddr,
        processor::{ProcessorLevel, ProcessorMode},
    },
    ontology::OntologyInit,
    phf::PhfMap,
    DefId, DomainIndex, PropId,
};

use super::data::{TypeAddr, TypeData};

#[derive(Serialize, Deserialize)]
pub struct GraphqlSchema {
    pub domain_index: DomainIndex,
    pub query: TypeAddr,
    pub mutation: TypeAddr,
    pub page_info: TypeAddr,
    pub json_scalar: TypeAddr,
    pub i64_custom_scalar: Option<TypeAddr>,
    pub types: Vec<TypeData>,
    pub type_addr_by_def: FnvHashMap<(DefId, QueryLevel), TypeAddr>,
    pub type_addr_by_typename: PhfMap<TypeAddr>,
    pub interface_implementors: FnvHashMap<TypeAddr, Vec<InterfaceImplementor>>,
}

/// Only used for the "flattened union" case for now
#[derive(Serialize, Deserialize)]
pub struct InterfaceImplementor {
    pub addr: TypeAddr,
    pub attribute_predicate: Box<[(PropId, DefId)]>,
}

impl GraphqlSchema {
    pub fn type_data(&self, type_addr: TypeAddr) -> &TypeData {
        &self.types[type_addr.0 as usize]
    }

    pub fn type_data_by_key(&self, key: (DefId, QueryLevel)) -> Option<&TypeData> {
        let type_addr = self.type_addr_by_def.get(&key)?;
        Some(self.type_data(*type_addr))
    }

    pub fn push_type_data(&mut self, type_data: TypeData) -> TypeAddr {
        let type_addr = TypeAddr(self.types.len() as u32);
        self.types.push(type_data);
        type_addr
    }

    pub(crate) fn empty() -> Self {
        Self {
            domain_index: DomainIndex(0),
            query: TypeAddr(0),
            mutation: TypeAddr(0),
            page_info: TypeAddr(0),
            json_scalar: TypeAddr(0),
            i64_custom_scalar: None,
            types: vec![],
            type_addr_by_def: Default::default(),
            type_addr_by_typename: Default::default(),
            interface_implementors: Default::default(),
        }
    }
}

impl OntologyInit for GraphqlSchema {
    fn ontology_init(&mut self, ontology: &crate::ontology::Ontology) {
        for type_data in self.types.iter_mut() {
            type_data.ontology_init(ontology);
        }

        self.type_addr_by_typename.ontology_init(ontology);
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum TypingPurpose {
    Selection,
    Input,
    PartialInput,
    InputOrReference,
    EntityId,
}

impl TypingPurpose {
    pub fn child(self) -> Self {
        match self {
            Self::Selection => Self::Selection,
            Self::Input => Self::Input,
            Self::PartialInput => Self::Input,
            Self::InputOrReference => Self::Input,
            Self::EntityId => Self::EntityId,
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
            TypingPurpose::EntityId => (ProcessorMode::Raw, ProcessorLevel::new_root()),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash, Debug)]
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
