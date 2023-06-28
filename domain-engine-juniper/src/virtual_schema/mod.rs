use std::sync::Arc;

use ontol_runtime::{
    env::Env,
    serde::{
        operator::SerdeOperatorId,
        processor::{ProcessorLevel, ProcessorMode},
    },
};

use self::data::{TypeData, TypeIndex};

pub mod argument;
pub mod data;

mod builder;
mod namespace;
mod schema;

pub use schema::VirtualSchema;

#[derive(Copy, Clone, Debug)]
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
            TypingPurpose::Selection => (ProcessorMode::Read, ProcessorLevel::new_root()),
            TypingPurpose::Input => (ProcessorMode::Create, ProcessorLevel::new_root()),
            TypingPurpose::PartialInput => (ProcessorMode::Update, ProcessorLevel::new_root()),
            TypingPurpose::ReferenceInput => (ProcessorMode::Create, ProcessorLevel::new_child()),
        }
    }
}

#[derive(Clone)]
pub struct VirtualIndexedTypeInfo {
    pub virtual_schema: Arc<VirtualSchema>,
    pub type_index: TypeIndex,
    pub typing_purpose: TypingPurpose,
}

impl VirtualIndexedTypeInfo {
    pub fn env(&self) -> &Env {
        &self.virtual_schema.env
    }

    pub fn type_data(&self) -> &TypeData {
        self.virtual_schema.type_data(self.type_index)
    }

    pub fn typename(&self) -> &str {
        let type_data = &self.type_data();
        match self.typing_purpose {
            TypingPurpose::Selection => &type_data.typename,
            TypingPurpose::Input | TypingPurpose::ReferenceInput => type_data
                .input_typename
                .as_deref()
                .expect("No input typename available"),
            TypingPurpose::PartialInput => type_data
                .partial_input_typename
                .as_deref()
                .expect("No partial input typename available"),
        }
    }

    pub fn description(&self) -> Option<String> {
        self.type_data().description(self.env())
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum QueryLevel {
    Node,
    Edge { rel_params: Option<SerdeOperatorId> },
    Connection { rel_params: Option<SerdeOperatorId> },
}
