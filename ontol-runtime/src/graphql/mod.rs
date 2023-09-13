use crate::serde::{
    operator::SerdeOperatorId,
    processor::{ProcessorLevel, ProcessorMode},
};

pub mod argument;
pub mod data;
pub mod schema;

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
            TypingPurpose::Selection => (ProcessorMode::Inspect, ProcessorLevel::new_root()),
            TypingPurpose::Input => (ProcessorMode::Create, ProcessorLevel::new_root()),
            TypingPurpose::PartialInput => (ProcessorMode::Update, ProcessorLevel::new_root()),
            TypingPurpose::ReferenceInput => (ProcessorMode::Create, ProcessorLevel::new_child()),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum QueryLevel {
    Node,
    Edge { rel_params: Option<SerdeOperatorId> },
    Connection { rel_params: Option<SerdeOperatorId> },
}
