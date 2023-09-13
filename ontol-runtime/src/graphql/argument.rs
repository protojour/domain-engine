use crate::{serde::operator::SerdeOperatorId, DefId};

use super::{data::TypeIndex, TypingPurpose};

pub enum ArgKind {
    Def(TypeIndex, DefId),
    Operator(SerdeOperatorId),
}

pub trait FieldArg {
    fn name(&self) -> &str;
}

pub trait DomainFieldArg: FieldArg {
    fn typing_purpose(&self) -> TypingPurpose {
        TypingPurpose::Input
    }

    fn kind(&self) -> ArgKind;
}

#[derive(Debug)]
pub struct Input(pub TypeIndex, pub DefId, pub TypingPurpose);

impl FieldArg for Input {
    fn name(&self) -> &str {
        "input"
    }
}

impl DomainFieldArg for Input {
    fn typing_purpose(&self) -> TypingPurpose {
        self.2
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Def(self.0, self.1)
    }
}

#[derive(Debug)]
pub struct Id(pub SerdeOperatorId);

impl FieldArg for Id {
    fn name(&self) -> &str {
        "id"
    }
}

impl DomainFieldArg for Id {
    fn kind(&self) -> ArgKind {
        ArgKind::Operator(self.0)
    }
}

#[derive(Debug)]
pub struct First;

impl FieldArg for First {
    fn name(&self) -> &str {
        "first"
    }
}

#[derive(Debug)]
pub struct After;

impl FieldArg for After {
    fn name(&self) -> &str {
        "after"
    }
}
