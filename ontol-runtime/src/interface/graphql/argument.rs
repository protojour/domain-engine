use serde::{Deserialize, Serialize};

use crate::{interface::serde::operator::SerdeOperatorId, DefId};

use super::{data::TypeIndex, schema::TypingPurpose};

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

    fn operator_id(&self) -> SerdeOperatorId;

    fn kind(&self) -> ArgKind;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Input {
    pub type_index: TypeIndex,
    pub def_id: DefId,
    pub operator_id: SerdeOperatorId,
    pub typing_purpose: TypingPurpose,
}

impl FieldArg for Input {
    fn name(&self) -> &str {
        "input"
    }
}

impl DomainFieldArg for Input {
    fn typing_purpose(&self) -> TypingPurpose {
        self.typing_purpose
    }

    fn operator_id(&self) -> SerdeOperatorId {
        self.operator_id
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Def(self.type_index, self.def_id)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Id(pub SerdeOperatorId);

impl FieldArg for Id {
    fn name(&self) -> &str {
        "id"
    }
}

impl DomainFieldArg for Id {
    fn operator_id(&self) -> SerdeOperatorId {
        self.0
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Operator(self.0)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct First;

impl FieldArg for First {
    fn name(&self) -> &str {
        "first"
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct After;

impl FieldArg for After {
    fn name(&self) -> &str {
        "after"
    }
}
