use serde::{Deserialize, Serialize};

use crate::{interface::serde::operator::SerdeOperatorId, DefId};

use super::{
    data::{TypeIndex, UnitTypeRef},
    schema::TypingPurpose,
};

pub enum ArgKind {
    Indexed(TypeIndex),
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
        ArgKind::Indexed(self.type_index)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Id {
    pub operator_id: SerdeOperatorId,
    pub unit_type_ref: UnitTypeRef,
}

impl FieldArg for Id {
    fn name(&self) -> &str {
        "id"
    }
}

impl DomainFieldArg for Id {
    fn operator_id(&self) -> SerdeOperatorId {
        self.operator_id
    }

    fn kind(&self) -> ArgKind {
        match &self.unit_type_ref {
            UnitTypeRef::Indexed(type_index) => ArgKind::Indexed(*type_index),
            UnitTypeRef::NativeScalar(_) => ArgKind::Operator(self.operator_id),
        }
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
