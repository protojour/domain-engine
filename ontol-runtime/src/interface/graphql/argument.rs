use serde::{Deserialize, Serialize};

use crate::{interface::serde::operator::SerdeOperatorAddr, DefId};

use super::{
    data::{TypeAddr, UnitTypeRef},
    schema::TypingPurpose,
};

pub enum ArgKind {
    Addr(TypeAddr),
    Operator(SerdeOperatorAddr),
}

pub trait FieldArg {
    fn name(&self) -> &str;
}

pub trait DomainFieldArg: FieldArg {
    fn typing_purpose(&self) -> TypingPurpose {
        TypingPurpose::Input
    }

    fn operator_addr(&self) -> SerdeOperatorAddr;

    fn kind(&self) -> ArgKind;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Input {
    pub type_addr: TypeAddr,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
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

    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Addr(self.type_addr)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Id {
    pub operator_addr: SerdeOperatorAddr,
    pub unit_type_ref: UnitTypeRef,
}

impl FieldArg for Id {
    fn name(&self) -> &str {
        "id"
    }
}

impl DomainFieldArg for Id {
    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        match &self.unit_type_ref {
            UnitTypeRef::Addr(type_addr) => ArgKind::Addr(*type_addr),
            UnitTypeRef::NativeScalar(_) => ArgKind::Operator(self.operator_addr),
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
