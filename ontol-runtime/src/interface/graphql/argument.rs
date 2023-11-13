use serde::{Deserialize, Serialize};
use smartstring::alias::String;

use crate::{interface::serde::operator::SerdeOperatorAddr, DefId};

use super::{
    data::{TypeAddr, UnitTypeRef},
    schema::TypingPurpose,
};

pub enum ArgKind {
    Addr(TypeAddr),
    Operator(SerdeOperatorAddr),
    /// The argument is "hidden" in GraphQL.
    /// The reason can be that it's a struct without members.
    Hidden,
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

    fn default_arg(&self) -> Option<DefaultArg>;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DefaultArg {
    EmptyObject,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct InputArg {
    pub type_addr: TypeAddr,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
    pub typing_purpose: TypingPurpose,
}

impl FieldArg for InputArg {
    fn name(&self) -> &str {
        "input"
    }
}

impl DomainFieldArg for InputArg {
    fn typing_purpose(&self) -> TypingPurpose {
        self.typing_purpose
    }

    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Addr(self.type_addr)
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        None
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MapInputArg {
    pub operator_addr: SerdeOperatorAddr,
    /// If this string is defined, there will be a single argument with this name.
    pub scalar_input_name: Option<String>,
    pub default_arg: Option<DefaultArg>,
    pub hidden: bool,
}

impl FieldArg for MapInputArg {
    fn name(&self) -> &str {
        match &self.scalar_input_name {
            Some(name) => name,
            None => "input",
        }
    }
}

impl DomainFieldArg for MapInputArg {
    fn typing_purpose(&self) -> TypingPurpose {
        TypingPurpose::Input
    }

    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        if self.hidden {
            ArgKind::Hidden
        } else {
            ArgKind::Operator(self.operator_addr)
        }
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        self.default_arg.clone()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IdArg {
    pub operator_addr: SerdeOperatorAddr,
    pub unit_type_ref: UnitTypeRef,
}

impl FieldArg for IdArg {
    fn name(&self) -> &str {
        "id"
    }
}

impl DomainFieldArg for IdArg {
    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        match &self.unit_type_ref {
            UnitTypeRef::Addr(type_addr) => ArgKind::Addr(*type_addr),
            UnitTypeRef::NativeScalar(_) => ArgKind::Operator(self.operator_addr),
        }
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        None
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FirstArg;

impl FieldArg for FirstArg {
    fn name(&self) -> &str {
        "first"
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AfterArg;

impl FieldArg for AfterArg {
    fn name(&self) -> &str {
        "after"
    }
}
