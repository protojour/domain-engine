use arcstr::{ArcStr, literal};
use serde::{Deserialize, Serialize};

use crate::{
    DefId,
    interface::serde::operator::SerdeOperatorAddr,
    ontology::{Ontology, ontol::TextConstant},
};

use super::{
    data::{Optionality, TypeAddr, TypeModifier},
    schema::TypingPurpose,
};

pub enum ArgKind {
    Addr(TypeAddr, TypeModifier),
    Operator(SerdeOperatorAddr),
    /// The argument is "hidden" in GraphQL.
    /// The reason can be that it's a struct without members.
    Hidden,
}

pub trait FieldArg {
    fn name(&self, ontology: &Ontology) -> ArcStr;
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
    EmptyList,
    EmptyObject,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MapInputArg {
    pub operator_addr: SerdeOperatorAddr,
    /// If this string is defined, there will be a single argument with this name.
    pub scalar_input_name: Option<TextConstant>,
    pub default_arg: Option<DefaultArg>,
    pub hidden: bool,
}

impl FieldArg for MapInputArg {
    fn name(&self, ontology: &Ontology) -> ArcStr {
        match &self.scalar_input_name {
            Some(name) => ontology.clone_text_constant(*name),
            None => literal!("input"),
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
pub struct InputArg {
    pub type_addr: TypeAddr,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
    pub typing_purpose: TypingPurpose,
}

impl FieldArg for InputArg {
    fn name(&self, _: &Ontology) -> ArcStr {
        literal!("input")
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
        ArgKind::Addr(self.type_addr, TypeModifier::Unit(Optionality::Mandatory))
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        None
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EntityCreateInputsArg {
    pub type_addr: TypeAddr,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
}

impl FieldArg for EntityCreateInputsArg {
    fn name(&self, _: &Ontology) -> ArcStr {
        literal!("create")
    }
}

impl DomainFieldArg for EntityCreateInputsArg {
    fn typing_purpose(&self) -> TypingPurpose {
        TypingPurpose::Input
    }

    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Addr(
            self.type_addr,
            TypeModifier::Array {
                array: Optionality::Optional,
                element: Optionality::Mandatory,
            },
        )
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        Some(DefaultArg::EmptyList)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EntityUpdateInputsArg {
    pub type_addr: TypeAddr,
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
}

impl FieldArg for EntityUpdateInputsArg {
    fn name(&self, _: &Ontology) -> ArcStr {
        literal!("update")
    }
}

impl DomainFieldArg for EntityUpdateInputsArg {
    fn typing_purpose(&self) -> TypingPurpose {
        TypingPurpose::PartialInput
    }

    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Addr(
            self.type_addr,
            TypeModifier::Array {
                array: Optionality::Optional,
                element: Optionality::Mandatory,
            },
        )
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        Some(DefaultArg::EmptyList)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EntityDeleteInputsArg {
    pub def_id: DefId,
    pub operator_addr: SerdeOperatorAddr,
}

impl FieldArg for EntityDeleteInputsArg {
    fn name(&self, _: &Ontology) -> ArcStr {
        literal!("delete")
    }
}

impl DomainFieldArg for EntityDeleteInputsArg {
    fn typing_purpose(&self) -> TypingPurpose {
        TypingPurpose::EntityId
    }

    fn operator_addr(&self) -> SerdeOperatorAddr {
        self.operator_addr
    }

    fn kind(&self) -> ArgKind {
        ArgKind::Operator(self.operator_addr)
    }

    fn default_arg(&self) -> Option<DefaultArg> {
        Some(DefaultArg::EmptyList)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FirstArg;

impl FieldArg for FirstArg {
    fn name(&self, _: &Ontology) -> ArcStr {
        literal!("first")
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AfterArg;

impl FieldArg for AfterArg {
    fn name(&self, _: &Ontology) -> ArcStr {
        literal!("after")
    }
}
