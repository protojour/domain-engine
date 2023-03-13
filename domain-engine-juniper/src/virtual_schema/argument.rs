use ontol_runtime::{serde::operator::SerdeOperatorId, DefId};

use super::{data::TypeIndex, TypingPurpose};

#[derive(Debug)]
pub enum FieldArgument {
    Input(TypeIndex, DefId, TypingPurpose),
    Id(SerdeOperatorId, TypingPurpose),
}

impl FieldArgument {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Input(..) => "input",
            Self::Id(..) => "id",
        }
    }
}
