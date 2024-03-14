use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::format_utils::Literal;

use super::condition::Condition;

/// A combination of a condition and an order, without select.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Filter {
    condition: Condition,
}

impl Filter {
    pub fn condition(&self) -> &Condition {
        &self.condition
    }

    pub fn condition_mut(&mut self) -> &mut Condition {
        &mut self.condition
    }
}

impl Debug for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.condition)
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.condition)
    }
}

/// The PartialEq implementation is only meant for debugging purposes
impl<'a> PartialEq<Literal<'a>> for Filter {
    fn eq(&self, other: &Literal<'a>) -> bool {
        let a = format!("{self}");
        let b = format!("{other}");
        a == b
    }
}
