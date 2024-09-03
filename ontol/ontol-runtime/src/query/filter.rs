use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{
    format_utils::Literal,
    value::{Value, ValueDebug},
    OntolDefTag,
};

use super::{condition::Condition, order::Direction};

/// A combination of a condition and an order, without select.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Filter {
    condition: Condition,
    order: Vec<Value>,
    direction: Option<Direction>,
}

pub struct InvalidDirection;

impl Filter {
    pub fn condition(&self) -> &Condition {
        &self.condition
    }

    pub fn order(&self) -> &[Value] {
        self.order.as_ref()
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or(Direction::Ascending)
    }

    pub fn condition_mut(&mut self) -> &mut Condition {
        &mut self.condition
    }

    pub fn set_order(&mut self, order: Value) {
        self.order = match order {
            Value::Sequence(seq, _) => seq.elements.into_iter().collect(),
            other => vec![other],
        };
    }

    pub fn set_direction(&mut self, direction: Value) -> Result<(), InvalidDirection> {
        let def_id = direction.type_def_id();

        if def_id == OntolDefTag::SymAscending.def_id() {
            self.direction = Some(Direction::Ascending);
        } else if def_id == OntolDefTag::SymDescending.def_id() {
            self.direction = Some(Direction::Descending);
        } else {
            return Err(InvalidDirection);
        }

        Ok(())
    }
}

impl Debug for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.condition)?;
        if !self.order.is_empty() {
            write!(f, "(order")?;
            for order in &self.order {
                write!(f, " {}", ValueDebug(order))?;
            }
            writeln!(f, ")")?;
        }
        if let Some(direction) = &self.direction {
            writeln!(f, "(direction {:?})", direction)?;
        }

        Ok(())
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
