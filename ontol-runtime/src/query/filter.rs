use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{
    format_utils::Literal,
    ontology::Ontology,
    value::{Value, ValueDebug},
};

use super::{condition::Condition, order::Direction};

/// A combination of a condition and an order, without select.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Filter {
    condition: Condition,
    order: Option<Value>,
    direction: Option<Direction>,
}

pub struct InvalidDirection;

impl Filter {
    pub fn condition(&self) -> &Condition {
        &self.condition
    }

    pub fn order(&self) -> Option<&Value> {
        self.order.as_ref()
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or(Direction::Ascending)
    }

    pub fn condition_mut(&mut self) -> &mut Condition {
        &mut self.condition
    }

    pub fn set_order(&mut self, order: Value) {
        self.order = Some(order);
    }

    pub fn set_direction(
        &mut self,
        direction: Value,
        ontology: &Ontology,
    ) -> Result<(), InvalidDirection> {
        let def_id = direction.type_def_id();
        let meta = ontology.ontol_domain_meta();

        if def_id == meta.ascending {
            self.direction = Some(Direction::Ascending);
        } else if def_id == meta.descending {
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
        if let Some(order) = &self.order {
            writeln!(f, "(order {})", ValueDebug(order))?;
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
