use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{
    format_utils::Literal,
    ontology::domain::EntityOrder,
    value::{Value, ValueDebug},
    OntolDefTag,
};

use super::{condition::Condition, order::Direction};

/// A combination of a condition and an order, without select.
#[derive(Clone, Serialize, Deserialize)]
pub struct Filter {
    condition: Condition,
    order: Order,
    direction: Option<Direction>,
}

pub struct InvalidDirection;

impl Filter {
    pub fn default_for_domain() -> Self {
        Self {
            condition: Condition::default(),
            order: Order::Symbolic(vec![]),
            direction: None,
        }
    }

    pub fn default_for_datastore() -> Self {
        Self {
            condition: Condition::default(),
            order: Order::EntityOrder(vec![]),
            direction: None,
        }
    }

    pub fn condition(&self) -> &Condition {
        &self.condition
    }

    /// Symbolic order is the order format expected inside domains
    pub fn symbolic_order(&self) -> Option<&[Value]> {
        match &self.order {
            Order::Symbolic(symbols) => Some(symbols.as_slice()),
            Order::EntityOrder(_) => None,
        }
    }

    /// Entity order is the order format expected inside the data stores
    pub fn entity_order(&self) -> Option<&[EntityOrder]> {
        match &self.order {
            Order::Symbolic(_) => None,
            Order::EntityOrder(paths) => Some(paths.as_slice()),
        }
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or(Direction::Ascending)
    }

    pub fn condition_mut(&mut self) -> &mut Condition {
        &mut self.condition
    }

    pub fn set_symbolic_order(&mut self, order: Value) {
        self.order = match order {
            Value::Sequence(seq, _) => Order::Symbolic(seq.elements.into_iter().collect()),
            other => Order::Symbolic(vec![other]),
        };
    }

    pub fn set_entity_order(&mut self, order: Vec<EntityOrder>) {
        self.order = Order::EntityOrder(order);
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

#[derive(Clone, Serialize, Deserialize)]
pub enum Order {
    /// This is the ordering while in domain-space.
    Symbolic(Vec<Value>),
    /// This is the ordering in data-store space.
    EntityOrder(Vec<EntityOrder>),
}

impl Order {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Symbolic(vals) => vals.is_empty(),
            Self::EntityOrder(props) => props.is_empty(),
        }
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
            match &self.order {
                Order::Symbolic(symbols) => {
                    for order in symbols {
                        write!(f, " {}", ValueDebug(order))?;
                    }
                }
                Order::EntityOrder(_paths) => {
                    write!(f, "(field-paths)")?;
                }
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
