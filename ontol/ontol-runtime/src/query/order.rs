use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Default for Direction {
    fn default() -> Self {
        Self::Ascending
    }
}

impl Direction {
    /// Re-order an [Ordering] by this direction
    pub fn reorder(self, ordering: Ordering) -> Ordering {
        match self {
            Self::Ascending => ordering,
            Self::Descending => ordering.reverse(),
        }
    }

    /// Chain two directions.
    ///
    /// for example, applying descending twice leads to ascending order.
    pub fn chain(self, other: Direction) -> Direction {
        if self == other {
            Self::Ascending
        } else {
            Self::Descending
        }
    }
}
