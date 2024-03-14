use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub enum Direction {
    Ascending,
    Descending,
}

impl Direction {
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
