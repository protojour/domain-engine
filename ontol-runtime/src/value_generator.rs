use ::serde::{Deserialize, Serialize};

use crate::vm::proc::Address;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ValueGenerator {
    DefaultProc(Address),
    Uuid,
    Autoincrement,
    CreatedAtTime,
    UpdatedAtTime,
}
