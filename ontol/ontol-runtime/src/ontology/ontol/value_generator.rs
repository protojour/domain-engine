use ::serde::{Deserialize, Serialize};

use crate::{impl_ontol_debug, vm::proc::Address};

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum ValueGenerator {
    DefaultProc(Address),
    Uuid,
    Autoincrement,
    CreatedAtTime,
    UpdatedAtTime,
}

impl_ontol_debug!(ValueGenerator);
