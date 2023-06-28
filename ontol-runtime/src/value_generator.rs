use crate::vm::proc::Address;

#[derive(Clone, Copy, Debug)]
pub enum ValueGenerator {
    DefaultProc(Address),
    UuidV4,
    Autoincrement,
    CreatedAtTime,
    UpdatedAtTime,
}
