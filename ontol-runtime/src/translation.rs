use crate::{vm::EntryPoint, DefId};

pub struct Translation {
    pub from: DefId,
    pub to: DefId,
    pub entry_point: EntryPoint,
}
