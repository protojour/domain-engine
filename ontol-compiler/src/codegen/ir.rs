use ontol_runtime::{
    proc::{BuiltinProc, Local, Procedure},
    value::PropertyId,
    DefId,
};

use crate::SourceSpan;

#[derive(Clone, Copy, Debug)]
pub struct BlockIndex(pub u32);

pub struct Instr(pub Ir, pub SourceSpan);

#[derive(Debug)]
#[allow(unused)]
pub enum Ir {
    Call(Procedure),
    CallBuiltin(BuiltinProc, DefId),
    Remove(Local),
    /// Clone a local and put top of stack
    Clone(Local),
    Swap(Local, Local),
    Iter(Local, Local, BlockIndex),
    LoadAttr(Local, PropertyId),
    PutUnitAttr(Local, PropertyId),
    AppendAttr(Local),
    Constant(i64, DefId),
    Unit,
}

#[derive(Clone, Debug)]
pub enum Terminator {
    Return(Local),
    Goto(BlockIndex, u32),
}
