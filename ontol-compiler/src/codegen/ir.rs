use ontol_runtime::{
    proc::{BuiltinProc, Local, Procedure},
    value::PropertyId,
    DefId,
};

#[derive(Clone, Copy)]
pub struct BlockIndex(pub u32);

#[allow(unused)]
pub enum Ir {
    Call(Procedure),
    CallBuiltin(BuiltinProc, DefId),
    Clone(Local),
    Remove(Local),
    Swap(Local, Local),
    ForEach(Local, Local, BlockIndex),
    TakeAttrValue(Local, PropertyId),
    PutUnitAttr(Local, PropertyId),
    AppendAttr(Local),
    PushConstant(i64, DefId),
    PushUnit,
    PushSequence(DefId),
}

#[derive(Clone)]
pub enum Terminator {
    Return(Local),
    Goto(BlockIndex, u32),
}
