use ontol_runtime::{
    proc::{BuiltinProc, Local, Procedure},
    value::PropertyId,
    DefId,
};

#[derive(Clone, Copy, Debug)]
pub struct BlockIndex(pub u32);

#[derive(Debug)]
#[allow(unused)]
pub enum Ir {
    Call(Procedure),
    CallBuiltin(BuiltinProc, DefId),
    Remove(Local),
    /// Clone a local and put top of stack
    Clone(Local),
    Iter(Local, Local, BlockIndex),
    /// Take attribute and push two values on the stack: value(top), rel_params
    TakeAttr2(Local, PropertyId),
    PutAttrValue(Local, PropertyId),
    AppendAttr2(Local),
    Constant(i64, DefId),
}

#[derive(Clone, Debug)]
pub enum Terminator {
    Return(Local),
    Goto(BlockIndex, u32),
}
