use ontol_runtime::{
    value::PropertyId,
    vm::proc::{BuiltinProc, Local, PatternCaptureGroup, Predicate, Procedure},
    DefId,
};
use smartstring::alias::String;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockIndex(pub u32);

#[derive(Clone, Copy, Debug)]
pub struct BlockOffset(pub u32);

impl BlockOffset {
    pub fn plus(self, offset: u32) -> Self {
        Self(self.0 + offset)
    }
}

#[derive(Debug)]
#[allow(unused)]
pub enum Ir {
    Call(Procedure),
    CallBuiltin(BuiltinProc, DefId),
    PopUntil(Local),
    /// Clone a local and put top of stack
    Clone(Local),
    /// Bump some local to top, leaving Unit behind
    Bump(Local),
    Iter(Local, Local, BlockIndex),
    /// Take attribute and push two values on the stack: value(top), rel_params
    TakeAttr2(Local, PropertyId),
    TryTakeAttr2(Local, PropertyId),
    PutAttr1(Local, PropertyId),
    PutAttr2(Local, PropertyId),
    AppendAttr2(Local),
    I64(i64, DefId),
    F64(f64, DefId),
    String(String, DefId),
    Cond(Predicate, BlockIndex),
    TypePun(Local, DefId),
    RegexCapture(Local, DefId, Box<[PatternCaptureGroup]>),
    AssertTrue,
}

#[derive(Clone, Debug)]
pub enum Terminator {
    /// The procedure returns a specific local
    Return(Local),
    /// All the block locals are popped and control resumes at the parent block
    PopGoto(BlockIndex, BlockOffset),
}
