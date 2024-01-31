use ontol_runtime::{
    var::Var,
    vm::proc::{Local, OpCode, Predicate},
};
use smartstring::alias::String;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BlockLabel(pub Var);

#[derive(Clone, Copy, Debug)]
pub struct BlockOffset(pub u32);

impl BlockOffset {
    pub fn plus(self, offset: u32) -> Self {
        Self(self.0 + offset)
    }
}

/// "Intermediate representation" of opcodes.
/// Some opcodes that involve branching use addresses,
/// but these are not computed until the whole procedure is computed.
#[derive(Debug)]
#[allow(unused)]
pub enum Ir {
    /// Raw OpCode.
    /// Ir should not contain raw opcodes that refer to raw addresses.
    /// For these, there are special Ir versions.
    Op(OpCode),
    /// Take attribute and push two values on the stack: value(top), rel_params
    Cond(Predicate, BlockLabel),
    /// Iterate sequence using param(1) as counter. Call block for each iteration.
    Iter(Local, Local, BlockLabel),
}

#[derive(Clone, Debug)]
pub enum Terminator {
    /// Return the top of the stack
    Return,
    /// Just a "goto", nothing is popped
    Goto(BlockLabel, BlockOffset),
    /// Just enter the next block. No instruction needed.
    GotoNext,
    /// All the block locals are popped and control resumes at the parent block
    PopGoto(BlockLabel, BlockOffset),
    Panic(String),
}
