use std::fmt::Display;

use ::serde::{Deserialize, Serialize};
use bit_vec::BitVec;
use derive_debug_extras::DebugExtras;
use smartstring::alias::String;

use crate::{
    condition::{Clause, CondTerm, Condition},
    ontology::ValueCardinality,
    value::PropertyId,
    var::Var,
    DefId,
};

/// A complete ONTOL code library consisting of procedures.
/// This structure only stores opcodes.
/// Handles to procedures are held outside the Lib itself.
#[derive(Default, Serialize, Deserialize)]
pub struct Lib {
    pub opcodes: Vec<OpCode>,
}

impl Lib {
    pub fn append_procedure(
        &mut self,
        n_params: NParams,
        opcodes: impl IntoIterator<Item = OpCode>,
    ) -> Procedure {
        let address = Address(self.opcodes.len() as u32);
        self.opcodes.extend(opcodes);

        Procedure { address, n_params }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Address(pub u32);

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct AddressOffset(pub u32);

/// Handle to an ONTOL procedure.
///
/// The VM is a stack machine, the arguments to the called procedure
/// must be top of the stack when it's called.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Procedure {
    /// 'Pointer' to the first OpCode
    pub address: Address,
    /// Number of parameters
    pub n_params: NParams,
}

/// The number of parameters to a procedure.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct NParams(pub u8);

/// ONTOL opcode.
///
/// When the documentation mentions the stack, the _leftmost_ value is the top of the stack.
#[derive(DebugExtras, Serialize, Deserialize)]
pub enum OpCode {
    /// Take the top of stack, and push that value onto the previous frame's stack,
    /// and continue executing at the previous frame's program counter, or exit VM with popped value
    Return,
    /// Go to address, relative to start_address
    Goto(AddressOffset),
    /// Call a procedure. Its arguments must be top of the value stack.
    Call(Procedure),
    /// Call a builtin procedure
    CallBuiltin(BuiltinProc, DefId),
    /// Clone a specific local, putting its clone on the top of the stack.
    Clone(Local),
    /// Take a local, replace with unit, and put on top of stack
    Bump(Local),
    /// Pop all at top of stack, until (not including) the given local
    PopUntil(Local),
    /// Iterate all items in #0, #1 is the counter.
    /// Pushes two items on the stack
    Iter(Local, Local, AddressOffset),
    /// Get an attribute from a struct.
    /// See GetAttrFlags for different semantic variations.
    GetAttr(Local, PropertyId, GetAttrFlags),
    /// Pop 1 value from stack, and move it into the specified local struct. Sets the attribute parameter to unit.
    PutAttr1(Local, PropertyId),
    /// Pop 2 stack values, rel_params (top) then value, and move it into the specified local struct.
    PutAttr2(Local, PropertyId),
    /// Pop 2 stack values, rel_params (top) then value, and append resulting attribute to sequence
    AppendAttr2(Local),
    /// Pop 1 stack value, which must be Data::String, and append to local which must also be a string
    AppendString(Local),
    /// Push a constant i64 to the stack.
    I64(i64, DefId),
    /// Push a constant f64 to the stack.
    F64(f64, DefId),
    /// Push a constant string to the stack.
    String(String, DefId),
    /// Evaluate a predicate. If true, jumps to AddressOffset.
    Cond(Predicate, AddressOffset),
    /// Take the sequence stored at Local, replace it with unit, and push all its values onto the stack.
    /// These sequences must have a size known in advance, typically temporary storage.
    MoveSeqValsToStack(Local),
    SetSubSeq(Local, Local),
    /// Overwrite runtime type info with a new type (the local on the top of stack)
    TypePunTop(DefId),
    /// Overwrite runtime type info with a new type
    TypePun(Local, DefId),
    /// Run a regex search on the first match of string at Local.
    /// The next instruction must be a `RegexCaptureIndexes`.
    /// RegexCaptureIndexes contains a bit vector of the capture groups to save, and push on the stack.
    /// If successful, pushes n values on the stack: [I64(1), ..captures]
    /// If unsuccessful, pushes one value on the stakc: [I64(0)]
    RegexCapture(Local, DefId),
    RegexCaptureIter(Local, DefId),
    /// Required parameter to RegexCapture and RegexCaptureIter
    RegexCaptureIndexes(BitVec),
    /// Yanks True from the stack and crashes unless true
    AssertTrue,
    /// Push a condition clause into the condition at local
    PushCondClause(Local, Clause<OpCodeCondTerm>),
    /// Execute a match on a datastore, using the condition at top of stack
    MatchCondition(Var, ValueCardinality),
    Panic(String),
}

/// A reference to a local on the value stack during procedure execution.
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, DebugExtras)]
pub struct Local(pub u16);

/// Builtin procedures.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BuiltinProc {
    Add,
    Sub,
    Mul,
    Div,
    Append,
    NewStruct,
    NewSeq,
    NewUnit,
    NewCondition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    MatchesDiscriminant(Local, DefId),
    IsUnit(Local),
    /// Test if true. NB: Yanks from stack.
    YankTrue(Local),
    /// Test if not true. NB: Yanks from stack.
    YankFalse(Local),
}

bitflags::bitflags! {
    ///
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct GetAttrFlags: u8 {
        /// Push true/false on stack as status on whether the attr is present
        const TRY   = 0b00000001;
        /// If flag is set, take the attribute instead of cloning it
        const TAKE  = 0b00000010;
        /// Push attr rel param on top of stack, if present
        const REL   = 0b00000100;
        /// Push attr val on top of stack, if present
        const VAL   = 0b00001000;
    }
}

impl GetAttrFlags {
    pub fn take2() -> Self {
        Self::TAKE | Self::REL | Self::VAL
    }

    pub fn try_take2() -> Self {
        Self::TRY | Self::TAKE | Self::REL | Self::VAL
    }
}

pub enum Yield {
    Match(Var, ValueCardinality, Condition<CondTerm>),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum OpCodeCondTerm {
    Wildcard,
    Var(Var),
    Value(Local),
}

impl Display for OpCodeCondTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
