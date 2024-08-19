use std::fmt::{Debug, Display};

use ::serde::{Deserialize, Serialize};
use bit_vec::BitVec;
use ontol_macros::OntolDebug;

use crate::{
    debug::OntolDebug,
    impl_ontol_debug,
    ontology::ontol::TextConstant,
    property::ValueCardinality,
    query::{condition::ClausePair, filter::Filter},
    value::Value,
    var::Var,
    DefId, PropId,
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

#[derive(Clone, Copy, Serialize, Deserialize, Debug, OntolDebug)]
pub struct Address(pub u32);

#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub struct AddressOffset(pub u32);

/// Handle to an ONTOL procedure.
///
/// The VM is a stack machine, the arguments to the called procedure
/// must be top of the stack when it's called.
#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub struct Procedure {
    /// 'Pointer' to the first OpCode
    pub address: Address,
    /// Number of parameters
    pub n_params: NParams,
}

/// The number of parameters to a procedure.
#[derive(Clone, Copy, Serialize, Deserialize, OntolDebug)]
pub struct NParams(pub u8);

/// ONTOL opcode.
///
/// When the documentation mentions the stack, the _leftmost_ value is the top of the stack.
#[derive(Serialize, Deserialize, OntolDebug)]
pub enum OpCode {
    /// Take the top of stack, and push that value onto the previous frame's stack,
    /// and continue executing at the previous frame's program counter, or exit VM with popped value
    Return,
    /// Go to address, relative to start_address
    Goto(AddressOffset),
    /// Call a procedure. Its arguments must be top of the value stack.
    Call(Procedure),
    /// Clone a specific local, putting its clone on the top of the stack.
    Clone(Local),
    /// Take a local, replace with unit, and put on top of stack
    Bump(Local),
    /// Pop all at top of stack, until (not including) the given local
    PopUntil(Local),
    /// Call a builtin procedure
    CallBuiltin(BuiltinProc, DefId),
    /// Call the given extern, having the given return value
    CallExtern(DefId, DefId),
    /// Iterate all items in #0, #1 is the counter.
    /// Pushes two items on the stack
    Iter(Local, u8, Local, AddressOffset),
    /// Get an attribute from a struct.
    /// See GetAttrFlags for different semantic variations.
    GetAttr(Local, PropId, u8, GetAttrFlags),
    /// Pop 1 value from stack, and move it into the specified local struct. Sets the attribute parameter to unit.
    PutAttrUnit(Local, PropId),
    /// Pop n stack values, insert them as a tuple attribute in reverse order
    PutAttrTup(Local, u8, PropId),
    /// Pop n stack values which must be sequence-typed, insert them as columns in a matrix-valued attribute in reverse order
    PutAttrMat(Local, u8, PropId),
    /// Move rest attrs from the second local into the first local.
    MoveRestAttrs(Local, Local),
    /// Pop N stack values, push each of those consecutively into N different contiguous sequences
    /// starting at the given local position.
    SeqAppendN(Local, u8),
    /// Pop 1 stack value, which must be Data::String, and append to local which must also be a string
    AppendString(Local),
    /// Push a constant i64 to the stack.
    I64(i64, DefId),
    /// Push a constant f64 to the stack.
    F64(f64, DefId),
    /// Push a constant string to the stack.
    String(TextConstant, DefId),
    /// Evaluate a predicate. If true, jumps to AddressOffset.
    Cond(Predicate, AddressOffset),
    /// Take the sequence stored at Local, replace it with unit, and push all its values onto the stack.
    /// These sequences must have a size known in advance, typically temporary storage.
    MoveSeqValsToStack(Local),
    CopySubSeq(Local, Local),
    /// Overwrite runtime type info with a new type (the local on the top of stack)
    TypePunTop(DefId),
    /// Overwrite runtime type info with a new type
    TypePun(Local, DefId),
    /// Run a regex search on the first match of string at Local.
    /// The next instruction must be a `RegexCaptureIndexes`.
    /// RegexCaptureIndexes contains a bit vector of the capture groups to save, and push on the stack.
    /// Pushes one value to the stack: A sequence of strings if successful, #void otherwise
    RegexCapture(Local, DefId),
    RegexCaptureIter(Local, DefId),
    /// Required parameter to RegexCapture and RegexCaptureIter
    RegexCaptureIndexes(BitVec),
    /// Push a new condition variable derived from the condition to the stack
    CondVar(Local),
    /// Push a condition clause into the condition at local
    PushCondClause(Local, ClausePair<Local, OpCodeCondTerm>),
    /// Execute a match on a datastore, using the filter at top of stack
    MatchFilter(Var, ValueCardinality),
    Panic(TextConstant),
}

/// A reference to a local on the value stack during procedure execution.
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct Local(pub u16);

impl_ontol_debug!(Local);

impl Debug for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "L{}", self.0)
    }
}

impl Display for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "L{}", self.0)
    }
}

/// Builtin procedures.
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub enum BuiltinProc {
    AddI64,
    SubI64,
    MulI64,
    DivI64,
    AddF64,
    SubF64,
    MulF64,
    DivF64,
    Append,
    NewStruct,
    NewSeq,
    NewUnit,
    NewFilter,
    NewVoid,
}

impl_ontol_debug!(BuiltinProc);

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, OntolDebug)]
pub enum Predicate {
    MatchesDiscriminant(Local, DefId),
    NotMatchesDiscriminant(Local, DefId),
    IsVoid(Local),
    IsNotVoid(Local),
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug, Serialize, Deserialize)]
    pub struct GetAttrFlags: u8 {
        /// If flag is set, take the attribute instead of cloning it
        const TAKE  = 0b00000001;
    }
}

impl_ontol_debug!(GetAttrFlags);

pub enum Yield {
    Match(Var, ValueCardinality, Filter),
    CallExtern(DefId, Value, DefId),
}

#[derive(Serialize, Deserialize, OntolDebug)]
pub enum OpCodeCondTerm {
    Wildcard,
    CondVar(Local),
    Value(Local),
}

impl Display for OpCodeCondTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.debug(&()))
    }
}
