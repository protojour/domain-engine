use derive_debug_extras::DebugExtras;
use smartstring::alias::String;

use crate::{value::PropertyId, DefId};

/// A complete ONTOL code library consisting of procedures.
/// This structure only stores opcodes.
/// Handles to procedures are held outside the Lib itself.
#[derive(Default)]
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
        self.opcodes.extend(opcodes.into_iter());

        Procedure { address, n_params }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Address(pub u32);

#[derive(Clone, Copy, Debug)]
pub struct AddressOffset(pub u32);

/// Handle to an ONTOL procedure.
///
/// The VM is a stack machine, the arguments to the called procedure
/// must be top of the stack when it's called.
#[derive(Clone, Copy, Debug)]
pub struct Procedure {
    /// 'Pointer' to the first OpCode
    pub address: Address,
    /// Number of parameters
    pub n_params: NParams,
}

/// The number of parameters to a procedure.
#[derive(Clone, Copy, Debug)]
pub struct NParams(pub u8);

/// ONTOL opcode.
///
/// When the documentation mentions the stack, the _leftmost_ value is the top of the stack.
#[derive(DebugExtras)]
pub enum OpCode {
    /// Return a specific local
    Return(Local),
    /// Optimization: Return Local(0)
    Return0,
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
    /// Take attribute and push two values on the stack: [value, rel_params].
    /// The attribute _must_ be present.
    TakeAttr2(Local, PropertyId),
    /// Try to take attr, with two outcomes:
    /// If present, pushes three values on the stack: [value, rel_params, Int(1)].
    /// If absent, pushes one value on the stack: [Int(0)].
    TryTakeAttr2(Local, PropertyId),
    /// Pop 1 value from stack, and move it into the specified local struct. Sets the attribute parameter to unit.
    PutAttr1(Local, PropertyId),
    /// Pop 2 stack values, rel_params (top) then value, and move it into the specified local struct.
    PutAttr2(Local, PropertyId),
    /// Pop 2 stack values, rel_params (top) then value, and append resulting attribute to sequence
    AppendAttr2(Local),
    /// Push a constant i64 to the stack.
    I64(i64, DefId),
    /// Push a constant string to the stack.
    String(String, DefId),
    /// Evaluate a predicate. If true, jumps to AddressOffset.
    Cond(Predicate, AddressOffset),
    /// Overwrite runtime type info with a new type
    TypePun(Local, DefId),
}

/// A reference to a local on the value stack during procedure execution.
#[derive(Clone, Copy, Eq, PartialEq, DebugExtras)]
pub struct Local(pub u16);

/// Builtin procedures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuiltinProc {
    Add,
    Sub,
    Mul,
    Div,
    Append,
    NewStruct,
    NewSeq,
    NewUnit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Predicate {
    MatchesDiscriminant(Local, DefId),
    /// Test if true. NB: Yanks from stack.
    YankTrue(Local),
}
