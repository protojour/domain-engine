use derive_debug_extras::DebugExtras;

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
        let address = self.opcodes.len() as u32;
        self.opcodes.extend(opcodes.into_iter());

        Procedure { address, n_params }
    }
}

/// Handle to an ONTOL procedure.
///
/// The VM is a stack machine, the arguments to the called procedure
/// must be top of the stack when it's called.
#[derive(Clone, Copy, Debug)]
pub struct Procedure {
    /// 'Pointer' to the first OpCode
    pub address: u32,
    /// Number of parameters
    pub n_params: NParams,
}

/// The number of parameters to a procedure.
#[derive(Clone, Copy, Debug)]
pub struct NParams(pub u8);

/// ONTOL opcode.
#[derive(DebugExtras)]
pub enum OpCode {
    /// Call a procedure. Its arguments must be top of the value stack.
    Call(Procedure),
    /// Return a specific local
    Return(Local),
    /// Optimization: Return Local(0)
    Return0,
    /// Call a builtin procedure
    CallBuiltin(BuiltinProc, DefId),
    /// Clone a specific local, putting its clone on the top of the stack.
    Clone(Local),
    /// Swap the position of two locals.
    Swap(Local, Local),
    /// Take an attribute from local map, and put its value on the top of the stack.
    /// Discards the attribute edge.
    TakeAttrValue(Local, PropertyId),
    /// Pop value from stack, and move it into the specified local map.
    PutUnitAttr(Local, PropertyId),
    /// Push a constant to the stack.
    Constant(i64, DefId),
    /// Push a sequence to the stack.
    Sequence(DefId),
}

/// A reference to a local on the value stack during procedure execution.
#[derive(Clone, Copy, Eq, PartialEq, DebugExtras)]
pub struct Local(pub u32);

/// Builtin procedures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuiltinProc {
    Add,
    Sub,
    Mul,
    Div,
    Append,
    NewMap,
}
