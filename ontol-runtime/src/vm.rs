use crate::{
    proc::{BuiltinProc, Lib, Local, OpCode, Procedure},
    PropertyId,
};

/// Abstract virtual machine for executing ONTOL procedures.
///
/// The stack of the stack machine is abstracted away.
///
/// The abstract machine is in charge of the program counter and the call stack.
pub struct AbstractVm<'l> {
    /// The position of the pending program opcode
    program_counter: usize,
    /// Stack for restoring state when returning from a subroutine.
    /// When a `Return` opcode is executed and this stack is empty, the VM evaluation session ends.
    call_stack: Vec<CallStackFrame>,

    /// Reference to the ONTOL library being executed
    pub(crate) lib: &'l Lib,
}

/// A stack frame indicating a procedure called another procedure.
/// The currently executing procedure is _not_ on the stack.
struct CallStackFrame {
    /// The stack position to restore when this frame is popped.
    local0_pos: usize,
    /// The program position to resume when this frame is popped.
    program_counter: usize,
}

/// Trait for implementing stacks.
pub trait Stack {
    fn size(&self) -> usize;
    fn local0_pos(&self) -> usize;
    fn local0_pos_mut(&mut self) -> &mut usize;

    fn truncate(&mut self, n_locals: usize);
    fn call_builtin(&mut self, proc: BuiltinProc);
    fn clone(&mut self, source: Local);
    fn swap(&mut self, a: Local, b: Local);
    fn take_attr(&mut self, source: Local, property_id: PropertyId);
    fn put_attr(&mut self, target: Local, property_id: PropertyId);
    fn constant(&mut self, k: i64);
}

impl<'l> AbstractVm<'l> {
    pub fn new(lib: &'l Lib) -> Self {
        Self {
            program_counter: 0,
            lib,
            call_stack: vec![],
        }
    }

    pub fn pending_opcode(&self) -> &OpCode {
        &self.lib.opcodes[self.program_counter]
    }

    pub fn execute<S: Stack>(
        &mut self,
        procedure: Procedure,
        stack: &mut S,
        debug: &mut impl VmDebug<S>,
    ) {
        self.program_counter = procedure.start as usize;

        let opcodes = self.lib.opcodes.as_slice();

        loop {
            debug.tick(self, stack);

            match &opcodes[self.program_counter] {
                OpCode::Call(procedure) => {
                    self.call_stack.push(CallStackFrame {
                        program_counter: self.program_counter + 1,
                        local0_pos: stack.local0_pos(),
                    });
                    *stack.local0_pos_mut() = stack.size() - procedure.n_params.0 as usize;
                    self.program_counter = procedure.start as usize;
                }
                OpCode::Return(local) => {
                    stack.swap(*local, Local(0));
                    return0!(self, stack);
                }
                OpCode::Return0 => {
                    return0!(self, stack);
                }
                OpCode::CallBuiltin(builtin_proc) => {
                    stack.call_builtin(*builtin_proc);
                    self.program_counter += 1;
                }
                OpCode::Clone(source) => {
                    stack.clone(*source);
                    self.program_counter += 1;
                }
                OpCode::Swap(a, b) => {
                    stack.swap(*a, *b);
                    self.program_counter += 1;
                }
                OpCode::TakeAttr(source, property_id) => {
                    stack.take_attr(*source, *property_id);
                    self.program_counter += 1;
                }
                OpCode::PutAttr(target, property_id) => {
                    stack.put_attr(*target, *property_id);
                    self.program_counter += 1;
                }
                OpCode::Constant(k) => {
                    stack.constant(*k);
                    self.program_counter += 1;
                }
            }
        }
    }
}

macro_rules! return0 {
    ($vm:ident, $stack:ident) => {
        $stack.truncate(1);

        match $vm.call_stack.pop() {
            Some(CallStackFrame {
                program_counter,
                local0_pos,
            }) => {
                $vm.program_counter = program_counter;
                *$stack.local0_pos_mut() = local0_pos;
            }
            None => {
                return;
            }
        }
    };
}

pub(crate) use return0;

pub trait VmDebug<S: Stack> {
    fn tick(&mut self, vm: &AbstractVm, stack: &S);
}

impl<S: Stack> VmDebug<S> for () {
    #[inline(always)]
    fn tick(&mut self, _: &AbstractVm, _: &S) {}
}
