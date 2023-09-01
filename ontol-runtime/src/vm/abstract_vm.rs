use crate::{
    ontology::Ontology,
    text_pattern::TextPattern,
    value::PropertyId,
    vm::proc::{BuiltinProc, Local, OpCode, Predicate, Procedure},
    DefId,
};

/// Abstract virtual machine for executing ONTOL procedures.
///
/// The stack of the stack machine is abstracted away.
///
/// The abstract machine is in charge of the program counter and the call stack.
pub struct AbstractVm<'o, P: Processor> {
    /// The position of the pending program opcode
    program_counter: usize,
    /// The address where the current frame started executing
    proc_address: usize,
    /// Stack for restoring state when returning from a subroutine.
    /// When a `Return` opcode is executed and this stack is empty, the VM evaluation session ends.
    call_stack: Vec<CallStackFrame<P>>,

    pub(crate) ontology: &'o Ontology,
}

/// A stack frame indicating a procedure called another procedure.
/// The currently executing procedure is _not_ on the stack.
struct CallStackFrame<P: Processor> {
    /// The stack to be restored after the frame is popped.
    stack: Vec<P::Value>,
    /// The program position to resume when this frame is popped.
    program_counter: usize,
    /// What the program counter started as
    proc_address: usize,
}

/// Trait for implementing stacks.
pub trait Processor {
    type Value: Sized;

    fn size(&self) -> usize;
    fn stack_mut(&mut self) -> &mut Vec<Self::Value>;

    fn call_builtin(&mut self, proc: BuiltinProc, result_type: DefId);
    fn clone(&mut self, source: Local);
    fn bump(&mut self, source: Local);
    fn pop_until(&mut self, local: Local);
    fn swap(&mut self, a: Local, b: Local);
    fn iter_next(&mut self, seq: Local, index: Local) -> bool;
    fn take_attr2(&mut self, source: Local, key: PropertyId);
    fn try_take_attr2(&mut self, source: Local, key: PropertyId);
    fn put_attr1(&mut self, target: Local, key: PropertyId);
    fn put_attr2(&mut self, target: Local, key: PropertyId);
    fn push_i64(&mut self, k: i64, result_type: DefId);
    fn push_f64(&mut self, k: f64, result_type: DefId);
    fn push_string(&mut self, k: &str, result_type: DefId);
    fn append_attr2(&mut self, seq: Local);
    fn append_string(&mut self, to: Local);
    fn cond_predicate(&mut self, predicate: &Predicate) -> bool;
    fn type_pun(&mut self, local: Local, def_id: DefId);
    fn regex_capture(
        &mut self,
        local: Local,
        text_pattern: &TextPattern,
        capture_indexes: &[PatternCaptureGroup],
    );
    fn assert_true(&mut self);
}

impl<'o, P: Processor> AbstractVm<'o, P> {
    pub fn new(ontology: &'o Ontology) -> Self {
        Self {
            program_counter: 0,
            proc_address: 0,
            ontology,
            call_stack: vec![],
        }
    }

    pub fn pending_opcode(&self) -> &OpCode {
        &self.ontology.lib.opcodes[self.program_counter]
    }

    pub fn execute(&mut self, procedure: Procedure, processor: &mut P, debug: &mut dyn VmDebug<P>) {
        self.program_counter = procedure.address.0 as usize;
        self.proc_address = procedure.address.0 as usize;

        let opcodes = self.ontology.lib.opcodes.as_slice();

        loop {
            debug.tick(self, processor);

            match &opcodes[self.program_counter] {
                OpCode::Goto(offset) => {
                    self.program_counter = self.proc_address + offset.0 as usize;
                }
                OpCode::Call(procedure) => {
                    let current_stack = processor.stack_mut();
                    let mut next_stack: Vec<P::Value> = Default::default();

                    let params_start = current_stack.len() - (procedure.n_params.0 as usize);
                    next_stack.extend(current_stack.drain(params_start..));

                    std::mem::swap(current_stack, &mut next_stack);

                    self.call_stack.push(CallStackFrame {
                        stack: next_stack,
                        program_counter: self.program_counter + 1,
                        proc_address: self.proc_address,
                    });
                    self.program_counter = procedure.address.0 as usize;
                    self.proc_address = procedure.address.0 as usize;
                }
                OpCode::Return(local) => {
                    processor.swap(*local, Local(0));
                    return0!(self, processor);
                }
                OpCode::Return0 => {
                    return0!(self, processor);
                }
                OpCode::CallBuiltin(builtin_proc, result_type) => {
                    processor.call_builtin(*builtin_proc, *result_type);
                    self.program_counter += 1;
                }
                OpCode::Clone(source) => {
                    processor.clone(*source);
                    self.program_counter += 1;
                }
                OpCode::Bump(source) => {
                    processor.bump(*source);
                    self.program_counter += 1;
                }
                OpCode::PopUntil(local) => {
                    processor.pop_until(*local);
                    self.program_counter += 1;
                }
                OpCode::TakeAttr2(source, property_id) => {
                    processor.take_attr2(*source, *property_id);
                    self.program_counter += 1;
                }
                OpCode::TryTakeAttr2(source, property_id) => {
                    processor.try_take_attr2(*source, *property_id);
                    self.program_counter += 1;
                }
                OpCode::PutAttr1(target, property_id) => {
                    processor.put_attr1(*target, *property_id);
                    self.program_counter += 1;
                }
                OpCode::PutAttr2(target, property_id) => {
                    processor.put_attr2(*target, *property_id);
                    self.program_counter += 1;
                }
                OpCode::I64(k, result_type) => {
                    processor.push_i64(*k, *result_type);
                    self.program_counter += 1;
                }
                OpCode::F64(k, result_type) => {
                    processor.push_f64(*k, *result_type);
                    self.program_counter += 1;
                }
                OpCode::String(k, result_type) => {
                    processor.push_string(k, *result_type);
                    self.program_counter += 1;
                }
                OpCode::Iter(seq, index, offset) => {
                    if processor.iter_next(*seq, *index) {
                        self.program_counter = self.proc_address + offset.0 as usize;
                    } else {
                        self.program_counter += 1;
                    }
                }
                OpCode::AppendAttr2(seq) => {
                    processor.append_attr2(*seq);
                    self.program_counter += 1;
                }
                OpCode::AppendString(to) => {
                    processor.append_string(*to);
                    self.program_counter += 1;
                }
                OpCode::Cond(predicate, offset) => {
                    if processor.cond_predicate(predicate) {
                        self.program_counter = self.proc_address + offset.0 as usize;
                    } else {
                        self.program_counter += 1;
                    }
                }
                OpCode::TypePun(local, def_id) => {
                    processor.type_pun(*local, *def_id);
                    self.program_counter += 1;
                }
                OpCode::RegexCapture(local, def_id, groups) => {
                    let text_pattern = self.ontology.get_text_pattern(*def_id).unwrap();
                    processor.regex_capture(*local, text_pattern, groups);
                    self.program_counter += 1;
                }
                OpCode::AssertTrue => {
                    processor.assert_true();
                    self.program_counter += 1;
                }
            }
        }
    }
}

/// Return Local(0) from a function, yielding control to the next pushed stack frame,
/// or returning from the VM if there are no more frames.
macro_rules! return0 {
    ($vm:ident, $processor:ident) => {
        match $vm.call_stack.pop() {
            Some(CallStackFrame {
                stack: mut next_stack,
                program_counter,
                proc_address,
            }) => {
                $vm.program_counter = program_counter;
                $vm.proc_address = proc_address;
                let returning_stack = $processor.stack_mut();

                // transfer return value to next stack frame
                returning_stack.truncate(1);
                next_stack.push(returning_stack.drain(0..1).next().unwrap());

                // swap stacks so that the processor's stack is now `next_stack`
                // instead of `returning_stack`.
                std::mem::swap(returning_stack, &mut next_stack);
            }
            None => {
                return;
            }
        }
    };
}

pub(crate) use return0;

use super::proc::PatternCaptureGroup;

pub trait VmDebug<P: Processor> {
    fn tick(&mut self, vm: &AbstractVm<P>, processor: &P);
}

impl<P: Processor> VmDebug<P> for () {
    #[inline(always)]
    fn tick(&mut self, _: &AbstractVm<P>, _: &P) {}
}
