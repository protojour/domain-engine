use bit_vec::BitVec;
use tracing::trace;

use super::proc::{GetAttrFlags, OpCodeCondTerm};
use crate::{
    condition::Clause,
    ontology::{Ontology, ValueCardinality},
    text_pattern::TextPattern,
    value::PropertyId,
    var::Var,
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
    type Yield: Sized;

    fn size(&self) -> usize;
    fn stack_mut(&mut self) -> &mut Vec<Self::Value>;

    fn call_builtin(&mut self, proc: BuiltinProc, result_type: DefId);
    fn clone(&mut self, source: Local);
    fn bump(&mut self, source: Local);
    fn pop_until(&mut self, local: Local);
    fn swap(&mut self, a: Local, b: Local);
    fn iter_next(&mut self, seq: Local, index: Local) -> bool;
    fn get_attr(&mut self, source: Local, key: PropertyId, flags: GetAttrFlags);
    fn put_attr1(&mut self, target: Local, key: PropertyId);
    fn put_attr2(&mut self, target: Local, key: PropertyId);
    fn push_i64(&mut self, k: i64, result_type: DefId);
    fn push_f64(&mut self, k: f64, result_type: DefId);
    fn push_string(&mut self, k: &str, result_type: DefId);
    fn append_attr2(&mut self, seq: Local);
    fn append_string(&mut self, to: Local);
    fn cond_predicate(&mut self, predicate: &Predicate) -> bool;
    fn move_seq_vals_to_stack(&mut self, source: Local);
    fn type_pun(&mut self, local: Option<Local>, def_id: DefId);
    fn regex_capture(&mut self, local: Local, text_pattern: &TextPattern, index_filter: &BitVec);
    fn regex_capture_iter(
        &mut self,
        local: Local,
        text_pattern: &TextPattern,
        index_filter: &BitVec,
    );
    fn assert_true(&mut self);
    fn push_cond_clause(&mut self, cond_local: Local, clause: &Clause<OpCodeCondTerm>);
    fn yield_match_condition(
        &mut self,
        var: Var,
        value_cardinality: ValueCardinality,
    ) -> Self::Yield;
}

impl<'o, P: Processor> AbstractVm<'o, P> {
    pub fn new(ontology: &'o Ontology, procedure: Procedure) -> Self {
        trace!("AbstractVm::new({procedure:?})");
        Self {
            program_counter: procedure.address.0 as usize,
            proc_address: procedure.address.0 as usize,
            ontology,
            call_stack: vec![],
        }
    }

    pub fn pending_opcode(&self) -> &OpCode {
        &self.ontology.lib.opcodes[self.program_counter]
    }

    pub fn run(&mut self, processor: &mut P, debug: &mut dyn VmDebug<P>) -> Option<P::Yield> {
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
                OpCode::Return => {
                    match self.call_stack.pop() {
                        Some(CallStackFrame {
                            stack: mut next_stack,
                            program_counter,
                            proc_address,
                        }) => {
                            self.program_counter = program_counter;
                            self.proc_address = proc_address;
                            let returning_stack = processor.stack_mut();

                            // transfer return value to next stack frame
                            let return_value = returning_stack.pop().unwrap();
                            next_stack.push(return_value);

                            // swap stacks so that the processor's stack is now `next_stack`
                            // instead of `returning_stack`.
                            std::mem::swap(returning_stack, &mut next_stack);
                        }
                        None => {
                            return None;
                        }
                    }
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
                OpCode::GetAttr(local, property_id, flags) => {
                    processor.get_attr(*local, *property_id, *flags);
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
                OpCode::MoveSeqValsToStack(local) => {
                    processor.move_seq_vals_to_stack(*local);
                    self.program_counter += 1;
                }
                OpCode::TypePunTop(def_id) => {
                    processor.type_pun(None, *def_id);
                    self.program_counter += 1;
                }
                OpCode::TypePun(local, def_id) => {
                    processor.type_pun(Some(*local), *def_id);
                    self.program_counter += 1;
                }
                opcode @ (OpCode::RegexCapture(local, def_id)
                | OpCode::RegexCaptureIter(local, def_id)) => {
                    let text_pattern = self.ontology.get_text_pattern(*def_id).unwrap();
                    self.program_counter += 1;
                    let OpCode::RegexCaptureIndexes(index_filter) = &opcodes[self.program_counter]
                    else {
                        panic!("Expected capture indexes");
                    };
                    if matches!(opcode, OpCode::RegexCaptureIter(..)) {
                        processor.regex_capture_iter(*local, text_pattern, index_filter);
                    } else {
                        processor.regex_capture(*local, text_pattern, index_filter);
                    }
                    self.program_counter += 1;
                }
                OpCode::RegexCaptureIndexes(_) => unreachable!(),
                OpCode::AssertTrue => {
                    processor.assert_true();
                    self.program_counter += 1;
                }
                OpCode::PushCondClause(cond_local, clause) => {
                    processor.push_cond_clause(*cond_local, clause);
                    self.program_counter += 1;
                }
                OpCode::MatchCondition(var, cardinality) => {
                    self.program_counter += 1;
                    return Some(processor.yield_match_condition(*var, *cardinality));
                }
                OpCode::Panic(message) => {
                    panic!("{message}");
                }
            }
        }
    }
}

pub trait VmDebug<P: Processor> {
    fn tick(&mut self, vm: &AbstractVm<P>, processor: &P);
}

impl<P: Processor> VmDebug<P> for () {
    #[inline(always)]
    fn tick(&mut self, _: &AbstractVm<P>, _: &P) {}
}
