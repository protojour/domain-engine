use std::{array, collections::HashMap, fmt::Debug};

use derive_debug_extras::DebugExtras;
use smartstring::alias::String;
use tracing::debug;

use crate::{value::Value, PropertyId};

pub struct ProcId(u32);

#[derive(Clone, Copy, Eq, PartialEq, DebugExtras)]
pub struct Local(pub u32);

#[derive(Clone, Copy, Debug)]
pub struct NParams(pub u8);

#[derive(Default)]
pub struct Program {
    opcodes: Vec<OpCode>,
}

/// Handle to an ONTOL procedure.
///
/// The VM is a stack machine, the arguments to the called procedure
/// must be top of the stack when it's called.
#[derive(Clone, Copy, Debug)]
pub struct Procedure {
    start: u32,
    n_params: NParams,
}

impl Program {
    pub fn add_procedure(
        &mut self,
        n_params: NParams,
        opcodes: impl IntoIterator<Item = OpCode>,
    ) -> Procedure {
        let start = self.opcodes.len() as u32;

        self.opcodes.extend(opcodes.into_iter());
        Procedure { start, n_params }
    }
}

#[derive(DebugExtras)]
pub enum OpCode {
    /// Call a procedure. Its arguments must be top of the value stack.
    Call(Procedure),
    /// Return a specific local
    Return(Local),
    /// Optimization: Return Local(0)
    Return0,
    /// Call a builtin procedure
    CallBuiltin(BuiltinProc),
    /// Clone a specific local, putting its clone on the top of the stack.
    Clone(Local),
    /// Swap the position of two locals.
    Swap(Local, Local),
    /// Take an attribute from local compound, and put its value on the top of the stack.
    TakeAttr(Local, PropertyId),
    /// Pop value from stack, and move it into the specified compound local.
    PutAttr(Local, PropertyId),
    /// Push a constant to the stack.
    Constant(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuiltinProc {
    Add,
    Sub,
    Mul,
    Div,
    Append,
    NewCompound,
}

pub struct Vm<'p> {
    /// The position of Local(0) on the value stack
    local0_pos: usize,
    /// The position of the pending program opcode
    program_counter: usize,
    /// Stack for storing function parameters and locals
    value_stack: Vec<Value>,
    /// Stack for restoring state when returning from a subroutine.
    /// When a `Return` opcode is executed and this stack is empty, the VM evaluation session ends.
    call_stack: Vec<StackFrame>,

    /// Reference to the program being executed
    program: &'p Program,
}

/// A stack frame indicating a procedure called another procedure.
/// The currently executing procedure is _not_ on the stack.
struct StackFrame {
    /// The stack position to restore when this frame is popped.
    local0_pos: usize,
    /// The program position to resume when this frame is popped.
    program_counter: usize,
}

pub trait VmDebug {
    fn tick(&mut self, vm: &Vm);
}

impl<'p> Vm<'p> {
    pub fn new(program: &'p Program) -> Self {
        Self {
            local0_pos: 0,
            program_counter: 0,
            program,
            value_stack: vec![],
            call_stack: vec![],
        }
    }

    pub fn eval(&mut self, proc: Procedure, args: impl IntoIterator<Item = Value>) -> Value {
        self.eval_debug(proc, args, &mut ())
    }

    pub fn trace_eval(&mut self, proc: Procedure, args: impl IntoIterator<Item = Value>) -> Value {
        self.eval_debug(proc, args, &mut VmTracer)
    }

    pub fn eval_debug<D: VmDebug>(
        &mut self,
        procedure: Procedure,
        args: impl IntoIterator<Item = Value>,
        debug: &mut D,
    ) -> Value {
        for arg in args {
            self.value_stack.push(arg);
        }

        self.program_counter = procedure.start as usize;
        self.run(debug);

        let stack = std::mem::take(&mut self.value_stack);
        if stack.len() != 1 {
            panic!("Stack did not contain one value");
        }
        stack.into_iter().next().unwrap()
    }

    fn run<D: VmDebug>(&mut self, debug: &mut D) {
        let opcodes = self.program.opcodes.as_slice();

        loop {
            debug.tick(self);

            match &opcodes[self.program_counter] {
                OpCode::Call(procedure) => {
                    self.call_stack.push(StackFrame {
                        program_counter: self.program_counter + 1,
                        local0_pos: self.local0_pos,
                    });
                    self.local0_pos = self.value_stack.len() - procedure.n_params.0 as usize;
                    self.program_counter = procedure.start as usize;
                }
                OpCode::Return(local) => {
                    self.swap(*local, Local(0));
                    return0!(self);
                }
                OpCode::Return0 => {
                    return0!(self);
                }
                OpCode::CallBuiltin(builtin_proc) => {
                    let value = self.call_builtin(*builtin_proc);
                    self.value_stack.push(value);
                    self.program_counter += 1;
                }
                OpCode::Clone(source) => {
                    let value = self.local(*source).clone();
                    self.value_stack.push(value);
                    self.program_counter += 1;
                }
                OpCode::Swap(a, b) => {
                    self.swap(*a, *b);
                    self.program_counter += 1;
                }
                OpCode::TakeAttr(source, property_id) => {
                    let compound = self.compound_local_mut(*source);
                    let value = compound.remove(&property_id).expect("Attribute not found");
                    self.value_stack.push(value);
                    self.program_counter += 1;
                }
                OpCode::PutAttr(target, property_id) => {
                    let value = self.value_stack.pop().unwrap();
                    let compound = self.compound_local_mut(*target);
                    compound.insert(*property_id, value);
                    self.program_counter += 1;
                }
                OpCode::Constant(k) => {
                    self.value_stack.push(Value::Number(*k));
                    self.program_counter += 1;
                }
            }
        }
    }

    fn call_builtin(&mut self, proc: BuiltinProc) -> Value {
        match proc {
            BuiltinProc::Add => {
                let [a, b]: [i64; 2] = self.pop_n();
                Value::Number(a + b)
            }
            BuiltinProc::Sub => {
                let [a, b]: [i64; 2] = self.pop_n();
                Value::Number(a - b)
            }
            BuiltinProc::Mul => {
                let [a, b]: [i64; 2] = self.pop_n();
                Value::Number(a * b)
            }
            BuiltinProc::Div => {
                let [a, b]: [i64; 2] = self.pop_n();
                Value::Number(a / b)
            }
            BuiltinProc::Append => {
                let [a, b]: [String; 2] = self.pop_n();
                Value::String(a + b)
            }
            BuiltinProc::NewCompound => Value::Compound([].into()),
        }
    }

    #[inline(always)]
    fn local(&self, local: Local) -> &Value {
        &self.value_stack[self.local0_pos + local.0 as usize]
    }

    #[inline(always)]
    fn local_mut(&mut self, local: Local) -> &mut Value {
        &mut self.value_stack[self.local0_pos + local.0 as usize]
    }

    #[inline(always)]
    fn swap(&mut self, a: Local, b: Local) {
        let stack_pos = self.local0_pos;
        self.value_stack
            .swap(stack_pos + a.0 as usize, stack_pos + b.0 as usize);
    }

    fn compound_local_mut(&mut self, local: Local) -> &mut HashMap<PropertyId, Value> {
        match self.local_mut(local) {
            Value::Compound(hash_map) => hash_map,
            _ => panic!("Value at {local:?} is not a compound value"),
        }
    }

    #[inline(always)]
    fn pop_one(&mut self) -> Value {
        match self.value_stack.pop() {
            Some(value) => value,
            None => panic!("Nothing to pop"),
        }
    }

    fn pop_n<T, const N: usize>(&mut self) -> [T; N]
    where
        Value: Cast<T>,
    {
        let mut arr = array::from_fn(|_| self.pop_one().cast());
        arr.reverse();
        arr
    }
}

macro_rules! return0 {
    ($vm:ident) => {
        let pop = $vm.value_stack.len() - ($vm.local0_pos + 1);
        for _ in 0..pop {
            $vm.value_stack.pop();
        }

        match $vm.call_stack.pop() {
            Some(StackFrame {
                program_counter,
                local0_pos,
            }) => {
                $vm.program_counter = program_counter;
                $vm.local0_pos = local0_pos;
            }
            None => {
                return;
            }
        }
    };
}

pub(crate) use return0;

/// Cast a value into T, panic if this fails.
trait Cast<T> {
    fn cast(self) -> T;
}

impl Cast<Value> for Value {
    fn cast(self) -> Value {
        self
    }
}

impl Cast<i64> for Value {
    fn cast(self) -> i64 {
        match self {
            Self::Number(n) => n,
            _ => panic!("not a number"),
        }
    }
}

impl Cast<String> for Value {
    fn cast(self) -> String {
        match self {
            Self::String(s) => s,
            _ => panic!("not a string"),
        }
    }
}

impl VmDebug for () {
    fn tick(&mut self, _: &Vm) {}
}

struct VmTracer;

impl VmDebug for VmTracer {
    fn tick(&mut self, vm: &Vm) {
        debug!("   -> {:?}", vm.value_stack);
        debug!("{:?}", vm.program.opcodes[vm.program_counter]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn translate_map() {
        let mut program = Program::default();
        let proc = program.add_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewCompound),
                OpCode::TakeAttr(Local(0), PropertyId(1)),
                OpCode::PutAttr(Local(1), PropertyId(3)),
                OpCode::TakeAttr(Local(0), PropertyId(2)),
                OpCode::PutAttr(Local(1), PropertyId(4)),
                OpCode::Return(Local(1)),
            ],
        );

        let mut vm = Vm::new(&program);
        let output = vm.trace_eval(
            proc,
            [Value::Compound(
                [
                    (PropertyId(1), Value::String("foo".into())),
                    (PropertyId(2), Value::String("bar".into())),
                ]
                .into(),
            )],
        );

        let Value::Compound(map) = output else {
            panic!();
        };
        let properties = map.keys().cloned().collect::<HashSet<_>>();
        assert_eq!(HashSet::from([PropertyId(3), PropertyId(4)]), properties);
    }

    #[test]
    fn call_stack() {
        let mut program = Program::default();
        let double = program.add_procedure(
            NParams(1),
            [
                OpCode::Clone(Local(0)),
                OpCode::CallBuiltin(BuiltinProc::Add),
                OpCode::Return0,
            ],
        );
        let add_then_double = program.add_procedure(
            NParams(2),
            [
                OpCode::CallBuiltin(BuiltinProc::Add),
                OpCode::Call(double),
                OpCode::Return0,
            ],
        );
        let translate = program.add_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewCompound),
                OpCode::TakeAttr(Local(0), PropertyId(1)),
                OpCode::Call(double),
                OpCode::PutAttr(Local(1), PropertyId(4)),
                OpCode::TakeAttr(Local(0), PropertyId(2)),
                OpCode::TakeAttr(Local(0), PropertyId(3)),
                OpCode::Call(add_then_double),
                OpCode::PutAttr(Local(1), PropertyId(5)),
                OpCode::Return(Local(1)),
            ],
        );

        let mut vm = Vm::new(&program);
        let output = vm.trace_eval(
            translate,
            [Value::Compound(
                [
                    (PropertyId(1), Value::Number(333)),
                    (PropertyId(2), Value::Number(10)),
                    (PropertyId(3), Value::Number(11)),
                ]
                .into(),
            )],
        );

        let Value::Compound(mut map) = output else {
            panic!();
        };
        let Value::Number(a) = map.remove(&PropertyId(4)).unwrap() else {
            panic!();
        };
        let Value::Number(b) = map.remove(&PropertyId(5)).unwrap() else {
            panic!();
        };
        assert_eq!(666, a);
        assert_eq!(42, b);
    }
}
