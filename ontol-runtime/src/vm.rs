use std::{array, collections::HashMap, fmt::Debug};

use smartstring::alias::String;

use crate::{value::Value, PropertyId};

pub struct ProcId(u32);

#[derive(Clone, Copy)]
pub struct Local(pub u32);

impl Debug for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Local({})", self.0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NArgs(pub u8);

#[derive(Default)]
pub struct Program {
    opcodes: Vec<OpCode>,
}

#[derive(Clone, Copy, Debug)]
pub struct EntryPoint {
    start: u32,
    n_args: NArgs,
}

impl Program {
    pub fn add_procedure(&mut self, n_args: NArgs, mut opcodes: Vec<OpCode>) -> EntryPoint {
        let start = self.opcodes.len() as u32;

        self.opcodes.append(&mut opcodes);
        EntryPoint { start, n_args }
    }
}

#[derive(Debug)]
pub enum OpCode {
    /// Call a procedure. Its arguments must be top of the stack.
    Call(EntryPoint),
    /// Return a specific local
    Return(Local),
    /// Optimization: Return Local(0)
    Return0,
    CallBuiltin(BuiltinProc),
    Clone(Local),
    TakeAttr(Local, PropertyId),
    PutAttr(Local, PropertyId),
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
    stack_pos: usize,
    program_counter: usize,

    program: &'p Program,
    value_stack: Vec<Value>,
    call_stack: Vec<(usize, usize)>,
}

pub trait VmDebug {
    fn tick(&mut self, vm: &Vm);
}

impl<'p> Vm<'p> {
    pub fn new(program: &'p Program) -> Self {
        Self {
            stack_pos: 0,
            program_counter: 0,
            program,
            value_stack: vec![],
            call_stack: vec![],
        }
    }

    pub fn eval(&mut self, proc: EntryPoint, args: Vec<Value>) -> Value {
        self.eval_debug(proc, args, &mut ())
    }

    pub fn eval_log(&mut self, proc: EntryPoint, args: Vec<Value>) -> Value {
        self.eval_debug(proc, args, &mut VmLogger)
    }

    pub fn eval_debug<D: VmDebug>(
        &mut self,
        entry_point: EntryPoint,
        args: Vec<Value>,
        debug: &mut D,
    ) -> Value {
        for arg in args {
            self.value_stack.push(arg);
        }

        self.program_counter = entry_point.start as usize;
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
                OpCode::Call(entry_point) => {
                    self.call_stack
                        .push((self.program_counter + 1, self.stack_pos));
                    self.stack_pos = self.value_stack.len() - entry_point.n_args.0 as usize;
                    self.program_counter = entry_point.start as usize;
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
        &self.value_stack[self.stack_pos + local.0 as usize]
    }

    #[inline(always)]
    fn local_mut(&mut self, local: Local) -> &mut Value {
        &mut self.value_stack[self.stack_pos + local.0 as usize]
    }

    #[inline(always)]
    fn swap(&mut self, a: Local, b: Local) {
        let stack_pos = self.stack_pos;
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
        let pop = $vm.value_stack.len() - ($vm.stack_pos + 1);
        for _ in 0..pop {
            $vm.value_stack.pop();
        }

        match $vm.call_stack.pop() {
            Some((program_counter, stack_pos)) => {
                $vm.program_counter = program_counter;
                $vm.stack_pos = stack_pos;
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

struct VmLogger;

impl VmDebug for VmLogger {
    fn tick(&mut self, vm: &Vm) {
        println!("   -> {:?}", vm.value_stack);
        println!("{:?}", vm.program.opcodes[vm.program_counter]);
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
            NArgs(1),
            vec![
                OpCode::CallBuiltin(BuiltinProc::NewCompound),
                OpCode::TakeAttr(Local(0), PropertyId(1)),
                OpCode::PutAttr(Local(1), PropertyId(3)),
                OpCode::TakeAttr(Local(0), PropertyId(2)),
                OpCode::PutAttr(Local(1), PropertyId(4)),
                OpCode::Return(Local(1)),
            ],
        );

        let mut vm = Vm::new(&program);
        let output = vm.eval_log(
            proc,
            vec![Value::Compound(
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
            NArgs(1),
            vec![
                OpCode::Clone(Local(0)),
                OpCode::CallBuiltin(BuiltinProc::Add),
                OpCode::Return0,
            ],
        );
        let add_then_double = program.add_procedure(
            NArgs(2),
            vec![
                OpCode::CallBuiltin(BuiltinProc::Add),
                OpCode::Call(double),
                OpCode::Return0,
            ],
        );
        let translate = program.add_procedure(
            NArgs(1),
            vec![
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
        let output = vm.eval_log(
            translate,
            vec![Value::Compound(
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
