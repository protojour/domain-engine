use std::{array, collections::HashMap};

use smartstring::alias::String;
use tracing::debug;

use crate::{
    proc::{BuiltinProc, Lib, Local, Procedure},
    value::Value,
    vm::{AbstractVm, Stack, VmDebug},
    PropertyId,
};

/// Virtual machine for executing ONTOL procedures
pub struct Translator<'l> {
    abstract_vm: AbstractVm<'l>,
    value_stack: ValueStack,
}

impl<'l> Translator<'l> {
    pub fn new(lib: &'l Lib) -> Self {
        Self {
            abstract_vm: AbstractVm::new(lib),
            value_stack: ValueStack::default(),
        }
    }

    pub fn eval(&mut self, proc: Procedure, args: impl IntoIterator<Item = Value>) -> Value {
        self.internal_eval(proc, args, &mut ())
    }

    pub fn trace_eval(&mut self, proc: Procedure, args: impl IntoIterator<Item = Value>) -> Value {
        self.internal_eval(proc, args, &mut Tracer)
    }

    pub fn internal_eval(
        &mut self,
        procedure: Procedure,
        args: impl IntoIterator<Item = Value>,
        debug: &mut impl VmDebug<ValueStack>,
        // debug: &mut D,
    ) -> Value {
        for arg in args {
            self.value_stack.stack.push(arg);
        }

        self.abstract_vm.program_counter = procedure.start as usize;
        self.abstract_vm.run(&mut self.value_stack, debug);

        let value_stack = std::mem::take(&mut self.value_stack);
        if value_stack.stack.len() != 1 {
            panic!("Stack did not contain one value");
        }
        value_stack.stack.into_iter().next().unwrap()
    }
}

#[derive(Default)]
pub struct ValueStack {
    local0_pos: usize,
    stack: Vec<Value>,
}

impl Stack for ValueStack {
    fn size(&self) -> usize {
        self.stack.len()
    }

    fn local0_pos(&self) -> usize {
        self.local0_pos
    }

    fn local0_pos_mut(&mut self) -> &mut usize {
        &mut self.local0_pos
    }

    fn truncate(&mut self, n_locals: usize) {
        self.stack.truncate(self.local0_pos + n_locals);
    }

    fn call_builtin(&mut self, proc: BuiltinProc) {
        let value = self.eval_builtin(proc);
        self.stack.push(value);
    }

    fn clone(&mut self, source: Local) {
        let value = self.local(source).clone();
        self.stack.push(value);
    }

    fn swap(&mut self, a: Local, b: Local) {
        let stack_pos = self.local0_pos;
        self.stack
            .swap(stack_pos + a.0 as usize, stack_pos + b.0 as usize);
    }

    fn take_attr(&mut self, source: Local, property_id: PropertyId) {
        let compound = self.compound_local_mut(source);
        let value = compound.remove(&property_id).expect("Attribute not found");
        self.stack.push(value);
    }

    fn put_attr(&mut self, target: Local, property_id: PropertyId) {
        let value = self.stack.pop().unwrap();
        let compound = self.compound_local_mut(target);
        compound.insert(property_id, value);
    }

    fn constant(&mut self, k: i64) {
        self.stack.push(Value::Number(k));
    }
}

impl ValueStack {
    fn eval_builtin(&mut self, proc: BuiltinProc) -> Value {
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
        &self.stack[self.local0_pos + local.0 as usize]
    }

    #[inline(always)]
    fn local_mut(&mut self, local: Local) -> &mut Value {
        &mut self.stack[self.local0_pos + local.0 as usize]
    }

    fn compound_local_mut(&mut self, local: Local) -> &mut HashMap<PropertyId, Value> {
        match self.local_mut(local) {
            Value::Compound(hash_map) => hash_map,
            _ => panic!("Value at {local:?} is not a compound value"),
        }
    }

    #[inline(always)]
    fn pop_one(&mut self) -> Value {
        match self.stack.pop() {
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

struct Tracer;

impl VmDebug<ValueStack> for Tracer {
    fn tick(&mut self, vm: &AbstractVm, stack: &ValueStack) {
        debug!("   -> {:?}", stack.stack);
        debug!("{:?}", vm.lib.opcodes[vm.program_counter]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::proc::{NParams, OpCode};

    use super::*;

    #[test]
    fn translate_map() {
        let mut lib = Lib::default();
        let proc = lib.add_procedure(
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

        let mut vm = Translator::new(&lib);
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
        let mut lib = Lib::default();
        let double = lib.add_procedure(
            NParams(1),
            [
                OpCode::Clone(Local(0)),
                OpCode::CallBuiltin(BuiltinProc::Add),
                OpCode::Return0,
            ],
        );
        let add_then_double = lib.add_procedure(
            NParams(2),
            [
                OpCode::CallBuiltin(BuiltinProc::Add),
                OpCode::Call(double),
                OpCode::Return0,
            ],
        );
        let translate = lib.add_procedure(
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

        let mut vm = Translator::new(&lib);
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
