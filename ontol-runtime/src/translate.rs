use std::{array, collections::HashMap};

use smartstring::alias::String;
use tracing::debug;

use crate::{
    proc::{BuiltinProc, Lib, Local, Procedure},
    value::{Data, Value},
    vm::{AbstractVm, Stack, VmDebug},
    DefId, RelationId,
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
    ) -> Value {
        for arg in args {
            self.value_stack.stack.push(arg);
        }

        self.abstract_vm
            .execute(procedure, &mut self.value_stack, debug);

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

    fn call_builtin(&mut self, proc: BuiltinProc, result_type: DefId) {
        let data = self.eval_builtin(proc);
        self.stack.push(Value::new(data, result_type));
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

    fn take_attr(&mut self, source: Local, relation_id: RelationId) {
        let map = self.map_local_mut(source);
        let value = map.remove(&relation_id).expect("Attribute not found");
        self.stack.push(value);
    }

    fn put_attr(&mut self, target: Local, relation_id: RelationId) {
        let value = self.stack.pop().unwrap();
        let map = self.map_local_mut(target);
        map.insert(relation_id, value);
    }

    fn constant(&mut self, k: i64, result_type: DefId) {
        self.stack.push(Value::new(Data::Int(k), result_type));
    }
}

impl ValueStack {
    fn eval_builtin(&mut self, proc: BuiltinProc) -> Data {
        match proc {
            BuiltinProc::Add => {
                let [a, b]: [i64; 2] = self.pop_n();
                Data::Int(a + b)
            }
            BuiltinProc::Sub => {
                let [a, b]: [i64; 2] = self.pop_n();
                Data::Int(a - b)
            }
            BuiltinProc::Mul => {
                let [a, b]: [i64; 2] = self.pop_n();
                Data::Int(a * b)
            }
            BuiltinProc::Div => {
                let [a, b]: [i64; 2] = self.pop_n();
                Data::Int(a / b)
            }
            BuiltinProc::Append => {
                let [a, b]: [String; 2] = self.pop_n();
                Data::String(a + b)
            }
            BuiltinProc::NewMap => Data::Map([].into()),
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

    fn map_local_mut(&mut self, local: Local) -> &mut HashMap<RelationId, Value> {
        match &mut self.local_mut(local).data {
            Data::Map(hash_map) => hash_map,
            _ => panic!("Value at {local:?} is not a map"),
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
        match self.data {
            Data::Int(n) => n,
            _ => panic!("not an integer"),
        }
    }
}

impl Cast<String> for Value {
    fn cast(self) -> String {
        match self.data {
            Data::String(s) => s,
            _ => panic!("not a string"),
        }
    }
}

struct Tracer;

impl VmDebug<ValueStack> for Tracer {
    fn tick(&mut self, vm: &AbstractVm, stack: &ValueStack) {
        debug!("   -> {:?}", stack.stack);
        debug!("{:?}", vm.pending_opcode());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        proc::{NParams, OpCode},
        value::Value,
        DefId,
    };

    use super::*;

    #[test]
    fn translate_map() {
        let mut lib = Lib::default();
        let proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewMap, DefId(42)),
                OpCode::TakeAttr(Local(0), RelationId(DefId(1))),
                OpCode::PutAttr(Local(1), RelationId(DefId(3))),
                OpCode::TakeAttr(Local(0), RelationId(DefId(2))),
                OpCode::PutAttr(Local(1), RelationId(DefId(4))),
                OpCode::Return(Local(1)),
            ],
        );

        let mut vm = Translator::new(&lib);
        let output = vm.trace_eval(
            proc,
            [Value::new(
                Data::Map(
                    [
                        (
                            RelationId(DefId(1)),
                            Value::new(Data::String("foo".into()), DefId(0)),
                        ),
                        (
                            RelationId(DefId(2)),
                            Value::new(Data::String("bar".into()), DefId(0)),
                        ),
                    ]
                    .into(),
                ),
                DefId(0),
            )],
        );

        let Data::Map(map) = output.data else {
            panic!();
        };
        let properties = map.keys().cloned().collect::<HashSet<_>>();
        assert_eq!(
            HashSet::from([RelationId(DefId(3)), RelationId(DefId(4))]),
            properties
        );
    }

    #[test]
    fn call_stack() {
        let mut lib = Lib::default();
        let double = lib.append_procedure(
            NParams(1),
            [
                OpCode::Clone(Local(0)),
                OpCode::CallBuiltin(BuiltinProc::Add, DefId(0)),
                OpCode::Return0,
            ],
        );
        let add_then_double = lib.append_procedure(
            NParams(2),
            [
                OpCode::CallBuiltin(BuiltinProc::Add, DefId(0)),
                OpCode::Call(double),
                OpCode::Return0,
            ],
        );
        let translate = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewMap, DefId(0)),
                OpCode::TakeAttr(Local(0), RelationId(DefId(1))),
                OpCode::Call(double),
                OpCode::PutAttr(Local(1), RelationId(DefId(4))),
                OpCode::TakeAttr(Local(0), RelationId(DefId(2))),
                OpCode::TakeAttr(Local(0), RelationId(DefId(3))),
                OpCode::Call(add_then_double),
                OpCode::PutAttr(Local(1), RelationId(DefId(5))),
                OpCode::Return(Local(1)),
            ],
        );

        let mut vm = Translator::new(&lib);
        let output = vm.trace_eval(
            translate,
            [Value::new(
                Data::Map(
                    [
                        (RelationId(DefId(1)), Value::new(Data::Int(333), DefId(0))),
                        (RelationId(DefId(2)), Value::new(Data::Int(10), DefId(0))),
                        (RelationId(DefId(3)), Value::new(Data::Int(11), DefId(0))),
                    ]
                    .into(),
                ),
                DefId(0),
            )],
        );

        let Data::Map(mut map) = output.data else {
            panic!();
        };
        let Data::Int(a) = map.remove(&RelationId(DefId(4))).unwrap().data else {
            panic!();
        };
        let Data::Int(b) = map.remove(&RelationId(DefId(5))).unwrap().data else {
            panic!();
        };
        assert_eq!(666, a);
        assert_eq!(42, b);
    }
}
