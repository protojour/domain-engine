use std::{array, collections::BTreeMap};

use smartstring::alias::String;
use tracing::trace;

use crate::{
    cast::Cast,
    proc::{BuiltinProc, Lib, Local, Procedure},
    value::{Attribute, Data, PropertyId, Value},
    vm::{AbstractVm, Stack, VmDebug},
    DefId,
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
    #[inline(always)]
    fn size(&self) -> usize {
        self.stack.len()
    }

    #[inline(always)]
    fn local0_pos(&self) -> usize {
        self.local0_pos
    }

    #[inline(always)]
    fn local0_pos_mut(&mut self) -> &mut usize {
        &mut self.local0_pos
    }

    #[inline(always)]
    fn truncate(&mut self, n_locals: usize) {
        self.stack.truncate(self.local0_pos + n_locals);
    }

    #[inline(always)]
    fn call_builtin(&mut self, proc: BuiltinProc, result_type: DefId) {
        let data = self.eval_builtin(proc);
        self.stack.push(Value::new(data, result_type));
    }

    #[inline(always)]
    fn clone(&mut self, source: Local) {
        let value = self.local(source).clone();
        self.stack.push(value);
    }

    #[inline(always)]
    fn remove(&mut self, local: Local) {
        let stack_pos = self.local0_pos;
        self.stack.remove(stack_pos + local.0 as usize);
    }

    #[inline(always)]
    fn swap(&mut self, a: Local, b: Local) {
        let stack_pos = self.local0_pos;
        self.stack
            .swap(stack_pos + a.0 as usize, stack_pos + b.0 as usize);
    }

    #[inline(always)]
    fn for_each(&mut self, seq: Local, index: Local) -> bool {
        let i = *self.int_local_mut(index) as usize;
        let seq = self.sequence_local_mut(seq);

        if seq.len() <= i {
            false
        } else {
            let mut attr = Attribute::with_unit_params(Value::unit());
            std::mem::swap(&mut seq[i], &mut attr);

            self.stack.push(attr.rel_params);
            self.stack.push(attr.value);

            *self.int_local_mut(index) += 1;

            true
        }
    }

    #[inline(always)]
    fn take_attr_value(&mut self, source: Local, key: PropertyId) {
        let map = self.map_local_mut(source);
        let attribute = map.remove(&key).expect("Attribute not found");
        self.stack.push(attribute.value);
    }

    #[inline(always)]
    fn put_unit_attr(&mut self, target: Local, key: PropertyId) {
        let value = self.stack.pop().unwrap();
        let map = self.map_local_mut(target);
        map.insert(key, Attribute::with_unit_params(value));
    }

    #[inline(always)]
    fn push_constant(&mut self, k: i64, result_type: DefId) {
        self.stack.push(Value::new(Data::Int(k), result_type));
    }

    #[inline(always)]
    fn push_sequence(&mut self, result_type: DefId) {
        self.stack
            .push(Value::new(Data::Sequence(vec![]), result_type));
    }

    #[inline(always)]
    fn push_unit(&mut self) {
        self.stack.push(Value::unit());
    }

    fn append_attr(&mut self, seq: Local) {
        let [rel_params, value]: [Value; 2] = self.pop_n();
        let seq = self.sequence_local_mut(seq);

        seq.push(Attribute { value, rel_params });
    }
}

impl ValueStack {
    fn eval_builtin(&mut self, proc: BuiltinProc) -> Data {
        match proc {
            BuiltinProc::Add => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::Int(a + b)
            }
            BuiltinProc::Sub => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::Int(a - b)
            }
            BuiltinProc::Mul => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::Int(a * b)
            }
            BuiltinProc::Div => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::Int(a / b)
            }
            BuiltinProc::Append => {
                let [b, a]: [String; 2] = self.pop_n();
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

    fn int_local_mut(&mut self, local: Local) -> &mut i64 {
        match &mut self.local_mut(local).data {
            Data::Int(int) => int,
            _ => panic!("Value at {local:?} is not an int"),
        }
    }

    fn map_local_mut(&mut self, local: Local) -> &mut BTreeMap<PropertyId, Attribute> {
        match &mut self.local_mut(local).data {
            Data::Map(map) => map,
            _ => panic!("Value at {local:?} is not a map"),
        }
    }

    fn sequence_local_mut(&mut self, local: Local) -> &mut Vec<Attribute> {
        match &mut self.local_mut(local).data {
            Data::Sequence(seq) => seq,
            _ => panic!("Value at {local:?} is not a sequence"),
        }
    }

    #[inline(always)]
    fn pop_one(&mut self) -> Value {
        match self.stack.pop() {
            Some(value) => value,
            None => panic!("Nothing to pop"),
        }
    }

    /// Pop n items from stack (NB: returned in reverse order, top of stack is first array item)
    fn pop_n<T, const N: usize>(&mut self) -> [T; N]
    where
        Value: Cast<T>,
    {
        array::from_fn(|_| self.pop_one().cast_into())
    }
}

struct Tracer;

impl VmDebug<ValueStack> for Tracer {
    fn tick(&mut self, vm: &AbstractVm, stack: &ValueStack) {
        trace!("   -> {:?}", stack.stack);
        trace!("{:?}", vm.pending_opcode());
    }
}

#[cfg(test)]
mod tests {
    use fnv::FnvHashSet;
    use test_log::test;

    use crate::{
        proc::{AddressOffset, NParams, OpCode},
        value::{Attribute, Value},
        DefId, PackageId, RelationId,
    };

    use super::*;

    fn def_id(n: u16) -> DefId {
        DefId(PackageId(0), n)
    }

    #[test]
    fn translate_map() {
        let mut lib = Lib::default();
        let proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewMap, def_id(42)),
                OpCode::TakeAttrValue(Local(0), PropertyId::subject(RelationId(def_id(1)))),
                OpCode::PutUnitAttr(Local(1), PropertyId::subject(RelationId(def_id(3)))),
                OpCode::TakeAttrValue(Local(0), PropertyId::subject(RelationId(def_id(2)))),
                OpCode::PutUnitAttr(Local(1), PropertyId::subject(RelationId(def_id(4)))),
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
                            PropertyId::subject(RelationId(def_id(1))),
                            Attribute::with_unit_params(Value::new(
                                Data::String("foo".into()),
                                def_id(0),
                            )),
                        ),
                        (
                            PropertyId::subject(RelationId(def_id(2))),
                            Attribute::with_unit_params(Value::new(
                                Data::String("bar".into()),
                                def_id(0),
                            )),
                        ),
                    ]
                    .into(),
                ),
                def_id(0),
            )],
        );

        let Data::Map(map) = output.data else {
            panic!();
        };
        let properties = map.keys().cloned().collect::<FnvHashSet<_>>();
        assert_eq!(
            FnvHashSet::from_iter([
                PropertyId::subject(RelationId(def_id(3))),
                PropertyId::subject(RelationId(def_id(4)))
            ]),
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
                OpCode::CallBuiltin(BuiltinProc::Add, def_id(0)),
                OpCode::Return0,
            ],
        );
        let add_then_double = lib.append_procedure(
            NParams(2),
            [
                OpCode::CallBuiltin(BuiltinProc::Add, def_id(0)),
                OpCode::Call(double),
                OpCode::Return0,
            ],
        );
        let translate = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewMap, def_id(0)),
                OpCode::TakeAttrValue(Local(0), PropertyId::subject(RelationId(def_id(1)))),
                OpCode::Call(double),
                OpCode::PutUnitAttr(Local(1), PropertyId::subject(RelationId(def_id(4)))),
                OpCode::TakeAttrValue(Local(0), PropertyId::subject(RelationId(def_id(2)))),
                OpCode::TakeAttrValue(Local(0), PropertyId::subject(RelationId(def_id(3)))),
                OpCode::Call(add_then_double),
                OpCode::PutUnitAttr(Local(1), PropertyId::subject(RelationId(def_id(5)))),
                OpCode::Return(Local(1)),
            ],
        );

        let mut vm = Translator::new(&lib);
        let output = vm.trace_eval(
            translate,
            [Value::new(
                Data::Map(
                    [
                        (
                            PropertyId::subject(RelationId(def_id(1))),
                            Attribute::with_unit_params(Value::new(Data::Int(333), def_id(0))),
                        ),
                        (
                            PropertyId::subject(RelationId(def_id(2))),
                            Attribute::with_unit_params(Value::new(Data::Int(10), def_id(0))),
                        ),
                        (
                            PropertyId::subject(RelationId(def_id(3))),
                            Attribute::with_unit_params(Value::new(Data::Int(11), def_id(0))),
                        ),
                    ]
                    .into(),
                ),
                def_id(0),
            )],
        );

        let Data::Map(mut map) = output.data else {
            panic!();
        };
        let Data::Int(a) = map.remove(&PropertyId::subject(RelationId(def_id(4)))).unwrap().value.data else {
            panic!();
        };
        let Data::Int(b) = map.remove(&PropertyId::subject(RelationId(def_id(5)))).unwrap().value.data else {
            panic!();
        };
        assert_eq!(666, a);
        assert_eq!(42, b);
    }

    #[test]
    fn map_sequence() {
        let mut lib = Lib::default();

        let proc = lib.append_procedure(
            NParams(1),
            [
                // result sequence
                OpCode::PushSequence(def_id(0)),
                // index counter
                OpCode::PushConstant(0, def_id(0)),
                // Offset(2): for each in Local(0)
                OpCode::ForEach(Local(0), Local(2), AddressOffset(4)),
                OpCode::Return(Local(1)),
                // Offset(4): map item
                // remove rel params
                OpCode::Remove(Local(3)),
                OpCode::PushConstant(2, def_id(0)),
                OpCode::CallBuiltin(BuiltinProc::Mul, def_id(0)),
                // add rel params
                OpCode::PushUnit,
                // pop (rel_params, value), append to sequence
                OpCode::AppendAttr(Local(1)),
                OpCode::Goto(AddressOffset(2)),
            ],
        );

        let mut vm = Translator::new(&lib);
        let output = vm.trace_eval(
            proc,
            [Value::new(
                Data::Sequence(vec![
                    Attribute::with_unit_params(Value::new(Data::Int(1), def_id(0))),
                    Attribute::with_unit_params(Value::new(Data::Int(2), def_id(0))),
                ]),
                def_id(0),
            )],
        );

        let Data::Sequence(seq) = output.data else {
            panic!();
        };
        let output = seq
            .into_iter()
            .map(|attr| attr.value.cast_into())
            .collect::<Vec<i64>>();

        assert_eq!(vec![2, 4], output);
    }
}
