use std::{array, collections::BTreeMap};

use smartstring::alias::String;
use tracing::{debug, trace, Level};

use crate::{
    cast::Cast,
    ontology::Ontology,
    string_pattern::StringPattern,
    value::{Attribute, Data, PropertyId, Value, ValueDebug},
    vm::abstract_vm::{AbstractVm, Processor, VmDebug},
    vm::proc::{BuiltinProc, Local, Procedure},
    DefId,
};

use super::proc::{PatternCaptureGroup, Predicate};

/// Virtual machine for executing ONTOL procedures
pub struct OntolVm<'l> {
    abstract_vm: AbstractVm<'l, OntolProcessor>,
    processor: OntolProcessor,
}

impl<'o> OntolVm<'o> {
    pub fn new(ontology: &'o Ontology) -> Self {
        Self {
            abstract_vm: AbstractVm::new(ontology),
            processor: OntolProcessor::default(),
        }
    }

    pub fn eval(&mut self, proc: Procedure, params: impl IntoIterator<Item = Value>) -> Value {
        self.processor.stack.extend(params);

        if tracing::enabled!(Level::TRACE) {
            self.internal_eval(proc, &mut Tracer)
        } else {
            self.internal_eval(proc, &mut ())
        }
    }

    #[inline(never)]
    pub fn internal_eval(
        &mut self,
        procedure: Procedure,
        debug: &mut dyn VmDebug<OntolProcessor>,
    ) -> Value {
        debug!("evaluating {procedure:?}");

        self.abstract_vm
            .execute(procedure, &mut self.processor, debug);

        let value_stack = std::mem::take(&mut self.processor);
        value_stack.stack.into_iter().next().unwrap()
    }
}

#[derive(Default)]
pub struct OntolProcessor {
    stack: Vec<Value>,
}

impl Processor for OntolProcessor {
    type Value = Value;

    #[inline(always)]
    fn size(&self) -> usize {
        self.stack.len()
    }

    #[inline(always)]
    fn stack_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.stack
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
    fn bump(&mut self, source: Local) {
        let last = self.stack.len();
        self.stack.push(Value::unit());
        self.stack.swap(source.0 as usize, last);
    }

    #[inline(always)]
    fn pop_until(&mut self, local: Local) {
        self.stack.truncate(local.0 as usize + 1);
    }

    #[inline(always)]
    fn swap(&mut self, a: Local, b: Local) {
        self.stack.swap(a.0 as usize, b.0 as usize);
    }

    #[inline(always)]
    fn iter_next(&mut self, seq: Local, index: Local) -> bool {
        let i = *self.int_local_mut(index) as usize;
        let seq = self.sequence_local_mut(seq);

        if seq.len() <= i {
            false
        } else {
            let mut attr = Value::unit().to_unit_attr();
            std::mem::swap(&mut seq[i], &mut attr);

            self.stack.push(attr.rel_params);
            self.stack.push(attr.value);

            *self.int_local_mut(index) += 1;

            true
        }
    }

    #[inline(always)]
    fn take_attr2(&mut self, source: Local, key: PropertyId) {
        let map = self.struct_local_mut(source);
        match map.remove(&key) {
            Some(attribute) => {
                self.stack.push(attribute.rel_params);
                self.stack.push(attribute.value);
            }
            None => {
                panic!("Attribute {key} not present");
            }
        }
    }

    #[inline(always)]
    fn try_take_attr2(&mut self, source: Local, key: PropertyId) {
        let map = self.struct_local_mut(source);
        match map.remove(&key) {
            Some(attribute) => {
                self.push_true();
                self.stack.push(attribute.rel_params);
                self.stack.push(attribute.value);
            }
            None => {
                self.push_false();
            }
        }
    }

    #[inline(always)]
    fn put_attr1(&mut self, target: Local, key: PropertyId) {
        let value = self.stack.pop().unwrap();
        if !matches!(value.data, Data::Unit) {
            let map = self.struct_local_mut(target);
            map.insert(key, value.to_unit_attr());
        }
    }

    #[inline(always)]
    fn put_attr2(&mut self, target: Local, key: PropertyId) {
        let [rel_params, value]: [Value; 2] = self.pop_n();
        if !matches!(value.data, Data::Unit) {
            let map = self.struct_local_mut(target);
            map.insert(key, Attribute { value, rel_params });
        }
    }

    #[inline(always)]
    fn push_i64(&mut self, k: i64, result_type: DefId) {
        self.stack.push(Value::new(Data::I64(k), result_type));
    }

    #[inline(always)]
    fn push_f64(&mut self, k: f64, result_type: DefId) {
        self.stack.push(Value::new(Data::F64(k), result_type));
    }

    #[inline(always)]
    fn push_string(&mut self, k: &str, result_type: DefId) {
        self.stack
            .push(Value::new(Data::String(k.into()), result_type));
    }

    #[inline(always)]
    fn append_attr2(&mut self, seq: Local) {
        let [rel_params, value]: [Value; 2] = self.pop_n();
        let seq = self.sequence_local_mut(seq);
        seq.push(Attribute { value, rel_params });
    }

    #[inline(always)]
    fn cond_predicate(&mut self, predicate: &Predicate) -> bool {
        match predicate {
            Predicate::MatchesDiscriminant(local, def_id) => {
                let value = self.local(*local);
                value.type_def_id == *def_id
            }
            Predicate::YankTrue(local) => !matches!(self.yank(*local).data, Data::I64(0)),
            Predicate::YankFalse(local) => matches!(self.yank(*local).data, Data::I64(0)),
        }
    }

    #[inline(always)]
    fn type_pun(&mut self, local: Local, def_id: DefId) {
        self.local_mut(local).type_def_id = def_id;
    }

    fn regex_capture(
        &mut self,
        local: Local,
        string_pattern: &StringPattern,
        groups: &[PatternCaptureGroup],
    ) {
        let Data::String(haystack) = &self.local(local).data else {
            panic!("Not a string");
        };
        match string_pattern.regex.captures(haystack) {
            Some(captures) => {
                let tmp_stack: Vec<Value> = groups
                    .iter()
                    .map(|group| {
                        let capture_match = captures
                            .get(group.group_index)
                            .expect("Capture group undefined");

                        Value::new(
                            Data::String(capture_match.as_str().into()),
                            group.type_def_id,
                        )
                    })
                    .collect();

                self.push_true();
                self.stack.extend(tmp_stack);
            }
            None => {
                self.push_false();
            }
        }
    }

    #[inline(always)]
    fn assert_true(&mut self) {
        let [val]: [Value; 1] = self.pop_n();
        if !matches!(val.data, Data::I64(1)) {
            panic!("Assertion failed");
        }
    }
}

impl OntolProcessor {
    fn eval_builtin(&mut self, proc: BuiltinProc) -> Data {
        match proc {
            BuiltinProc::Add => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::I64(a + b)
            }
            BuiltinProc::Sub => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::I64(a - b)
            }
            BuiltinProc::Mul => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::I64(a * b)
            }
            BuiltinProc::Div => {
                let [b, a]: [i64; 2] = self.pop_n();
                Data::I64(a / b)
            }
            BuiltinProc::Append => {
                let [b, a]: [String; 2] = self.pop_n();
                Data::String(a + b)
            }
            BuiltinProc::NewStruct => Data::Struct([].into()),
            BuiltinProc::NewSeq => Data::Sequence(vec![]),
            BuiltinProc::NewUnit => Data::Unit,
        }
    }

    #[inline(always)]
    fn local(&self, local: Local) -> &Value {
        &self.stack[local.0 as usize]
    }

    #[inline(always)]
    fn local_mut(&mut self, local: Local) -> &mut Value {
        &mut self.stack[local.0 as usize]
    }

    #[inline(always)]
    fn int_local_mut(&mut self, local: Local) -> &mut i64 {
        match &mut self.local_mut(local).data {
            Data::I64(int) => int,
            _ => panic!("Value at {local:?} is not an int"),
        }
    }

    #[inline(always)]
    fn struct_local_mut(&mut self, local: Local) -> &mut BTreeMap<PropertyId, Attribute> {
        match &mut self.local_mut(local).data {
            Data::Struct(attrs) => attrs,
            _ => panic!("Value at {local:?} is not a map"),
        }
    }

    #[inline(always)]
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
    #[inline(always)]
    fn pop_n<T, const N: usize>(&mut self) -> [T; N]
    where
        Value: Cast<T>,
    {
        array::from_fn(|_| self.pop_one().cast_into())
    }

    #[inline(always)]
    fn yank(&mut self, local: Local) -> Value {
        self.stack.remove(local.0 as usize)
    }

    #[inline(always)]
    fn push_true(&mut self) {
        self.stack.push(Value::new(Data::I64(1), DefId::unit()));
    }

    #[inline(always)]
    fn push_false(&mut self) {
        self.stack.push(Value::new(Data::I64(0), DefId::unit()));
    }
}

struct Tracer;

impl VmDebug<OntolProcessor> for Tracer {
    fn tick(&mut self, vm: &AbstractVm<OntolProcessor>, stack: &OntolProcessor) {
        if tracing::enabled!(Level::TRACE) {
            for (index, value) in stack.stack.iter().enumerate() {
                trace!("    Local({index}): {}", ValueDebug(value));
            }
        }
        trace!("{:?}", vm.pending_opcode());
    }
}

#[cfg(test)]
mod tests {
    use fnv::FnvHashSet;
    use test_log::test;

    use crate::{
        ontology::Ontology,
        value::Value,
        vm::proc::{AddressOffset, Lib, NParams, OpCode},
        DefId, PackageId,
    };

    use super::*;

    fn def_id(n: u16) -> DefId {
        DefId(PackageId(0), n)
    }

    #[test]
    fn map_struct() {
        let mut lib = Lib::default();
        let proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(42)),
                OpCode::TakeAttr2(Local(0), "S:0:1".parse().unwrap()),
                OpCode::PutAttr1(Local(1), "S:0:3".parse().unwrap()),
                OpCode::TakeAttr2(Local(0), "S:0:2".parse().unwrap()),
                OpCode::PutAttr1(Local(1), "S:0:4".parse().unwrap()),
                OpCode::Return(Local(1)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let mut vm = ontology.new_vm();
        let output = vm.eval(
            proc,
            [Value::new(
                Data::Struct(
                    [
                        (
                            "S:0:1".parse().unwrap(),
                            Value::new(Data::String("foo".into()), def_id(0)).into(),
                        ),
                        (
                            "S:0:2".parse().unwrap(),
                            Value::new(Data::String("bar".into()), def_id(0)).into(),
                        ),
                    ]
                    .into(),
                ),
                def_id(0),
            )],
        );

        let Data::Struct(attrs) = output.data else {
            panic!();
        };
        let properties = attrs.keys().cloned().collect::<FnvHashSet<_>>();
        assert_eq!(
            FnvHashSet::from_iter(["S:0:3".parse().unwrap(), "S:0:4".parse().unwrap(),]),
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
        let mapping_proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(0)),
                // 2, 3:
                OpCode::TakeAttr2(Local(0), "S:0:1".parse().unwrap()),
                OpCode::Call(double),
                OpCode::PutAttr1(Local(1), "S:0:4".parse().unwrap()),
                // 3, 4:
                OpCode::TakeAttr2(Local(0), "S:0:2".parse().unwrap()),
                // 5, 6:
                OpCode::TakeAttr2(Local(0), "S:0:3".parse().unwrap()),
                OpCode::Clone(Local(4)),
                // pop(6, 7):
                OpCode::Call(add_then_double),
                OpCode::PutAttr1(Local(1), "S:0:5".parse().unwrap()),
                OpCode::Return(Local(1)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let mut vm = ontology.new_vm();
        let output = vm.eval(
            mapping_proc,
            [Value::new(
                Data::Struct(
                    [
                        (
                            "S:0:1".parse().unwrap(),
                            Value::new(Data::I64(333), def_id(0)).into(),
                        ),
                        (
                            "S:0:2".parse().unwrap(),
                            Value::new(Data::I64(10), def_id(0)).into(),
                        ),
                        (
                            "S:0:3".parse().unwrap(),
                            Value::new(Data::I64(11), def_id(0)).into(),
                        ),
                    ]
                    .into(),
                ),
                def_id(0),
            )],
        );

        let Data::Struct(mut attrs) = output.data else {
            panic!();
        };
        let Data::I64(a) = attrs.remove(&"S:0:4".parse().unwrap()).unwrap().value.data else {
            panic!();
        };
        let Data::I64(b) = attrs.remove(&"S:0:5".parse().unwrap()).unwrap().value.data else {
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
                OpCode::CallBuiltin(BuiltinProc::NewSeq, def_id(0)),
                // index counter
                OpCode::I64(0, def_id(0)),
                // Offset(2): for each in Local(0)
                OpCode::Iter(Local(0), Local(2), AddressOffset(4)),
                OpCode::Return(Local(1)),
                // Offset(4): map item
                OpCode::I64(2, def_id(0)),
                OpCode::CallBuiltin(BuiltinProc::Mul, def_id(0)),
                // add rel params
                OpCode::CallBuiltin(BuiltinProc::NewUnit, def_id(0)),
                // pop (rel_params, value), append to sequence
                OpCode::AppendAttr2(Local(1)),
                OpCode::Goto(AddressOffset(2)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let mut vm = ontology.new_vm();
        let output = vm.eval(
            proc,
            [Value::new(
                Data::Sequence(vec![
                    Value::new(Data::I64(1), def_id(0)).into(),
                    Value::new(Data::I64(2), def_id(0)).into(),
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

    #[test]
    fn flat_map_object() {
        let mut lib = Lib::default();

        let prop_a: PropertyId = "S:0:0".parse().unwrap();
        let prop_b: PropertyId = "S:0:1".parse().unwrap();

        let proc = lib.append_procedure(
            NParams(1),
            [
                // a -> Local(2):
                OpCode::TakeAttr2(Local(0), prop_a),
                // [b] -> Local(4):
                OpCode::TakeAttr2(Local(0), prop_b),
                // counter -> Local(5):
                OpCode::I64(0, def_id(0)),
                // output -> Local(6):
                OpCode::CallBuiltin(BuiltinProc::NewSeq, def_id(0)),
                OpCode::Iter(Local(4), Local(5), AddressOffset(6)),
                OpCode::Return(Local(6)),
                // Loop
                // New object -> Local(9)
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(0)),
                OpCode::Clone(Local(2)),
                OpCode::PutAttr1(Local(9), prop_a),
                OpCode::Bump(Local(8)),
                OpCode::PutAttr1(Local(9), prop_b),
                OpCode::Bump(Local(7)),
                OpCode::AppendAttr2(Local(6)),
                OpCode::PopUntil(Local(6)),
                OpCode::Goto(AddressOffset(4)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let mut vm = ontology.new_vm();
        let output = vm.eval(
            proc,
            [Value::new(
                Data::Struct(
                    [
                        (
                            prop_a,
                            Value::new(Data::String("a".into()), def_id(0)).into(),
                        ),
                        (
                            prop_b,
                            Value::new(
                                Data::Sequence(vec![
                                    Value::new(Data::String("b0".into()), def_id(0)).into(),
                                    Value::new(Data::String("b1".into()), def_id(0)).into(),
                                ]),
                                def_id(0),
                            )
                            .into(),
                        ),
                    ]
                    .into(),
                ),
                def_id(0),
            )],
        );

        assert_eq!(
            "[{S:0:0 -> 'a', S:0:1 -> 'b0'}, {S:0:0 -> 'a', S:0:1 -> 'b1'}]",
            format!("{}", ValueDebug(&output))
        );
    }

    #[test]
    fn discriminant_cond() {
        let mut lib = Lib::default();

        let prop: PropertyId = "S:0:42".parse().unwrap();
        let inner_def_id = def_id(100);

        let proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(7)),
                OpCode::TryTakeAttr2(Local(0), prop),
                OpCode::Cond(Predicate::YankTrue(Local(2)), AddressOffset(4)),
                // AddressOffset(3):
                OpCode::Return(Local(1)),
                // AddressOffset(4):
                OpCode::Cond(
                    Predicate::MatchesDiscriminant(Local(3), inner_def_id),
                    AddressOffset(6),
                ),
                OpCode::Goto(AddressOffset(3)),
                // AddressOffset(6):
                OpCode::I64(666, def_id(200)),
                OpCode::PutAttr1(Local(1), prop),
                OpCode::Goto(AddressOffset(3)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let mut vm = ontology.new_vm();

        assert_eq!(
            "{}",
            format!(
                "{}",
                ValueDebug(&vm.eval(proc, [Value::new(Data::Struct([].into()), def_id(0))]))
            )
        );

        assert_eq!(
            "{}",
            format!(
                "{}",
                ValueDebug(&vm.eval(
                    proc,
                    [Value::new(
                        Data::Struct([(prop, Value::unit().into())].into()),
                        def_id(0)
                    )]
                ))
            )
        );

        assert_eq!(
            "{S:0:42 -> int(666)}",
            format!(
                "{}",
                ValueDebug(
                    &vm.eval(
                        proc,
                        [Value::new(
                            Data::Struct(
                                [(
                                    prop,
                                    Value::new(Data::String("a".into()), inner_def_id).into(),
                                )]
                                .into()
                            ),
                            def_id(0)
                        )]
                    )
                )
            )
        );
    }
}
