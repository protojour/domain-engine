use std::array;

use bit_vec::BitVec;
use fnv::FnvHashMap;
use regex_automata::{util::captures::Captures, Input};
use smallvec::SmallVec;
use smartstring::alias::String;
use thin_vec::ThinVec;
use tracing::{error, trace, Level};

use crate::{
    attr::{Attr, AttrMatrix},
    cast::Cast,
    debug::OntolDebug,
    ontology::{ontol::TextConstant, Ontology},
    property::ValueCardinality,
    query::condition::{Clause, ClausePair, CondTerm},
    sequence::Sequence,
    tuple::{EndoTuple, EndoTupleElements},
    value::{Value, ValueDebug, ValueTag},
    var::Var,
    vm::{
        abstract_vm::{AbstractVm, Processor, VmDebug},
        proc::{BuiltinProc, Local, Procedure},
        VmError,
    },
    DefId, OntolDefTag, PropId,
};

use super::{
    proc::{GetAttrFlags, OpCodeCondTerm, Predicate, Yield},
    VmResult, VmState,
};

/// Virtual machine for executing ONTOL procedures
pub struct OntolVm<'o> {
    abstract_vm: AbstractVm<'o, OntolProcessor<'o>>,
    processor: OntolProcessor<'o>,
}

impl<'o> OntolVm<'o> {
    pub fn new(ontology: &'o Ontology, proc: Procedure) -> Self {
        Self {
            abstract_vm: AbstractVm::new(ontology, proc),
            processor: OntolProcessor {
                stack: Default::default(),
                ontology,
            },
        }
    }

    pub fn run(
        &mut self,
        params: impl IntoIterator<Item = Value>,
    ) -> VmResult<VmState<Value, Yield>> {
        self.processor.stack.extend(params);

        let result = if tracing::enabled!(Level::TRACE) {
            let ontology = self.processor.ontology;
            self.abstract_vm
                .run(&mut self.processor, &mut Tracer { ontology })?
        } else {
            self.abstract_vm.run(&mut self.processor, &mut ())?
        };

        match result {
            Some(y) => Ok(VmState::Yield(y)),
            None => {
                let mut stack = std::mem::take(&mut self.processor.stack);
                Ok(VmState::Complete(stack.pop().unwrap()))
            }
        }
    }
}

pub struct OntolProcessor<'o> {
    stack: Vec<Value>,
    ontology: &'o Ontology,
}

impl<'o> Processor for OntolProcessor<'o> {
    type Value = Value;
    type Yield = Yield;

    #[inline(always)]
    fn stack_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.stack
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
    fn call_builtin(&mut self, proc: BuiltinProc, result_type: DefId) -> VmResult<()> {
        let value = self.eval_builtin(proc, result_type.into());
        self.stack.push(value);
        Ok(())
    }

    #[inline(always)]
    fn iter_next(&mut self, start_column: Local, n: u8, index: Local) -> VmResult<bool> {
        let i = *self.int_local_mut(index)? as usize;

        let mut result = true;

        for n in 0..(n as u16) {
            let column_local = Local(start_column.0 + n);

            match self.local_mut(column_local) {
                Value::Sequence(seq, _) => {
                    if seq.elements.len() <= i {
                        trace!("ending iteration on tuple {n}");

                        result = false;
                    } else {
                        // TODO(optimize): Figure out when clone is needed!
                        let value = seq.elements[i].clone();

                        self.stack.push(value);
                    }
                }
                Value::Void(_) => {
                    // Yields one #void tuple, then stops
                    if i == 0 {
                        self.push_void();
                    } else {
                        result = false;
                    }
                }
                _ => return Err(VmError::InvalidType(column_local)),
            }
        }

        *self.int_local_mut(index)? += 1;

        Ok(result)
    }

    #[inline(always)]
    fn get_attr(
        &mut self,
        source: Local,
        key: PropId,
        arity: u8,
        flags: super::proc::GetAttrFlags,
    ) -> VmResult<()> {
        let _struct = self.struct_local_mut(source)?;
        let attr = if flags.contains(GetAttrFlags::TAKE) {
            _struct.remove(&key)
        } else {
            _struct.get(&key).cloned()
        };
        match attr {
            Some(attr) => {
                match attr {
                    Attr::Unit(value) if arity == 1 => {
                        self.stack.push(value);
                    }
                    Attr::Tuple(tuple) if tuple.elements.len() == arity as usize => {
                        for elem in tuple.elements.into_iter() {
                            self.stack.push(elem);
                        }
                    }
                    Attr::Matrix(mat) if mat.columns.len() == arity as usize => {
                        for elem in mat.columns.into_iter() {
                            self.stack.push(Value::Sequence(elem, ValueTag::unit()));
                        }
                    }
                    attr => {
                        error!("attr was: {}, flags was {flags:?}", ValueDebug(&attr));
                        return Err(VmError::InvalidType(source));
                    }
                }

                Ok(())
            }
            None => {
                for _ in 0..arity {
                    self.push_void();
                }
                Ok(())
            }
        }
    }

    #[inline(always)]
    fn put_attr_unit(&mut self, target: Local, key: PropId) -> VmResult<()> {
        let value = self.stack.pop().unwrap();
        if !matches!(value, Value::Unit(_) | Value::Void(_)) {
            match &mut self.stack[target.0 as usize] {
                Value::Struct(attrs, _) => {
                    attrs.insert(key, Attr::Unit(value));
                }
                Value::Filter(filter, _) => {
                    let relationship = key.0;

                    if relationship == OntolDefTag::Order.def_id() {
                        filter.set_order(value);
                    } else if relationship == OntolDefTag::Direction.def_id() {
                        filter
                            .set_direction(value)
                            .map_err(|_| VmError::InvalidDirection)?;
                    } else {
                        return Err(VmError::InvalidType(target));
                    }
                }
                _ => return Err(VmError::InvalidType(target)),
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn put_attr_tuple(&mut self, target: Local, arity: u8, key: PropId) -> VmResult<()> {
        let mut elements: SmallVec<Value, 1> = SmallVec::with_capacity(arity as usize);

        for _ in 0..arity {
            elements.push(self.pop_one());
        }

        let map = self.struct_local_mut(target)?;
        map.insert(key, Attr::Tuple(Box::new(EndoTuple { elements })));

        Ok(())
    }

    #[inline(always)]
    fn put_attr_matrix(&mut self, target: Local, arity: u8, key: PropId) -> VmResult<()> {
        let mut columns: EndoTupleElements<Sequence<Value>> =
            EndoTupleElements::with_capacity(arity as usize);

        for _ in 0..arity {
            let Value::Sequence(sequence, _) = self.pop_one() else {
                return Err(VmError::InvalidMatrixColumn);
            };

            columns.push(sequence);
        }

        match &mut self.stack[target.0 as usize] {
            Value::Struct(attrs, _) => {
                attrs.insert(key, Attr::Matrix(AttrMatrix { columns }));
            }
            Value::Filter(filter, _) => {
                let relationship = key.0;

                if relationship == OntolDefTag::Order.def_id() && arity == 1 {
                    filter.set_order(Value::Sequence(
                        columns.into_iter().next().unwrap(),
                        ValueTag::unit(),
                    ));
                } else {
                    return Err(VmError::InvalidType(target));
                }
            }
            _ => return Err(VmError::InvalidType(target)),
        }

        Ok(())
    }

    #[inline(always)]
    fn move_rest_attrs(&mut self, target: Local, source: Local) -> VmResult<()> {
        let source = std::mem::take(self.struct_local_mut(source)?);
        let target = self.struct_local_mut(target)?;
        target.extend(source);
        Ok(())
    }

    #[inline(always)]
    fn push_i64(&mut self, k: i64, result_type: DefId) -> VmResult<()> {
        self.stack.push(Value::I64(k, result_type.into()));
        Ok(())
    }

    #[inline(always)]
    fn push_f64(&mut self, k: f64, result_type: DefId) -> VmResult<()> {
        self.stack.push(Value::F64(k, result_type.into()));
        Ok(())
    }

    #[inline(always)]
    fn push_string(&mut self, k: TextConstant, result_type: DefId) -> VmResult<()> {
        let str = &self.ontology[k];
        self.stack.push(Value::Text(str.into(), result_type.into()));
        Ok(())
    }

    #[inline(always)]
    fn seq_append_n(&mut self, seq: Local, len: u8) -> VmResult<()> {
        for i in (0..len as u16).rev() {
            let value = self.pop_one();

            let seq_local = Local(seq.0 + i);
            let seq = self.sequence_local_mut(seq_local)?;

            seq.elements.push(value);
        }

        Ok(())
    }

    #[inline(always)]
    fn append_string(&mut self, to: Local) -> VmResult<()> {
        let [appendee]: [String; 1] = self.pop_n();
        let to = self.string_local_mut(to)?;
        to.push_str(&appendee);
        Ok(())
    }

    #[inline(always)]
    fn cond_predicate(&mut self, predicate: &Predicate) -> VmResult<bool> {
        match predicate {
            Predicate::IsVoid(local) => Ok(matches!(self.local(*local), Value::Void(_))),
            Predicate::IsNotVoid(local) => Ok(!matches!(self.local(*local), Value::Void(_))),
            Predicate::NotMatchesDiscriminant(local, def_id) => {
                let value = self.local(*local);
                Ok(value.type_def_id() != *def_id)
            }
            Predicate::MatchesDiscriminant(local, def_id) => {
                let value = self.local(*local);
                Ok(value.type_def_id() == *def_id)
            }
        }
    }

    fn move_seq_vals_to_stack(&mut self, source: Local) -> VmResult<()> {
        let sequence = std::mem::take(&mut self.sequence_local_mut(source)?.elements);
        *self.local_mut(source) = Value::unit();
        for value in sequence.into_iter() {
            self.stack.push(value);
        }

        Ok(())
    }

    fn set_sub_seq(&mut self, target: Local, source: Local) -> VmResult<()> {
        let sub_seq = self.sequence_local_mut(source)?.clone_sub();
        self.sequence_local_mut(target)?.sub_seq = sub_seq;
        Ok(())
    }

    #[inline(always)]
    fn type_pun(&mut self, local: Option<Local>, def_id: DefId) -> VmResult<()> {
        if let Some(local) = local {
            self.local_mut(local).tag_mut().set_def_id(def_id);
        } else {
            self.stack.last_mut().unwrap().tag_mut().set_def_id(def_id);
        }
        Ok(())
    }

    fn regex_capture(
        &mut self,
        local: Local,
        pattern_id: DefId,
        group_filter: &BitVec,
    ) -> VmResult<()> {
        let Value::Text(haystack, _) = &self.local(local) else {
            return Err(VmError::InvalidType(local));
        };

        let text_pattern = self.ontology.get_text_pattern(pattern_id).unwrap();

        let mut captures = text_pattern.regex.create_captures();
        text_pattern.regex.captures(haystack, &mut captures);
        if captures.is_match() {
            let attributes = extract_regex_captures(
                haystack,
                &captures,
                group_filter,
                OntolDefTag::Text.def_id().into(),
            );
            self.stack.push(Value::Sequence(
                attributes.into(),
                OntolDefTag::Text.def_id().into(),
            ));
        } else {
            self.push_void();
        }
        Ok(())
    }

    fn regex_capture_iter(
        &mut self,
        local: Local,
        pattern_id: DefId,
        group_filter: &BitVec,
    ) -> VmResult<()> {
        let Value::Text(haystack, _) = &self.local(local) else {
            return Err(VmError::InvalidType(local));
        };

        let text_pattern = self.ontology.get_text_pattern(pattern_id).unwrap();

        let mut values: ThinVec<Value> = ThinVec::new();
        let text_tag: ValueTag = OntolDefTag::Text.def_id().into();

        for captures in text_pattern
            .regex
            // Operates in non-greedy mode with `earliest(true)`
            .captures_iter(Input::new(haystack).earliest(true))
        {
            let value_attributes =
                extract_regex_captures(haystack, &captures, group_filter, text_tag);
            values.push(Value::Sequence(value_attributes.into(), text_tag));
        }

        self.stack.push(Value::Sequence(values.into(), text_tag));
        Ok(())
    }

    fn cond_var(&mut self, cond_local: Local) -> VmResult<()> {
        let Value::Filter(filter, _) = &mut self.local_mut(cond_local) else {
            return Err(VmError::InvalidType(cond_local));
        };

        let cond_var = filter.condition_mut().mk_cond_var();

        self.stack_mut()
            .push(Value::I64(cond_var.0 as i64, ValueTag::unit()));

        Ok(())
    }

    fn push_cond_clause(
        &mut self,
        filter_local: Local,
        input: &ClausePair<Local, OpCodeCondTerm>,
    ) -> VmResult<()> {
        let var = self.var_local(input.0)?;
        let evaluated_clause: Clause<Var, CondTerm> = match &input.1 {
            Clause::Root => Clause::Root,
            Clause::IsDef(def_id) => Clause::IsDef(*def_id),
            Clause::MatchProp(prop_id, operator, set_local) => {
                Clause::MatchProp(*prop_id, *operator, self.var_local(*set_local)?)
            }
            Clause::Member(rel, val) => {
                let rel = self.opcode_term_to_cond_term(rel)?;
                let val = self.opcode_term_to_cond_term(val)?;
                Clause::Member(rel, val)
            }
        };

        let Value::Filter(filter, _) = &mut self.local_mut(filter_local) else {
            panic!();
        };
        filter.condition_mut().add_clause(var, evaluated_clause);
        Ok(())
    }

    fn yield_match_condition(
        &mut self,
        var: Var,
        value_cardinality: ValueCardinality,
    ) -> VmResult<Self::Yield> {
        match self.stack.pop().unwrap() {
            Value::Filter(filter, _) => Ok(Yield::Match(var, value_cardinality, *filter)),
            _ => Err(VmError::InvalidType(Local(self.stack.len() as u16))),
        }
    }

    fn yield_call_extern(
        &mut self,
        extern_def_id: DefId,
        output_def_id: DefId,
    ) -> VmResult<Self::Yield> {
        Ok(Yield::CallExtern(
            extern_def_id,
            self.stack.pop().unwrap(),
            output_def_id,
        ))
    }
}

impl<'o> OntolProcessor<'o> {
    fn eval_builtin(&mut self, proc: BuiltinProc, tag: ValueTag) -> Value {
        match proc {
            BuiltinProc::AddI64 => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a + b, tag)
            }
            BuiltinProc::SubI64 => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a - b, tag)
            }
            BuiltinProc::MulI64 => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a * b, tag)
            }
            BuiltinProc::DivI64 => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a / b, tag)
            }
            BuiltinProc::AddF64 => {
                let [b, a]: [f64; 2] = self.pop_n();
                Value::F64(a + b, tag)
            }
            BuiltinProc::SubF64 => {
                let [b, a]: [f64; 2] = self.pop_n();
                Value::F64(a - b, tag)
            }
            BuiltinProc::MulF64 => {
                let [b, a]: [f64; 2] = self.pop_n();
                Value::F64(a * b, tag)
            }
            BuiltinProc::DivF64 => {
                let [b, a]: [f64; 2] = self.pop_n();
                Value::F64(a / b, tag)
            }
            BuiltinProc::Append => {
                let [b, a]: [String; 2] = self.pop_n();
                Value::Text(a + b, tag)
            }
            BuiltinProc::NewStruct => Value::Struct(Default::default(), tag),
            BuiltinProc::NewSeq => Value::Sequence(Default::default(), tag),
            BuiltinProc::NewUnit => Value::Unit(tag),
            BuiltinProc::NewFilter => Value::Filter(Default::default(), tag),
            BuiltinProc::NewVoid => Value::Void(tag),
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
    fn int_local_mut(&mut self, local: Local) -> VmResult<&mut i64> {
        match self.local_mut(local) {
            Value::I64(int, _) => Ok(int),
            _ => Err(VmError::InvalidType(local)),
        }
    }

    #[inline(always)]
    fn int_local(&self, local: Local) -> VmResult<i64> {
        match self.local(local) {
            Value::I64(int, _) => Ok(*int),
            _ => Err(VmError::InvalidType(local)),
        }
    }

    fn var_local(&self, local: Local) -> VmResult<Var> {
        let int = self.int_local(local)?;
        Ok(Var(int.try_into().map_err(|_| VmError::Overflow)?))
    }

    #[inline(always)]
    fn string_local_mut(&mut self, local: Local) -> VmResult<&mut String> {
        match self.local_mut(local) {
            Value::Text(string, _) => Ok(string),
            _ => Err(VmError::InvalidType(local)),
        }
    }

    #[inline(always)]
    fn struct_local_mut(&mut self, local: Local) -> VmResult<&mut FnvHashMap<PropId, Attr>> {
        match self.local_mut(local) {
            Value::Struct(attrs, _) => Ok(attrs.as_mut()),
            _ => Err(VmError::InvalidType(local)),
        }
    }

    #[inline(always)]
    fn sequence_local_mut(&mut self, local: Local) -> VmResult<&mut Sequence<Value>> {
        match self.local_mut(local) {
            Value::Sequence(seq, _) => Ok(seq),
            _ => Err(VmError::InvalidType(local)),
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
    fn push_void(&mut self) {
        self.stack.push(Value::Void(ValueTag::unit()));
    }

    fn opcode_term_to_cond_term(&mut self, term: &OpCodeCondTerm) -> VmResult<CondTerm> {
        match term {
            OpCodeCondTerm::Wildcard => Ok(CondTerm::Wildcard),
            OpCodeCondTerm::CondVar(local) => Ok(CondTerm::Variable(self.var_local(*local)?)),
            OpCodeCondTerm::Value(local) => {
                Ok(CondTerm::Value(self.stack[local.0 as usize].take()))
            }
        }
    }
}

struct Tracer<'on> {
    ontology: &'on Ontology,
}

impl<'on> VmDebug<OntolProcessor<'on>> for Tracer<'on> {
    fn tick(&mut self, vm: &AbstractVm<OntolProcessor>, stack: &OntolProcessor) {
        if tracing::enabled!(Level::TRACE) {
            for (index, value) in stack.stack.iter().enumerate() {
                trace!("    L{index}: {}", ValueDebug(value));
            }
        }
        trace!("{:?}", &vm.pending_opcode().debug(self.ontology));
    }
}

fn extract_regex_captures(
    haystack: &str,
    captures: &Captures,
    group_filter: &BitVec,
    text_tag: ValueTag,
) -> ThinVec<Value> {
    group_filter
        .iter()
        .enumerate()
        .filter_map(|(index, value)| if value { Some(index) } else { None })
        .map(|index| match captures.get_group(index) {
            Some(span) => Value::Text(haystack[span.start..span.end].into(), text_tag),
            None => Value::Void(ValueTag::unit()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use fnv::FnvHashSet;
    use ontol_macros::test;

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
                OpCode::GetAttr(Local(0), "p@0:0:1".parse().unwrap(), 1, GetAttrFlags::TAKE),
                OpCode::PutAttrUnit(Local(1), "p@0:0:3".parse().unwrap()),
                OpCode::GetAttr(Local(0), "p@0:0:2".parse().unwrap(), 1, GetAttrFlags::TAKE),
                OpCode::PutAttrUnit(Local(1), "p@0:0:4".parse().unwrap()),
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let output = OntolVm::new(&ontology, proc)
            .run([Value::new_struct(
                [
                    (
                        "p@0:0:1".parse().unwrap(),
                        Value::Text("foo".into(), ValueTag::unit()).into(),
                    ),
                    (
                        "p@0:0:2".parse().unwrap(),
                        Value::Text("bar".into(), ValueTag::unit()).into(),
                    ),
                ],
                ValueTag::unit(),
            )])
            .unwrap()
            .unwrap();

        let Value::Struct(attrs, _) = output else {
            panic!();
        };
        let properties = attrs.keys().cloned().collect::<FnvHashSet<_>>();
        assert_eq!(
            FnvHashSet::from_iter(["p@0:0:3".parse().unwrap(), "p@0:0:4".parse().unwrap(),]),
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
                OpCode::CallBuiltin(BuiltinProc::AddI64, def_id(0)),
                OpCode::Return,
            ],
        );
        let add_then_double = lib.append_procedure(
            NParams(2),
            [
                OpCode::CallBuiltin(BuiltinProc::AddI64, def_id(0)),
                OpCode::Call(double),
                OpCode::Return,
            ],
        );
        let mapping_proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(0)),
                // 2, 3:
                OpCode::GetAttr(Local(0), "p@0:0:1".parse().unwrap(), 1, GetAttrFlags::TAKE),
                OpCode::Call(double),
                OpCode::PutAttrUnit(Local(1), "p@0:0:4".parse().unwrap()),
                // 3, 4:
                OpCode::GetAttr(Local(0), "p@0:0:2".parse().unwrap(), 1, GetAttrFlags::TAKE),
                // 5, 6:
                OpCode::GetAttr(Local(0), "p@0:0:3".parse().unwrap(), 1, GetAttrFlags::TAKE),
                OpCode::Clone(Local(2)),
                // pop(6, 7):
                OpCode::Call(add_then_double),
                OpCode::PutAttrUnit(Local(1), "p@0:0:5".parse().unwrap()),
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let output = OntolVm::new(&ontology, mapping_proc)
            .run([Value::new_struct(
                [
                    (
                        "p@0:0:1".parse().unwrap(),
                        Value::I64(333, ValueTag::unit()).into(),
                    ),
                    (
                        "p@0:0:2".parse().unwrap(),
                        Value::I64(10, ValueTag::unit()).into(),
                    ),
                    (
                        "p@0:0:3".parse().unwrap(),
                        Value::I64(11, ValueTag::unit()).into(),
                    ),
                ],
                ValueTag::unit(),
            )])
            .unwrap()
            .unwrap();

        let Value::Struct(mut attrs, _) = output else {
            panic!();
        };
        let Value::I64(a, _) = attrs
            .remove(&"p@0:0:4".parse().unwrap())
            .unwrap()
            .unwrap_unit()
        else {
            panic!();
        };
        let Value::I64(b, _) = attrs
            .remove(&"p@0:0:5".parse().unwrap())
            .unwrap()
            .unwrap_unit()
        else {
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
                OpCode::Iter(Local(0), 1, Local(2), AddressOffset(5)),
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
                // Offset(4): map item
                OpCode::I64(2, def_id(0)),
                OpCode::CallBuiltin(BuiltinProc::MulI64, def_id(0)),
                // append value to matrix
                OpCode::SeqAppendN(Local(1), 1),
                OpCode::Goto(AddressOffset(2)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let output = OntolVm::new(&ontology, proc)
            .run([Value::sequence_of([
                Value::I64(1, ValueTag::unit()),
                Value::I64(2, ValueTag::unit()),
            ])])
            .unwrap()
            .unwrap();

        let Value::Sequence(seq, _) = output else {
            panic!();
        };
        let output = seq
            .elements
            .into_iter()
            .map(|value| value.cast_into())
            .collect::<Vec<i64>>();

        assert_eq!(vec![2, 4], output);
    }

    #[test]
    fn flat_map_object() {
        let mut lib = Lib::default();

        let prop_a: PropId = "p@0:0:0".parse().unwrap();
        let prop_b: PropId = "p@0:0:1".parse().unwrap();

        let proc = lib.append_procedure(
            NParams(1),
            [
                // a -> Local(1):
                OpCode::GetAttr(Local(0), prop_a, 1, GetAttrFlags::TAKE),
                // [b] -> Local(2):
                OpCode::GetAttr(Local(0), prop_b, 1, GetAttrFlags::TAKE),
                // counter -> Local(3):
                OpCode::I64(0, def_id(0)),
                // output -> Local(4):
                OpCode::CallBuiltin(BuiltinProc::NewSeq, def_id(0)),
                OpCode::Iter(Local(2), 1, Local(3), AddressOffset(7)),
                OpCode::PopUntil(Local(4)),
                OpCode::Return,
                // Loop
                // New object -> Local(6)
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(0)),
                OpCode::Clone(Local(1)),
                OpCode::PutAttrUnit(Local(6), prop_a),
                OpCode::Bump(Local(5)),
                OpCode::PutAttrUnit(Local(6), prop_b),
                OpCode::Bump(Local(6)),
                OpCode::SeqAppendN(Local(4), 1),
                OpCode::PopUntil(Local(4)),
                OpCode::Goto(AddressOffset(4)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let output = OntolVm::new(&ontology, proc)
            .run([Value::new_struct(
                [
                    (prop_a, Value::Text("a".into(), ValueTag::unit()).into()),
                    (
                        prop_b,
                        Value::sequence_of([
                            Value::Text("b0".into(), ValueTag::unit()),
                            Value::Text("b1".into(), ValueTag::unit()),
                        ])
                        .into(),
                    ),
                ],
                ValueTag::unit(),
            )])
            .unwrap()
            .unwrap();

        assert_eq!(
            "[{p@0:0:1 -> 'b0', p@0:0:0 -> 'a'}, {p@0:0:1 -> 'b1', p@0:0:0 -> 'a'}]",
            format!("{}", ValueDebug(&output))
        );
    }

    #[test]
    fn discriminant_cond() {
        let mut lib = Lib::default();

        let prop: PropId = "p@0:0:42".parse().unwrap();
        let inner_tag = ValueTag::from(def_id(100));

        let proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(7)),
                OpCode::GetAttr(Local(0), prop, 1, GetAttrFlags::TAKE),
                OpCode::Cond(Predicate::IsNotVoid(Local(2)), AddressOffset(5)),
                // AddressOffset(3):
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
                // AddressOffset(4):
                OpCode::Cond(
                    Predicate::MatchesDiscriminant(Local(2), inner_tag.into()),
                    AddressOffset(7),
                ),
                OpCode::Goto(AddressOffset(3)),
                // AddressOffset(7):
                OpCode::I64(666, def_id(200)),
                OpCode::PutAttrUnit(Local(1), prop),
                OpCode::Goto(AddressOffset(3)),
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();

        assert_eq!(
            "{}",
            format!(
                "{}",
                ValueDebug(
                    &OntolVm::new(&ontology, proc)
                        .run([Value::new_struct([], ValueTag::unit())])
                        .unwrap()
                        .unwrap()
                )
            )
        );

        assert_eq!(
            "{}",
            format!(
                "{}",
                ValueDebug(
                    &OntolVm::new(&ontology, proc)
                        .run([Value::new_struct(
                            [(prop, Value::unit().into())],
                            ValueTag::unit()
                        )])
                        .unwrap()
                        .unwrap()
                )
            )
        );

        assert_eq!(
            "{p@0:0:42 -> int(666)}",
            format!(
                "{}",
                ValueDebug(
                    &OntolVm::new(&ontology, proc)
                        .run([Value::new_struct(
                            [(prop, Value::Text("a".into(), inner_tag).into())],
                            ValueTag::unit()
                        )])
                        .unwrap()
                        .unwrap()
                )
            )
        );
    }
}
