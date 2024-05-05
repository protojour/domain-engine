use std::array;

use bit_vec::BitVec;
use fnv::FnvHashMap;
use regex_automata::{util::captures::Captures, Input};
use smartstring::alias::String;
use thin_vec::ThinVec;
use tracing::{trace, Level};

use crate::{
    cast::Cast,
    ontology::{ontol::TextConstant, Ontology},
    property::{PropertyId, ValueCardinality},
    query::condition::{Clause, ClausePair, CondTerm},
    sequence::Sequence,
    value::{Attribute, Value, ValueDebug},
    var::Var,
    vm::proc::{BuiltinProc, Local, Procedure},
    vm::{
        abstract_vm::{AbstractVm, Processor, VmDebug},
        VmError,
    },
    DefId,
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
        let value = self.eval_builtin(proc, result_type);
        self.stack.push(value);
        Ok(())
    }

    #[inline(always)]
    fn iter_next(&mut self, seq: Local, index: Local) -> VmResult<bool> {
        let i = *self.int_local_mut(index)? as usize;

        match self.local_mut(seq) {
            Value::Sequence(seq, _) => {
                if seq.attrs.len() <= i {
                    Ok(false)
                } else {
                    // TODO(optimize): Figure out when clone is needed!
                    let attr = seq.attrs[i].clone();

                    self.stack.push(attr.rel);
                    self.stack.push(attr.val);

                    *self.int_local_mut(index)? += 1;

                    Ok(true)
                }
            }
            Value::Void(_) => {
                // Yields one #void item, then stops
                if i == 0 {
                    self.push_void();
                    self.push_void();
                    *self.int_local_mut(index)? += 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Err(VmError::InvalidType(seq)),
        }
    }

    #[inline(always)]
    fn get_attr(
        &mut self,
        source: Local,
        key: PropertyId,
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
                if flags.contains(GetAttrFlags::REL) {
                    self.stack.push(attr.rel);
                }
                if flags.contains(GetAttrFlags::VAL) {
                    self.stack.push(attr.val);
                }
                Ok(())
            }
            None => {
                if flags.contains(GetAttrFlags::REL) {
                    self.push_void();
                }
                if flags.contains(GetAttrFlags::VAL) {
                    self.push_void();
                }
                Ok(())
            }
        }
    }

    #[inline(always)]
    fn put_attr1(&mut self, target: Local, key: PropertyId) -> VmResult<()> {
        let value = self.stack.pop().unwrap();
        if !matches!(value, Value::Unit(_)) {
            match &mut self.stack[target.0 as usize] {
                Value::Struct(attrs, _) | Value::StructUpdate(attrs, _) => {
                    attrs.insert(key, value.to_unit_attr());
                }
                Value::Filter(filter, _) => {
                    let meta = self.ontology.ontol_domain_meta();
                    let relationship = key.relationship_id.0;

                    if relationship == meta.order_relationship {
                        filter.set_order(value);
                    } else if relationship == meta.direction_relationship {
                        filter
                            .set_direction(value, self.ontology)
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
    fn put_attr2(&mut self, target: Local, key: PropertyId) -> VmResult<()> {
        let [rel, val]: [Value; 2] = self.pop_n();
        if !matches!(val, Value::Unit(_)) {
            let map = self.struct_local_mut(target)?;
            map.insert(key, Attribute { rel, val });
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
    fn push_i64(&mut self, k: i64, result_type: DefId) {
        self.stack.push(Value::I64(k, result_type));
    }

    #[inline(always)]
    fn push_f64(&mut self, k: f64, result_type: DefId) {
        self.stack.push(Value::F64(k, result_type));
    }

    #[inline(always)]
    fn push_string(&mut self, k: TextConstant, result_type: DefId) {
        let str = &self.ontology[k];
        self.stack.push(Value::Text(str.into(), result_type));
    }

    #[inline(always)]
    fn append_attr2(&mut self, seq: Local) -> VmResult<()> {
        let [rel, val]: [Value; 2] = self.pop_n();
        let seq = self.sequence_local_mut(seq)?;
        seq.attrs.push(Attribute { rel, val });
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
            Predicate::MatchesDiscriminant(local, def_id) => {
                let value = self.local(*local);
                Ok(value.type_def_id() == *def_id)
            }
        }
    }

    fn move_seq_vals_to_stack(&mut self, source: Local) -> VmResult<()> {
        let sequence = std::mem::take(&mut self.sequence_local_mut(source)?.attrs);
        *self.local_mut(source) = Value::unit();
        self.stack.extend(sequence.into_iter().map(|attr| attr.val));
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
            *self.local_mut(local).type_def_id_mut() = def_id;
        } else {
            *self.stack.last_mut().unwrap().type_def_id_mut() = def_id;
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
                self.ontology.ontol_domain_meta().text,
            );
            self.stack.push(Value::Sequence(
                attributes.into(),
                self.ontology.ontol_domain_meta().text,
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

        let mut attrs: ThinVec<Attribute> = ThinVec::new();
        let text_def_id = self.ontology.ontol_domain_meta().text;

        for captures in text_pattern
            .regex
            // Operates in non-greedy mode with `earliest(true)`
            .captures_iter(Input::new(haystack).earliest(true))
        {
            let value_attributes =
                extract_regex_captures(haystack, &captures, group_filter, text_def_id);
            attrs.push(Attribute::from(Value::Sequence(
                value_attributes.into(),
                text_def_id,
            )));
        }

        self.stack.push(Value::Sequence(attrs.into(), text_def_id));
        Ok(())
    }

    fn cond_var(&mut self, cond_local: Local) -> VmResult<()> {
        let Value::Filter(filter, _) = &mut self.local_mut(cond_local) else {
            return Err(VmError::InvalidType(cond_local));
        };

        let cond_var = filter.condition_mut().mk_cond_var();

        self.stack_mut()
            .push(Value::I64(cond_var.0 as i64, DefId::unit()));

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
            Clause::IsEntity(def_id) => Clause::IsEntity(*def_id),
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
    fn eval_builtin(&mut self, proc: BuiltinProc, result_type: DefId) -> Value {
        match proc {
            BuiltinProc::Add => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a + b, result_type)
            }
            BuiltinProc::Sub => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a - b, result_type)
            }
            BuiltinProc::Mul => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a * b, result_type)
            }
            BuiltinProc::Div => {
                let [b, a]: [i64; 2] = self.pop_n();
                Value::I64(a / b, result_type)
            }
            BuiltinProc::Append => {
                let [b, a]: [String; 2] = self.pop_n();
                Value::Text(a + b, result_type)
            }
            BuiltinProc::NewStruct => Value::Struct(Default::default(), result_type),
            BuiltinProc::NewSeq => Value::Sequence(Default::default(), result_type),
            BuiltinProc::NewUnit => Value::Unit(result_type),
            BuiltinProc::NewFilter => Value::Filter(Default::default(), result_type),
            BuiltinProc::NewVoid => Value::Void(result_type),
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
    fn struct_local_mut(
        &mut self,
        local: Local,
    ) -> VmResult<&mut FnvHashMap<PropertyId, Attribute>> {
        match self.local_mut(local) {
            Value::Struct(attrs, _) | Value::StructUpdate(attrs, _) => Ok(attrs.as_mut()),
            _ => Err(VmError::InvalidType(local)),
        }
    }

    #[inline(always)]
    fn sequence_local_mut(&mut self, local: Local) -> VmResult<&mut Sequence> {
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
        self.stack.push(Value::Void(DefId::unit()));
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
        trace!("{:?}", self.ontology.debug(&vm.pending_opcode()));
    }
}

fn extract_regex_captures(
    haystack: &str,
    captures: &Captures,
    group_filter: &BitVec,
    text_def_id: DefId,
) -> ThinVec<Attribute> {
    group_filter
        .iter()
        .enumerate()
        .filter_map(|(index, value)| if value { Some(index) } else { None })
        .map(|index| {
            let value = match captures.get_group(index) {
                Some(span) => Value::Text(haystack[span.start..span.end].into(), text_def_id),
                None => Value::Void(DefId::unit()),
            };
            Attribute::from(value)
        })
        .collect()
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
                OpCode::GetAttr(Local(0), "S:0:1".parse().unwrap(), GetAttrFlags::take2()),
                OpCode::PutAttr1(Local(1), "S:0:3".parse().unwrap()),
                OpCode::GetAttr(Local(0), "S:0:2".parse().unwrap(), GetAttrFlags::take2()),
                OpCode::PutAttr1(Local(1), "S:0:4".parse().unwrap()),
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let output = OntolVm::new(&ontology, proc)
            .run([Value::new_struct(
                [
                    (
                        "S:0:1".parse().unwrap(),
                        Value::Text("foo".into(), def_id(0)).into(),
                    ),
                    (
                        "S:0:2".parse().unwrap(),
                        Value::Text("bar".into(), def_id(0)).into(),
                    ),
                ],
                def_id(0),
            )])
            .unwrap()
            .unwrap();

        let Value::Struct(attrs, _) = output else {
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
                OpCode::Return,
            ],
        );
        let add_then_double = lib.append_procedure(
            NParams(2),
            [
                OpCode::CallBuiltin(BuiltinProc::Add, def_id(0)),
                OpCode::Call(double),
                OpCode::Return,
            ],
        );
        let mapping_proc = lib.append_procedure(
            NParams(1),
            [
                OpCode::CallBuiltin(BuiltinProc::NewStruct, def_id(0)),
                // 2, 3:
                OpCode::GetAttr(Local(0), "S:0:1".parse().unwrap(), GetAttrFlags::take2()),
                OpCode::Call(double),
                OpCode::PutAttr1(Local(1), "S:0:4".parse().unwrap()),
                // 3, 4:
                OpCode::GetAttr(Local(0), "S:0:2".parse().unwrap(), GetAttrFlags::take2()),
                // 5, 6:
                OpCode::GetAttr(Local(0), "S:0:3".parse().unwrap(), GetAttrFlags::take2()),
                OpCode::Clone(Local(4)),
                // pop(6, 7):
                OpCode::Call(add_then_double),
                OpCode::PutAttr1(Local(1), "S:0:5".parse().unwrap()),
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
            ],
        );

        let ontology = Ontology::builder().lib(lib).build();
        let output = OntolVm::new(&ontology, mapping_proc)
            .run([Value::new_struct(
                [
                    ("S:0:1".parse().unwrap(), Value::I64(333, def_id(0)).into()),
                    ("S:0:2".parse().unwrap(), Value::I64(10, def_id(0)).into()),
                    ("S:0:3".parse().unwrap(), Value::I64(11, def_id(0)).into()),
                ],
                def_id(0),
            )])
            .unwrap()
            .unwrap();

        let Value::Struct(mut attrs, _) = output else {
            panic!();
        };
        let Value::I64(a, _) = attrs.remove(&"S:0:4".parse().unwrap()).unwrap().val else {
            panic!();
        };
        let Value::I64(b, _) = attrs.remove(&"S:0:5".parse().unwrap()).unwrap().val else {
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
                OpCode::Iter(Local(0), Local(2), AddressOffset(5)),
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
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
        let output = OntolVm::new(&ontology, proc)
            .run([Value::sequence_of([
                Value::I64(1, def_id(0)),
                Value::I64(2, def_id(0)),
            ])])
            .unwrap()
            .unwrap();

        let Value::Sequence(seq, _) = output else {
            panic!();
        };
        let output = seq
            .attrs
            .into_iter()
            .map(|attr| attr.val.cast_into())
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
                OpCode::GetAttr(Local(0), prop_a, GetAttrFlags::take2()),
                // [b] -> Local(4):
                OpCode::GetAttr(Local(0), prop_b, GetAttrFlags::take2()),
                // counter -> Local(5):
                OpCode::I64(0, def_id(0)),
                // output -> Local(6):
                OpCode::CallBuiltin(BuiltinProc::NewSeq, def_id(0)),
                OpCode::Iter(Local(4), Local(5), AddressOffset(7)),
                OpCode::PopUntil(Local(6)),
                OpCode::Return,
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
        let output = OntolVm::new(&ontology, proc)
            .run([Value::new_struct(
                [
                    (prop_a, Value::Text("a".into(), def_id(0)).into()),
                    (
                        prop_b,
                        Value::sequence_of([
                            Value::Text("b0".into(), def_id(0)),
                            Value::Text("b1".into(), def_id(0)),
                        ])
                        .into(),
                    ),
                ],
                def_id(0),
            )])
            .unwrap()
            .unwrap();

        assert_eq!(
            "[{S:0:1 -> 'b0', S:0:0 -> 'a'}, {S:0:1 -> 'b1', S:0:0 -> 'a'}]",
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
                OpCode::GetAttr(Local(0), prop, GetAttrFlags::take2()),
                OpCode::Cond(Predicate::IsNotVoid(Local(2)), AddressOffset(5)),
                // AddressOffset(3):
                OpCode::PopUntil(Local(1)),
                OpCode::Return,
                // AddressOffset(4):
                OpCode::Cond(
                    Predicate::MatchesDiscriminant(Local(3), inner_def_id),
                    AddressOffset(7),
                ),
                OpCode::Goto(AddressOffset(3)),
                // AddressOffset(7):
                OpCode::I64(666, def_id(200)),
                OpCode::PutAttr1(Local(1), prop),
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
                        .run([Value::new_struct([], def_id(0))])
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
                        .run([Value::new_struct([(prop, Value::unit().into())], def_id(0))])
                        .unwrap()
                        .unwrap()
                )
            )
        );

        assert_eq!(
            "{S:0:42 -> int(666)}",
            format!(
                "{}",
                ValueDebug(
                    &OntolVm::new(&ontology, proc)
                        .run([Value::new_struct(
                            [(prop, Value::Text("a".into(), inner_def_id).into(),)],
                            def_id(0)
                        )])
                        .unwrap()
                        .unwrap()
                )
            )
        );
    }
}
