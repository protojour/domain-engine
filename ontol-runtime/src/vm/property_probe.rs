use fnv::{FnvHashMap, FnvHashSet};
use tracing::debug;

use crate::{
    env::{Env, TypeInfo},
    serde::operator::SerdeOperator,
    value::PropertyId,
    vm::abstract_vm::{AbstractVm, Processor, VmDebug},
    vm::proc::{BuiltinProc, Lib, Local},
    DefId,
};

use super::proc::Predicate;

pub struct PropertyProbe<'l> {
    abstract_vm: AbstractVm<'l, PropProcessor>,
    prop_stack: PropProcessor,
}

impl<'l> PropertyProbe<'l> {
    pub fn new(lib: &'l Lib) -> Self {
        Self {
            abstract_vm: AbstractVm::new(lib),
            prop_stack: PropProcessor::default(),
        }
    }

    pub fn probe_from_serde_operator(
        &mut self,
        env: &Env,
        origin: &TypeInfo,
        destination: &TypeInfo,
    ) -> Option<FnvHashMap<PropertyId, FnvHashSet<PropertyId>>> {
        let dest_serde_operator_id = destination.operator_id?;
        let serde_operator = &env.get_serde_operator(dest_serde_operator_id);

        let map_info = env
            .map_meta_table
            .get(&(destination.def_id.into(), origin.def_id.into()))?;

        match serde_operator {
            SerdeOperator::Struct(struct_op) => {
                let start_map = struct_op
                    .properties
                    .iter()
                    .map(|(_, serde_property)| {
                        (
                            serde_property.property_id,
                            [serde_property.property_id].into_iter().collect(),
                        )
                    })
                    .collect::<FnvHashMap<_, FnvHashSet<_>>>();

                self.prop_stack.stack.push(Props::Map(start_map));
                self.abstract_vm
                    .execute(map_info.procedure, &mut self.prop_stack, &mut Tracer);

                let prop_stack = std::mem::take(&mut self.prop_stack);
                if prop_stack.stack.len() != 1 {
                    panic!("expected one value");
                }
                match prop_stack.stack.into_iter().next() {
                    Some(Props::Map(map)) => Some(map),
                    _ => panic!("error"),
                }
            }
            _ => todo!(),
        }
    }
}

#[derive(Clone, Debug)]
enum Props {
    Set(FnvHashSet<PropertyId>),
    Map(FnvHashMap<PropertyId, FnvHashSet<PropertyId>>),
}

#[derive(Default)]
struct PropProcessor {
    stack: Vec<Props>,
}

impl Processor for PropProcessor {
    type Value = Props;

    fn size(&self) -> usize {
        self.stack.len()
    }

    #[inline(always)]
    fn stack_mut(&mut self) -> &mut Vec<Self::Value> {
        &mut self.stack
    }

    fn call_builtin(&mut self, proc: BuiltinProc, _: DefId) {
        let value = match proc {
            BuiltinProc::NewStruct => Props::Map(Default::default()),
            _ => {
                let mut a = self.pop_set();
                let b = self.pop_set();
                a.extend(b.into_iter());
                Props::Set(a)
            }
        };
        self.stack.push(value);
    }

    fn clone(&mut self, source: Local) {
        let value = self.local(source).clone();
        self.stack.push(value);
    }

    fn bump(&mut self, source: Local) {
        let top = self.stack.len();
        self.stack.push(Props::Set(Default::default()));
        self.stack.swap(source.0 as usize, top);
    }

    fn pop_until(&mut self, local: Local) {
        self.stack.truncate(local.0 as usize + 1);
    }

    fn swap(&mut self, a: Local, b: Local) {
        self.stack.swap(a.0 as usize, b.0 as usize);
    }

    fn iter_next(&mut self, _seq: Local, _index: Local) -> bool {
        false
    }

    fn take_attr2(&mut self, source: Local, key: PropertyId) {
        let map = self.get_map_mut(source);
        let set = map.remove(&key).unwrap();
        self.stack.push(Props::Set(FnvHashSet::default()));
        self.stack.push(Props::Set(set));
    }

    fn try_take_attr2(&mut self, source: Local, key: PropertyId) {
        let map = self.get_map_mut(source);
        match map.remove(&key) {
            Some(set) => {
                self.stack.push(Props::Set(FnvHashSet::default()));
                self.stack.push(Props::Set(FnvHashSet::default()));
                self.stack.push(Props::Set(set));
            }
            None => {
                self.stack.push(Props::Set(FnvHashSet::default()));
            }
        }
    }

    fn put_attr1(&mut self, target: Local, key: PropertyId) {
        let source_set = self.pop_set();
        let map = self.get_map_mut(target);
        let target_set = map.entry(key).or_default();
        target_set.extend(source_set.into_iter());
    }

    fn put_attr2(&mut self, target: Local, key: PropertyId) {
        let mut source_set = self.pop_set();
        source_set.extend(self.pop_set());

        let map = self.get_map_mut(target);
        let target_set = map.entry(key).or_default();
        target_set.extend(source_set.into_iter());
    }

    fn push_i64(&mut self, _: i64, _: DefId) {
        self.stack.push(Props::Set(FnvHashSet::default()));
    }

    fn push_string(&mut self, _: &str, _: DefId) {
        self.stack.push(Props::Set(FnvHashSet::default()));
    }

    fn append_attr2(&mut self, _seq: Local) {
        todo!();
    }

    fn cond_predicate(&mut self, _predicate: &Predicate) -> bool {
        false
    }

    fn type_pun(&mut self, _local: Local, _def_id: DefId) {}
}

impl PropProcessor {
    #[inline(always)]
    fn local(&self, local: Local) -> &Props {
        &self.stack[local.0 as usize]
    }

    #[inline(always)]
    fn local_mut(&mut self, local: Local) -> &mut Props {
        &mut self.stack[local.0 as usize]
    }

    #[inline(always)]
    fn get_map_mut(&mut self, local: Local) -> &mut FnvHashMap<PropertyId, FnvHashSet<PropertyId>> {
        match self.local_mut(local) {
            Props::Map(map) => map,
            Props::Set(_) => panic!("expected map"),
        }
    }

    #[inline(always)]
    fn pop_set(&mut self) -> FnvHashSet<PropertyId> {
        match self.stack.pop() {
            Some(Props::Set(set)) => set,
            _ => panic!("expected set"),
        }
    }
}

struct Tracer;

impl VmDebug<PropProcessor> for Tracer {
    fn tick(&mut self, vm: &AbstractVm<PropProcessor>, stack: &PropProcessor) {
        debug!("   -> {:?}", stack.stack);
        debug!("{:?}", vm.pending_opcode());
    }
}
