use fnv::{FnvHashMap, FnvHashSet};
use tracing::debug;

use crate::{
    env::{Env, TypeInfo},
    proc::{BuiltinProc, Lib, Local},
    serde::operator::SerdeOperator,
    value::PropertyId,
    vm::{AbstractVm, Stack, VmDebug},
    DefId,
};

pub struct PropertyProbe<'l> {
    abstract_vm: AbstractVm<'l>,
    prop_stack: PropStack,
}

impl<'l> PropertyProbe<'l> {
    pub fn new(lib: &'l Lib) -> Self {
        Self {
            abstract_vm: AbstractVm::new(lib),
            prop_stack: PropStack::default(),
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

        let mapping_proc = env
            .mapper_proc_table
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
                    .execute(*mapping_proc, &mut self.prop_stack, &mut Tracer);

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
struct PropStack {
    local0_pos: usize,
    stack: Vec<Props>,
}

impl Stack for PropStack {
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

    fn call_builtin(&mut self, proc: BuiltinProc, _: DefId) {
        let value = match proc {
            BuiltinProc::NewMap => Props::Map(Default::default()),
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
        self.stack.push(Props::Set(Default::default()));
        let stack_len = self.stack.len();
        self.stack
            .swap(self.local0_pos + source.0 as usize, stack_len - 1);
    }

    fn pop_until(&mut self, local: Local) {
        self.stack.truncate(self.local0_pos + local.0 as usize + 1);
    }

    fn swap(&mut self, a: Local, b: Local) {
        let local0_pos = self.local0_pos;
        self.stack
            .swap(local0_pos + a.0 as usize, local0_pos + b.0 as usize);
    }

    fn iter_next(&mut self, _seq: Local, _index: Local) -> bool {
        false
    }

    fn take_map_attr2(&mut self, source: Local, key: PropertyId) {
        let map = self.get_map_mut(source);
        let set = map.remove(&key).unwrap();
        self.stack.push(Props::Set(FnvHashSet::default()));
        self.stack.push(Props::Set(set));
    }

    fn put_unit_attr(&mut self, target: Local, key: PropertyId) {
        let source_set = self.pop_set();
        let map = self.get_map_mut(target);
        let target_set = map.entry(key).or_default();
        target_set.extend(source_set.into_iter());
    }

    fn take_seq_attr2(&mut self, _local: Local, _index: usize) {
        todo!()
    }

    fn push_constant(&mut self, _k: i64, _: DefId) {
        self.stack.push(Props::Set(FnvHashSet::default()));
    }

    fn append_attr2(&mut self, _seq: Local) {
        todo!();
    }
}

impl PropStack {
    #[inline(always)]
    fn local(&self, local: Local) -> &Props {
        &self.stack[self.local0_pos + local.0 as usize]
    }

    #[inline(always)]
    fn local_mut(&mut self, local: Local) -> &mut Props {
        &mut self.stack[self.local0_pos + local.0 as usize]
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

impl VmDebug<PropStack> for Tracer {
    fn tick(&mut self, vm: &AbstractVm, stack: &PropStack) {
        debug!("   -> {:?}", stack.stack);
        debug!("{:?}", vm.pending_opcode());
    }
}
