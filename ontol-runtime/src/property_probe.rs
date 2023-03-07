use std::collections::{HashMap, HashSet};

use tracing::debug;

use crate::{
    env::{Env, TypeInfo},
    proc::{BuiltinProc, Lib, Local},
    serde::SerdeOperator,
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
    ) -> Option<HashMap<PropertyId, HashSet<PropertyId>>> {
        let dest_serde_operator_id = destination.serde_operator_id?;
        let serde_operator = &env.get_serde_operator(dest_serde_operator_id);

        let translation_proc = env.translations.get(&(destination.def_id, origin.def_id))?;

        match serde_operator {
            SerdeOperator::MapType(map_type) => {
                let start_map = map_type
                    .properties
                    .iter()
                    .map(|(_, serde_property)| {
                        (
                            serde_property.property_id,
                            HashSet::from([serde_property.property_id]),
                        )
                    })
                    .collect::<HashMap<_, _>>();

                self.prop_stack.stack.push(Props::Map(start_map));
                self.abstract_vm
                    .execute(*translation_proc, &mut self.prop_stack, &mut Tracer);

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
    Set(HashSet<PropertyId>),
    Map(HashMap<PropertyId, HashSet<PropertyId>>),
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

    fn swap(&mut self, a: Local, b: Local) {
        let local0_pos = self.local0_pos;
        self.stack
            .swap(local0_pos + a.0 as usize, local0_pos + b.0 as usize);
    }

    fn take_attr_value(&mut self, source: Local, key: PropertyId) {
        let map = self.get_map_mut(source);
        let set = map.remove(&key).unwrap();
        self.stack.push(Props::Set(set));
    }

    fn put_unit_attr(&mut self, target: Local, key: PropertyId) {
        let source_set = self.pop_set();
        let map = self.get_map_mut(target);
        let target_set = map.entry(key).or_default();
        target_set.extend(source_set.into_iter());
    }

    fn constant(&mut self, _k: i64, _: DefId) {
        self.stack.push(Props::Set([].into()));
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
    fn get_map_mut(&mut self, local: Local) -> &mut HashMap<PropertyId, HashSet<PropertyId>> {
        match self.local_mut(local) {
            Props::Map(map) => map,
            Props::Set(_) => panic!("expected map"),
        }
    }

    #[inline(always)]
    fn pop_set(&mut self) -> HashSet<PropertyId> {
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
