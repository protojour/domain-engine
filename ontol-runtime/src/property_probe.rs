use std::collections::{HashMap, HashSet};

use crate::{
    proc::{BuiltinProc, Local},
    vm::{AbstractVm, Stack},
    PropertyId,
};

pub struct PropertyProbe<'l> {
    abstract_vm: AbstractVm<'l>,
    prop_stack: PropStack,
}

impl<'l> PropertyProbe<'l> {}

#[derive(Clone)]
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

    fn call_builtin(&mut self, proc: BuiltinProc) {
        let value = match proc {
            BuiltinProc::NewCompound => Props::Map(Default::default()),
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

    fn take_attr(&mut self, source: Local, property_id: PropertyId) {
        let map = self.get_map_mut(source);
        let set = map.remove(&property_id).unwrap();
        self.stack.push(Props::Set(set));
    }

    fn put_attr(&mut self, target: Local, property_id: PropertyId) {
        let source_set = self.pop_set();
        let map = self.get_map_mut(target);
        let target_set = map.entry(property_id).or_default();
        target_set.extend(source_set.into_iter());
    }

    fn constant(&mut self, k: i64) {
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
