use crate::vm::AbstractVm;

pub struct PropertyProbe<'l> {
    abstract_vm: AbstractVm<'l>,
    prop_stack: PropStack,
}

struct PropStack {}
