pub mod ontol_vm;
pub mod proc;

pub(crate) mod abstract_vm;

pub enum VmState<C, Y> {
    Complete(C),
    Yielded(Y),
}

impl<C, Y> VmState<C, Y> {
    pub fn unwrap(self) -> C {
        match self {
            Self::Complete(complete) => complete,
            Self::Yielded(_) => panic!("VM yielded"),
        }
    }
}
