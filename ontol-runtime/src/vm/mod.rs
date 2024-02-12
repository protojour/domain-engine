use thiserror::Error;

use proc::Local;

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

#[derive(Debug, Error)]
pub enum VmError {
    #[error("invalid type at {0:?}")]
    InvalidType(Local),
    #[error("attribute not present")]
    AttributeNotPresent,
    #[error("assert failed")]
    AssertionFailed,
    #[error("overflow")]
    Overflow,
}

pub type VmResult<T> = Result<T, VmError>;
