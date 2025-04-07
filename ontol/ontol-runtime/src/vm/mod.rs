use ontol_core::tag::ValueTagError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use proc::Local;

pub mod ontol_vm;
pub mod proc;

pub(crate) mod abstract_vm;

pub enum VmState<C, Y> {
    Complete(C),
    Yield(Y),
}

impl<C, Y> VmState<C, Y> {
    pub fn unwrap(self) -> C {
        match self {
            Self::Complete(complete) => complete,
            Self::Yield(_) => panic!("VM yield"),
        }
    }
}

#[derive(Clone, Error, Serialize, Deserialize, Debug)]
pub enum VmError {
    #[error("invalid type at {0:?}")]
    InvalidType(Local),
    #[error("attribute not present")]
    AttributeNotPresent,
    #[error("invalid attribute cardinality at {0:?}")]
    InvalidAttributeCardinality(Local),
    #[error("assert failed")]
    AssertionFailed,
    #[error("overflow")]
    Overflow,
    #[error("invalid direction")]
    InvalidDirection,
    #[error("invalid matrix column")]
    InvalidMatrixColumn,
    #[error("package number exceeded")]
    PackageNumberExceeded,
}

pub type VmResult<T> = Result<T, VmError>;

impl From<ValueTagError> for VmError {
    fn from(_value: ValueTagError) -> Self {
        VmError::PackageNumberExceeded
    }
}
