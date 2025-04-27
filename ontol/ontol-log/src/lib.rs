use thin_vec::ThinVec;

pub mod analyzer;
pub mod compile_entrypoint;
pub mod error;
pub mod log_model;
pub mod logfile;
pub mod lookup;
pub mod sem_match;
pub mod sem_model;
pub mod sem_stmts;
pub mod symbol;
pub mod tables;
pub mod tag;
pub mod token;
pub mod with_span;

mod diff;
mod log_util;
mod sem_diff;

pub trait NoneIfEmpty: Sized {
    fn none_if_empty(self) -> Option<Self>;
}

impl<T> NoneIfEmpty for Vec<T> {
    fn none_if_empty(self) -> Option<Self> {
        if self.is_empty() { None } else { Some(self) }
    }
}

impl<T> NoneIfEmpty for ThinVec<T> {
    fn none_if_empty(self) -> Option<Self> {
        if self.is_empty() { None } else { Some(self) }
    }
}
