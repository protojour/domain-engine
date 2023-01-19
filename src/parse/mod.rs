use std::ops::Range;

use smartstring::{LazyCompact, SmartString};

pub mod ast;
pub mod tree;

mod parse;
mod tree_stream;

pub use parse::parse;

pub type SString = SmartString<LazyCompact>;
pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
