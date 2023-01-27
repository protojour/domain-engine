use std::ops::Range;

pub mod ast;
pub mod tree;

mod parse;
mod tree_stream;

pub use parse::parse;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
