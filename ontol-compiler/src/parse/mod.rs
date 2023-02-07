use std::ops::Range;

pub mod ast;
pub mod tree;

mod ast_parse;
mod tree_stream;

pub use ast_parse::parse;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
