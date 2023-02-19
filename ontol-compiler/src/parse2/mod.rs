use std::ops::Range;

pub mod ast2;
pub mod lexer;
pub mod parse_ast2;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
