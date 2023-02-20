use std::ops::Range;

pub mod ast;
pub mod lexer;
pub mod parser;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
