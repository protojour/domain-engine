use std::ops::Range;

pub mod ast;
pub mod ast_parser;
pub mod lexer;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);
