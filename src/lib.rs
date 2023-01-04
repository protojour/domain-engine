use std::ops::Range;

use chumsky::prelude::*;
use smartstring::{LazyCompact, SmartString};

pub mod lex;

type SmString = SmartString<LazyCompact>;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

#[derive(Clone)]
pub struct AstNode {
    pub span: Range<usize>,
    pub kind: AstKind,
}

#[derive(Clone)]
pub enum AstKind {
    Nothing,
}

pub fn parser() -> impl Parser<char, AstNode, Error = Simple<char>> {
    recursive(|value| {
        // value.map_err_with_span(f);

        just("null")
            .map_with_span(|_, span| AstNode {
                span,
                kind: AstKind::Nothing,
            })
            .labelled("null")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_works() {
        let parser = parser();
        parser.parse("fdsjklfds").unwrap();
    }
}
