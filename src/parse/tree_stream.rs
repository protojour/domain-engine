use std::iter::Peekable;

use chumsky::prelude::Simple;

use crate::{
    parse::ast::{error, ParseResult},
    parse::tree::Tree,
    SString, Span, Spanned,
};

/// A stream of token trees.
///
/// The stream is consumed by parsing one element at a time.
pub struct TreeStream {
    span: Span,
    remain_span: Span,
    iterator: Peekable<<Vec<Spanned<Tree>> as IntoIterator>::IntoIter>,
}

impl TreeStream {
    pub fn new(span: Span, trees: Vec<Spanned<Tree>>) -> Self {
        Self {
            span: span.clone(),
            remain_span: span,
            iterator: trees.into_iter().peekable(),
        }
    }

    pub fn span(&self) -> Span {
        self.span.clone()
    }

    pub fn remain_span(&self) -> Span {
        self.remain_span.clone()
    }

    pub fn peek_any(&mut self) -> bool {
        self.iterator.peek().is_some()
    }

    pub fn peek_dot(&mut self) -> bool {
        match self.iterator.peek() {
            Some((Tree::Dot, _)) => true,
            _ => false,
        }
    }

    pub fn next(&mut self) -> Option<Spanned<Tree>> {
        match self.iterator.next() {
            Some(next) => {
                self.remain_span.start = next.1.end;
                Some(next)
            }
            None => None,
        }
    }

    pub fn next_msg(&mut self, msg: impl ToString) -> ParseResult<Tree> {
        match self.next() {
            Some(next) => Ok(next),
            None => Err(error(self.remain_span(), msg)),
        }
    }

    pub fn next_sym(&mut self) -> ParseResult<SString> {
        self.next_sym_msg("Expected symbol")
    }

    pub fn next_sym_msg(&mut self, msg: impl ToString) -> ParseResult<SString> {
        match self.next() {
            Some((Tree::Sym(sym), span)) => Ok((sym, span)),
            Some((_, span)) => Err(error(span, msg)),
            None => Err(error(self.remain_span(), msg)),
        }
    }

    pub fn next_list_msg(&mut self, msg: impl ToString) -> Result<TreeStream, Simple<Tree>> {
        match self.next() {
            Some((Tree::Paren(vec), span)) => Ok(Self::new(span, vec)),
            Some((_, span)) => Err(error(span, msg)),
            None => Err(error(self.remain_span(), "Expected list")),
        }
    }

    pub fn next_dot(&mut self) -> ParseResult<()> {
        match self.next() {
            Some((Tree::Dot, span)) => Ok(((), span)),
            Some((_, span)) => Err(error(span, "Expected dot")),
            None => Err(error(self.remain_span(), "Expected dot")),
        }
    }

    pub fn end(&mut self) -> Result<(), Simple<Tree>> {
        match self.next() {
            Some((_, span)) => Err(error(span, "Expected end of list")),
            None => Ok(()),
        }
    }
}
