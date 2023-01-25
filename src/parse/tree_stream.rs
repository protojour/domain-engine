use std::iter::Peekable;

use chumsky::prelude::Simple;
use smartstring::alias::String;

use crate::{parse::ast::ParseResult, parse::tree::Tree};

use super::{parse::error, Span, Spanned};

/// A stream of token trees.
///
/// The stream is consumed by parsing one element at a time.
pub struct TreeStream {
    span: Span,
    remain_span: Span,
    iterator: Peekable<<Vec<Spanned<Tree>> as IntoIterator>::IntoIter>,
}

impl TreeStream {
    pub fn new(trees: Vec<Spanned<Tree>>, span: Span) -> Self {
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
        loop {
            match self.iterator.next() {
                Some((Tree::Comment(_), _)) => {}
                Some(next) => {
                    self.remain_span.start = next.1.end;
                    return Some(next);
                }
                None => {
                    return None;
                }
            }
        }
    }

    pub fn next_sym(&mut self) -> ParseResult<String> {
        self.next_sym_msg("expected symbol")
    }

    pub fn next_sym_msg(&mut self, msg: impl ToString) -> ParseResult<String> {
        self.next_msg(
            |tree| {
                if let Tree::Sym(sym) = tree {
                    Some(sym)
                } else {
                    None
                }
            },
            msg,
        )
    }

    pub fn next_list_msg(&mut self, msg: impl ToString) -> Result<TreeStream, Simple<Tree, Span>> {
        match self.next() {
            Some((Tree::Paren(vec), span)) => Ok(Self::new(vec, span)),
            Some((_, span)) => Err(error(msg, span)),
            None => Err(error(msg, self.remain_span())),
        }
    }

    pub fn next_dot(&mut self) -> ParseResult<()> {
        self.next_msg(
            |tree| if let Tree::Dot = tree { Some(()) } else { None },
            "expected dot",
        )
    }

    pub fn next_msg<T>(
        &mut self,
        f: impl FnOnce(Tree) -> Option<T>,
        msg: impl ToString,
    ) -> ParseResult<T> {
        match self.next() {
            Some((tree, span)) => match f(tree) {
                Some(value) => Ok((value, span)),
                None => Err(error(msg, span)),
            },
            None => Err(error(msg, self.remain_span())),
        }
    }

    pub fn end(&mut self) -> Result<(), Simple<Tree>> {
        match self.next() {
            Some((_, span)) => Err(error("expected end of list", span)),
            None => Ok(()),
        }
    }
}
