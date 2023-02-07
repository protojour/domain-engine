use std::iter::Peekable;

use chumsky::prelude::Simple;
use smartstring::alias::String;

use crate::{parse::ast::ParseResult, parse::tree::Tree};

use super::{ast_parse::error, Span, Spanned};

pub trait Next {
    type Data;

    fn peek(tree: &Tree) -> bool;
    fn next(tree: Spanned<Tree>) -> Option<Spanned<Self::Data>>;
}

/// A stream of token trees.
///
/// The stream is consumed by parsing one element at a time.
pub struct TreeStream {
    span: Span,
    remain_span: Span,
    iterator: Peekable<<Vec<Spanned<Tree>> as IntoIterator>::IntoIter>,
}

impl From<(Vec<Spanned<Tree>>, Span)> for TreeStream {
    fn from(value: (Vec<Spanned<Tree>>, Span)) -> Self {
        Self::new(value.0, value.1)
    }
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

    pub fn peek<N: Next>(&mut self) -> bool {
        if !self.peek_any() {
            return false;
        }
        return match self.iterator.peek() {
            Some((tree, _)) => N::peek(tree),
            None => false,
        };
    }

    pub fn peek_any(&mut self) -> bool {
        loop {
            match self.iterator.peek() {
                Some((Tree::Comment(_), _)) => {
                    self.iterator.next();
                }
                Some(_) => {
                    return true;
                }
                None => {
                    return false;
                }
            }
        }
    }

    pub fn next<N: Next>(&mut self, msg: impl ToString) -> ParseResult<N::Data> {
        match self.next_opt() {
            Some((tree, span)) => match N::next((tree, span.clone())) {
                Some(value) => Ok(value),
                None => Err(error(msg, span)),
            },
            None => Err(error(msg, self.remain_span())),
        }
    }

    pub fn next_any(&mut self, msg: impl ToString) -> ParseResult<Tree> {
        match self.next_opt() {
            Some((tree, span)) => Ok((tree, span)),
            None => Err(error(msg, self.remain_span())),
        }
    }

    fn next_opt(&mut self) -> Option<Spanned<Tree>> {
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

    pub fn end(&mut self) -> Result<(), Simple<Tree>> {
        match self.next_opt() {
            Some((_, span)) => Err(error("expected end of list", span)),
            None => Ok(()),
        }
    }
}

macro_rules! next {
    ($ident:ident, $data:ty, $pat:pat, $ret:expr) => {
        impl Next for $ident {
            type Data = $data;

            #[allow(unused)]
            fn peek(tree: &Tree) -> bool {
                match tree {
                    $pat => true,
                    _ => false,
                }
            }

            fn next(spanned_tree: Spanned<Tree>) -> Option<Spanned<Self::Data>> {
                match spanned_tree {
                    ($pat, span) => Some(($ret, span)),
                    _ => None,
                }
            }
        }
    };
}

pub struct Paren;
pub struct Bracket;
pub struct Dot;
pub struct Sym;
pub struct Variable;
pub struct Num;

next!(Paren, Vec<Spanned<Tree>>, Tree::Paren(vec), vec);
next!(Bracket, Vec<Spanned<Tree>>, Tree::Bracket(vec), vec);
next!(Dot, (), Tree::Dot, ());
next!(Sym, String, Tree::Sym(str), str);
next!(Variable, String, Tree::Variable(str), str);
next!(Num, String, Tree::Num(str), str);
