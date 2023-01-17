use std::ops::Range;

use chumsky::{chain::Chain, Parser};
use smartstring::{LazyCompact, SmartString};

pub mod ast;
pub mod ena;
pub mod tree;

mod env;
mod expr;
mod lambda;
mod misc;
mod tree_stream;
mod types;

pub type SString = SmartString<LazyCompact>;
pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

pub fn compile(src: &str) -> Result<(), ()> {
    let (trees, mut errs) = tree::trees_parser().parse_recovery(src);

    if let Some(trees) = trees {
        let len = src.chars().len();

        let (ast, parse_errs) = (1, 29);
        // ast::ast_parser().parse_recovery(Stream::from_iter(len..len + 1, tokens.into_iter()));
    }

    panic!()
}
