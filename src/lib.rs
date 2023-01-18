use std::ops::Range;

use chumsky::{chain::Chain, Parser};
use smartstring::{LazyCompact, SmartString};

pub mod ena;
pub mod env;
pub mod mem;

mod compile_error;
mod def;
mod expr;
mod lambda;
mod misc;
mod parse;
mod type_check;
mod types;

pub type SString = SmartString<LazyCompact>;
pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

pub fn compile(src: &str) -> Result<(), ()> {
    let (trees, mut errs) = parse::tree::trees_parser().parse_recovery(src);

    if let Some(trees) = trees {
        let len = src.chars().len();

        let (ast, parse_errs) = (1, 29);
        // ast::ast_parser().parse_recovery(Stream::from_iter(len..len + 1, tokens.into_iter()));
    }

    panic!()
}
