use std::ops::Range;

use chumsky::Parser;
use env::Env;
use lower::lower_ast;
use misc::{PackageId, SourceId};
use smartstring::{LazyCompact, SmartString};

pub mod ena;
pub mod env;
pub mod mem;

mod compile_error;
mod def;
mod expr;
mod lambda;
mod lower;
mod misc;
mod parse;
mod type_check;
mod types;

pub type SString = SmartString<LazyCompact>;
pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

pub fn compile<'m>(env: &mut Env<'m>, package: PackageId, src: &str) -> Result<(), ()> {
    let source = SourceId(0);
    let (trees, _lex_errors) = parse::tree::trees_parser().parse_recovery(src);

    if let Some(trees) = trees {
        let mut asts = vec![];
        let mut parse_errors = vec![];

        for tree in trees {
            match crate::parse::ast::parse(tree) {
                Ok(ast) => {
                    asts.push(ast);
                }
                Err(error) => {
                    parse_errors.push(error);
                }
            }
        }

        for ast in asts {
            if lower_ast(env, package, source, ast).is_err() {
                panic!()
            }
        }
    }

    Ok(())
}
