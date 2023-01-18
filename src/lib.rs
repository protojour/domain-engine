use std::ops::Range;

use chumsky::Parser;
use compile_error::CompileError;
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

pub trait Compile {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), CompileError>;
}

impl Compile for &str {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), CompileError> {
        let source = SourceId(0);
        let mut compile_errors = vec![];
        let (trees, lex_errors) = parse::tree::trees_parser().parse_recovery(self);

        for lex_error in lex_errors {
            compile_errors.push(CompileError::Lex(lex_error));
        }

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

            for parse_error in parse_errors {
                compile_errors.push(CompileError::Parse(parse_error));
            }

            for ast in asts {
                if let Err(error) = lower_ast(env, package, source, ast) {
                    compile_errors.push(error);
                }
            }
        }

        compile_errors.append(&mut env.errors.errors);

        match compile_errors.len() {
            0 => Ok(()),
            1 => Err(compile_errors.into_iter().next().unwrap()),
            _ => Err(CompileError::Multi(compile_errors)),
        }
    }
}
