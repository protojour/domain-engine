use std::{ops::Range, sync::Arc};

use chumsky::Parser;
use compile_error::{CompileError, SpannedCompileError, UnifiedCompileError};
use env::Env;
use lower::lower_ast;
use misc::{CompileSrc, PackageId, SourceId};
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

pub trait Compile {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), UnifiedCompileError>;
}

impl Compile for &str {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), UnifiedCompileError> {
        CompileSrc {
            package,
            id: SourceId(0),
            name: Arc::new("str".to_string()),
            text: Arc::new(self.to_string()),
        }
        .compile(env, package)
    }
}

impl Compile for CompileSrc {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), UnifiedCompileError> {
        let source = SourceId(0);
        let mut compile_errors = vec![];
        let (trees, lex_errors) = parse::tree::trees_parser().parse_recovery(self.text.as_str());

        for lex_error in lex_errors {
            compile_errors.push(SpannedCompileError::new(
                CompileError::Lex(lex_error),
                &self,
            ));
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
                compile_errors.push(SpannedCompileError::new(
                    CompileError::Parse(parse_error),
                    &self,
                ));
            }

            for ast in asts {
                if let Err(error) = lower_ast(env, &self, ast) {
                    compile_errors.push(error);
                }
            }
        }

        compile_errors.append(&mut env.errors.errors);

        if compile_errors.is_empty() {
            Ok(())
        } else {
            Err(UnifiedCompileError {
                errors: compile_errors,
            })
        }
    }
}
