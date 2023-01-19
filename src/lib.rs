use ::chumsky::Parser;
use compile::{
    error::{CompileError, UnifiedCompileError},
    lower::lower_ast,
};
use env::Env;
use source::{CompileSrc, PackageId};

pub mod ena;
pub mod env;
pub mod mem;

mod compile;
mod def;
mod expr;
mod lambda;
mod parse;
mod source;
mod types;

pub trait Compile {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), UnifiedCompileError>;
}

impl Compile for &str {
    fn compile<'m>(self, env: &mut Env<'m>, package: PackageId) -> Result<(), UnifiedCompileError> {
        let src = env.sources.add(package, "str".into(), self.into());
        src.compile(env, package)
    }
}

impl Compile for CompileSrc {
    fn compile<'m>(self, env: &mut Env<'m>, _: PackageId) -> Result<(), UnifiedCompileError> {
        let mut compile_errors = vec![];
        let (trees, lex_errors) = parse::tree::trees_parser().parse_recovery(self.text.as_str());

        for lex_error in lex_errors {
            let span = lex_error.span();
            compile_errors
                .push(CompileError::Lex(lex_error).spanned(&env.sources, &self.span(span)));
        }

        if let Some(trees) = trees {
            let mut asts = vec![];
            let mut parse_errors = vec![];

            for tree in trees {
                match crate::parse::parse(tree) {
                    Ok(ast) => {
                        asts.push(ast);
                    }
                    Err(error) => {
                        parse_errors.push(error);
                    }
                }
            }

            for parse_error in parse_errors {
                let span = parse_error.span();
                compile_errors
                    .push(CompileError::Parse(parse_error).spanned(&env.sources, &self.span(span)));
            }

            for ast in asts {
                if let Err(error) = lower_ast(env, &self, ast) {
                    compile_errors.push(error);
                }
            }
        }

        compile_errors.append(&mut env.errors.errors);

        if compile_errors.is_empty() {
            env.sources.compile_finished();
            Ok(())
        } else {
            Err(UnifiedCompileError {
                errors: compile_errors,
            })
        }
    }
}
