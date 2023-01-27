use ::chumsky::Parser;

use compile::{
    error::{CompileError, UnifiedCompileError},
    lowering::Lowering,
};
use compiler::Compiler;

pub use compile::error::*;
use ontol_runtime::PackageId;
pub use source::*;

pub mod compiler;
pub mod ena;
pub mod mem;
pub mod serde_codegen;

mod compile;
mod compiler_queries;
mod def;
mod expr;
mod namespace;
mod parse;
mod relation;
mod source;
mod types;

pub trait Compile {
    fn compile<'m>(
        self,
        compiler: &mut Compiler<'m>,
        package: PackageId,
    ) -> Result<(), UnifiedCompileError>;
}

impl Compile for &str {
    fn compile<'m>(
        self,
        compiler: &mut Compiler<'m>,
        package: PackageId,
    ) -> Result<(), UnifiedCompileError> {
        let src = compiler.sources.add(package, "str".into(), self.into());
        src.compile(compiler, package)
    }
}

impl Compile for CompileSrc {
    fn compile<'m>(
        self,
        compiler: &mut Compiler<'m>,
        _: PackageId,
    ) -> Result<(), UnifiedCompileError> {
        let mut compile_errors = vec![];
        let (trees, lex_errors) = parse::tree::trees_parser().parse_recovery(self.text.as_str());

        for lex_error in lex_errors {
            let span = lex_error.span();
            compile_errors
                .push(CompileError::Lex(lex_error).spanned(&compiler.sources, &self.span(&span)));
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
                compile_errors.push(
                    CompileError::Parse(parse_error).spanned(&compiler.sources, &self.span(&span)),
                );
            }

            let mut lowering = Lowering::new(compiler, &self);

            for ast in asts {
                if let Err(error) = lowering.lower_ast(ast) {
                    compile_errors.push(error);
                }
            }

            let root_defs = lowering.finish();
            let mut type_check = compiler.type_check();
            for root_def in root_defs {
                type_check.check_def(root_def);
            }
        }

        compile_errors.append(&mut compiler.errors.errors);

        if compile_errors.is_empty() {
            compiler.sources.compile_finished();
            Ok(())
        } else {
            Err(UnifiedCompileError {
                errors: compile_errors,
            })
        }
    }
}
