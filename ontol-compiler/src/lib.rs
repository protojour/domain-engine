use ::chumsky::Parser;

use codegen::execute_codegen_tasks;
use compiler::Compiler;
use error::{ChumskyError, CompileError, UnifiedCompileError};

pub use error::*;
use lowering::Lowering;
use ontol_runtime::PackageId;
pub use source::*;

pub mod compiler;
pub mod error;
pub mod mem;
pub mod serde_codegen;

mod codegen;
mod compiler_queries;
mod def;
mod expr;
mod lowering;
mod namespace;
mod parse;
mod relation;
mod source;
mod type_check;
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
        let (trees, lex_errors) = parse::tree::trees_parser().parse_recovery(self.text.as_str());

        for lex_error in lex_errors {
            let span = lex_error.span();
            compiler.push_error(
                CompileError::Lex(ChumskyError::new(lex_error))
                    .spanned(&compiler.sources, &self.span(&span)),
            );
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
                compiler.push_error(
                    CompileError::Parse(ChumskyError::new(parse_error))
                        .spanned(&compiler.sources, &self.span(&span)),
                );
            }

            let mut lowering = Lowering::new(compiler, &self);

            for ast in asts {
                let _ignored = lowering.lower_ast(ast);
            }

            let root_defs = lowering.finish();
            let mut type_check = compiler.type_check();
            for root_def in root_defs {
                type_check.check_def(root_def);
            }

            compiler.check_error()?;

            execute_codegen_tasks(compiler);
        }

        compiler.check_error()?;
        compiler.sources.compile_finished();
        Ok(())
    }
}
