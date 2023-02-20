use codegen::execute_codegen_tasks;
use compiler::Compiler;
use error::{ChumskyError, CompileError, UnifiedCompileError};

pub use error::*;
use lowering::Lowering;
use ontol_runtime::{DefId, PackageId};
use patterns::compile_all_patterns;
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
mod patterns;
mod regex;
mod relation;
mod sequence;
mod source;
mod strings;
mod type_check;
mod typed_expr;
mod types;

pub trait Compile {
    fn compile(
        self,
        compiler: &mut Compiler,
        package: PackageId,
    ) -> Result<(), UnifiedCompileError>;
}

impl Compile for &str {
    fn compile(
        self,
        compiler: &mut Compiler,
        package: PackageId,
    ) -> Result<(), UnifiedCompileError> {
        let src = compiler.sources.add(package, "str".into(), self.into());
        src.compile(compiler, package)
    }
}

impl Compile for CompileSrc {
    fn compile(self, compiler: &mut Compiler, _: PackageId) -> Result<(), UnifiedCompileError> {
        let root_defs = parse_and_lower_source(compiler, self);
        compile_all_packages(compiler, root_defs)
    }
}

/// Parse and lower a source of the new syntax
fn parse_and_lower_source(compiler: &mut Compiler, src: CompileSrc) -> Vec<DefId> {
    match ontol_parser::parse_statements(&src.text) {
        Ok(statements) => {
            let mut lowering = Lowering::new(compiler, &src);

            for stmt in statements {
                let _ignored = lowering.lower_statement(stmt);
            }

            lowering.finish()
        }
        Err(errors) => {
            for error in errors {
                compiler.push_error(match error {
                    ontol_parser::Error::Lex(lex_error) => {
                        let span = lex_error.span();
                        CompileError::Lex(ChumskyError::new(lex_error))
                            .spanned(&compiler.sources, &src.span(&span))
                    }
                    ontol_parser::Error::Parse(parse_error) => {
                        let span = parse_error.span();
                        CompileError::Parse(ChumskyError::new(parse_error))
                            .spanned(&compiler.sources, &src.span(&span))
                    }
                })
            }
            vec![]
        }
    }
}

fn compile_all_packages(
    compiler: &mut Compiler,
    root_defs: Vec<DefId>,
) -> Result<(), UnifiedCompileError> {
    let mut type_check = compiler.type_check();
    for root_def in root_defs {
        type_check.check_def(root_def);
    }

    // Call this after all source files have been compiled
    compile_all_patterns(compiler);
    compiler.type_check().check_unions();
    compiler.check_error()?;

    execute_codegen_tasks(compiler);
    compiler.check_error()?;

    compiler.sources.compile_finished();
    Ok(())
}
