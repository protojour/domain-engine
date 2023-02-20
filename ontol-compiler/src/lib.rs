use ::chumsky::Parser;

use chumsky::Stream;
use codegen::execute_codegen_tasks;
use compiler::Compiler;
use error::{ChumskyError, CompileError, UnifiedCompileError};

pub use error::*;
use lowering::Lowering;
use ontol_runtime::{DefId, PackageId};
use patterns::compile_all_patterns;
use s_lowering::SExprLowering;
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
mod patterns;
mod regex;
mod relation;
mod s_lowering;
mod s_parse;
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

    fn s_compile(
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

    fn s_compile(
        self,
        compiler: &mut Compiler,
        package: PackageId,
    ) -> Result<(), UnifiedCompileError> {
        let src = compiler.sources.add(package, "str".into(), self.into());
        src.s_compile(compiler, package)
    }
}

impl Compile for CompileSrc {
    fn compile(
        self,
        compiler: &mut Compiler,
        package: PackageId,
    ) -> Result<(), UnifiedCompileError> {
        let root_defs = parse_and_lower_source(compiler, self);
        compile_all_packages(compiler, root_defs)
    }

    fn s_compile(self, compiler: &mut Compiler, _: PackageId) -> Result<(), UnifiedCompileError> {
        let root_defs = parse_and_lower_s_source(compiler, self);
        compile_all_packages(compiler, root_defs)
    }
}

/// Parse and lower a source of the new syntax
fn parse_and_lower_source(compiler: &mut Compiler, src: CompileSrc) -> Vec<DefId> {
    let (tokens, lex_errors) = parse::lexer::lexer().parse_recovery(src.text.as_str());

    for lex_error in lex_errors {
        let span = lex_error.span();
        compiler.push_error(
            CompileError::Lex(ChumskyError::new(lex_error))
                .spanned(&compiler.sources, &src.span(&span)),
        );
    }

    if let Some(tokens) = tokens {
        let len = tokens.len();
        let stream = Stream::from_iter(len..len + 1, tokens.into_iter());
        let (statements, parse_errors) = parse::ast_parser::stmt_seq().parse_recovery(stream);

        for parse_error in parse_errors {
            let span = parse_error.span();
            compiler.push_error(
                CompileError::Parse(ChumskyError::new(parse_error))
                    .spanned(&compiler.sources, &src.span(&span)),
            );
        }

        let mut lowering = Lowering::new(compiler, &src);

        if let Some(statements) = statements {
            for stmt in statements {
                let _ignored = lowering.lower_stmt(stmt);
            }
        }

        lowering.finish()
    } else {
        vec![]
    }
}

// Parse and lower a source of the S-expression language
fn parse_and_lower_s_source(compiler: &mut Compiler, src: CompileSrc) -> Vec<DefId> {
    let (trees, lex_errors) = s_parse::tree::trees_parser().parse_recovery(src.text.as_str());

    for lex_error in lex_errors {
        let span = lex_error.span();
        compiler.push_error(
            CompileError::Lex(ChumskyError::new(lex_error))
                .spanned(&compiler.sources, &src.span(&span)),
        );
    }

    if let Some(trees) = trees {
        let mut asts = vec![];

        for tree in trees {
            match crate::s_parse::parse(tree) {
                Ok(ast) => {
                    asts.push(ast);
                }
                Err(error) => {
                    let span = error.span();
                    compiler.push_error(
                        CompileError::SParse(ChumskyError::new(error))
                            .spanned(&compiler.sources, &src.span(&span)),
                    );
                }
            }
        }

        let mut lowering = SExprLowering::new(compiler, &src);

        for ast in asts {
            let _ignored = lowering.lower_ast(ast);
        }

        lowering.finish()
    } else {
        vec![]
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
