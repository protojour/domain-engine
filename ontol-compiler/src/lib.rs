use codegen::execute_codegen_tasks;
use compiler::Compiler;
use error::{CompileError, ParseError, UnifiedCompileError};

pub use error::*;
use lowering::Lowering;
use ontol_runtime::{DefId, PackageId};
use package::PackageTopology;
use patterns::compile_all_patterns;
pub use source::*;
use tracing::debug;

pub mod compiler;
pub mod error;
pub mod mem;
pub mod package;
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

pub fn compile_package_topology(
    compiler: &mut Compiler,
    topology: PackageTopology,
) -> Result<(), UnifiedCompileError> {
    let mut root_defs = vec![];

    for package in topology.packages {
        debug!("lower package {:?}", package.package_id);
        let source_id = compiler
            .sources
            .source_id_for_package(package.package_id)
            .expect("no source id available for package");
        let src = compiler
            .sources
            .get_compiled_source(source_id)
            .expect("no compiled source available");

        for error in package.parser_errors {
            compiler.push_error(match error {
                ontol_parser::Error::Lex(lex_error) => {
                    let span = lex_error.span();
                    CompileError::Lex(LexError::new(lex_error))
                        .spanned(&compiler.sources, &src.span(&span))
                }
                ontol_parser::Error::Parse(parse_error) => {
                    let span = parse_error.span();
                    CompileError::Parse(ParseError::new(parse_error))
                        .spanned(&compiler.sources, &src.span(&span))
                }
            });
        }

        let mut lowering = Lowering::new(compiler, &src);

        for stmt in package.statements {
            let _ignored = lowering.lower_statement(stmt);
        }

        root_defs.append(&mut lowering.finish());
    }

    compile_all_packages(compiler, root_defs)
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
