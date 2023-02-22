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

impl<'m> Compiler<'m> {
    /// Entry point of all compilation: Compiles the full package topology
    pub fn compile_package_topology(
        &mut self,
        topology: PackageTopology,
    ) -> Result<(), UnifiedCompileError> {
        let mut root_defs = vec![];

        for parsed_package in topology.packages {
            debug!("lower package {:?}", parsed_package.package_id);
            let source_id = self
                .sources
                .source_id_for_package(parsed_package.package_id)
                .expect("no source id available for package");
            let src = self
                .sources
                .get_source(source_id)
                .expect("no compiled source available");

            for error in parsed_package.parser_errors {
                self.push_error(match error {
                    ontol_parser::Error::Lex(lex_error) => {
                        let span = lex_error.span();
                        CompileError::Lex(LexError::new(lex_error)).spanned(&src.span(&span))
                    }
                    ontol_parser::Error::Parse(parse_error) => {
                        let span = parse_error.span();
                        CompileError::Parse(ParseError::new(parse_error)).spanned(&src.span(&span))
                    }
                });
            }

            let package_def_id = self.define_package(parsed_package.package_id);
            self.packages
                .loaded_packages
                .insert(parsed_package.reference, package_def_id);

            let mut lowering = Lowering::new(self, &src);

            for stmt in parsed_package.statements {
                let _ignored = lowering.lower_statement(stmt);
            }

            root_defs.append(&mut lowering.finish());
        }

        self.compile_all_packages(root_defs)
    }

    fn compile_all_packages(&mut self, root_defs: Vec<DefId>) -> Result<(), UnifiedCompileError> {
        let mut type_check = self.type_check();
        for root_def in root_defs {
            type_check.check_def(root_def);
        }

        // Call this after all source files have been compiled
        compile_all_patterns(self);
        self.type_check().check_unions();
        self.check_error()?;

        execute_codegen_tasks(self);
        self.check_error()?;

        Ok(())
    }
}
