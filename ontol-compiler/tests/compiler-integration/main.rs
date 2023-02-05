#![allow(unused)]

use ontol_compiler::{compiler::Compiler, mem::Mem, Compile, SpannedCompileError};
use ontol_runtime::{env::Env, PackageId};

mod test_compile_errors;
mod test_deserialize;
mod test_eq_basic;
mod test_geojson;
mod test_serde;
mod util;

const TEST_PKG: PackageId = PackageId(42);

macro_rules! assert_error_msg {
    ($e:expr, $msg:expr) => {
        match $e {
            Ok(v) => panic!("Expected error, was Ok({v:?})"),
            Err(e) => pretty_assertions::assert_eq!($msg, format!("{e}").as_str()),
        }
    };
}

pub(crate) use assert_error_msg;

macro_rules! assert_json_io_matches {
    ($env:expr, $binding:expr, $json:expr) => {
        let input = $json;
        let value = match $binding.deserialize_value($env, input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = serialize_json($env, &value);

        pretty_assertions::assert_eq!(input, output);
    };
}

pub(crate) use assert_json_io_matches;

trait TestCompile {
    fn compile_ok(self, validator: impl Fn(&Env));
    fn compile_fail(self);
}

#[derive(Default)]
struct DiagnosticBuilder {
    cursor: usize,
    lines: Vec<DiagnosticsLine>,
}

struct DiagnosticsLine {
    start: usize,
    orig_stripped: String,
    errors: Vec<SpannedCompileError>,
}

impl TestCompile for &'static str {
    fn compile_ok(self, validator: impl Fn(&Env)) {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        self.compile(&mut compiler, TEST_PKG).unwrap();

        validator(&compiler.into_env());
    }

    fn compile_fail(self) {
        let mut mem = Mem::default();
        let mut compiler = Compiler::new(&mut mem).with_core();
        let compile_src = compiler
            .sources
            .add(PackageId(666), "str".into(), self.into());

        let Err(errors) = compile_src.clone().compile(&mut compiler, PackageId(1)) else {
            panic!("Script did not fail to compile");
        };

        let mut builder = self
            .lines()
            .fold(DiagnosticBuilder::default(), |mut builder, line| {
                let orig_stripped = if let Some(byte_index) = line.find(" ;; ERROR") {
                    &line[..byte_index]
                } else {
                    line
                };

                builder.lines.push(DiagnosticsLine {
                    start: builder.cursor,
                    orig_stripped: orig_stripped.to_string(),
                    errors: vec![],
                });
                builder.cursor += line.len() + 1;
                builder
            });

        for spanned_error in errors.errors {
            if spanned_error.span.source_id != compile_src.id {
                panic!("Error not from tested script: {spanned_error:?}");
            }
            let byte_pos = spanned_error.span.start as usize;

            let diagnostics_line = builder
                .lines
                .iter_mut()
                .rev()
                .find(|line| line.start <= byte_pos);

            if let Some(diagnostics_line) = diagnostics_line {
                diagnostics_line.errors.push(spanned_error);
            } else {
                panic!("Did not find originating line in script");
            }
        }

        let annotated_script = builder
            .lines
            .into_iter()
            .map(|line| {
                let original = line.orig_stripped;
                if line.errors.is_empty() {
                    original
                } else {
                    let joined_errors = line
                        .errors
                        .into_iter()
                        .map(|spanned_error| format!(";; ERROR {}", spanned_error.error))
                        .collect::<Vec<_>>()
                        .join("");
                    format!("{original} {joined_errors}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n");

        pretty_assertions::assert_eq!(self, &annotated_script);
    }
}

fn main() {}
