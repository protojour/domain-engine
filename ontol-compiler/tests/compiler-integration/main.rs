#![allow(unused)]

use ontol_compiler::{
    compiler::Compiler, error::UnifiedCompileError, mem::Mem, Compile, CompileSrc,
    SpannedCompileError,
};
use ontol_runtime::{env::Env, PackageId};

mod test_compile_errors;
mod test_deserialize;
mod test_entity;
mod test_eq_basic;
mod test_geojson;
mod test_serde;
mod test_string_patterns;
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

/// Assert that JSON that gets deserialized and then serialized again matches the expectation.
///
/// When passing one JSON parameter, the assertion means that input and output must match exactly.
/// With passing two JSON parameters, the left one is the input and the right one is the expected output.
macro_rules! assert_json_io_matches {
    ($binding:expr, $json:expr) => {
        assert_json_io_matches!($binding, $json, $json);
    };
    ($binding:expr, $input:expr, $expected_output:expr) => {
        let input = $input;
        let value = match $binding.deserialize_value(input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = crate::util::serialize_json($binding.env(), &value);

        pretty_assertions::assert_eq!($expected_output, output);
    };
}

pub(crate) use assert_json_io_matches;
use util::AnnotatedCompileError;

trait TestCompile: Sized {
    /// Compile
    fn compile_ok(self, validator: impl Fn(&Env));

    /// Compile, expect failure
    fn compile_fail(self) {
        self.compile_fail_then(|_| {})
    }

    /// Compile, expect failure with error closure
    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>));

    /// Compile using S-expr syntax
    fn s_compile_ok(self, validator: impl Fn(&Env));

    /// Compile using S-expr syntax, expect failure
    fn s_compile_fail(self) {
        self.s_compile_fail_then(|_| {})
    }

    /// Compile using S-expr syntax, expect failure with error closure
    fn s_compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>));
}

impl TestCompile for &'static str {
    fn compile_ok(self, validator: impl Fn(&Env)) {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        let compile_src = compiler.sources.add(TEST_PKG, "str".into(), self.into());

        match compile_src.clone().compile(&mut compiler, TEST_PKG) {
            Ok(()) => {
                validator(&compiler.into_env());
            }
            Err(errors) => {
                util::diff_errors(self, compile_src, errors, "// ERROR");
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
    }

    fn compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        let mut mem = Mem::default();
        let mut compiler = Compiler::new(&mut mem).with_core();
        let compile_src = compiler
            .sources
            .add(PackageId(666), "str".into(), self.into());

        let Err(errors) = compile_src.clone().compile(&mut compiler, PackageId(1)) else {
            panic!("Script did not fail to compile");
        };

        let annotated_errors = util::diff_errors(self, compile_src, errors, "// ERROR");
        validator(annotated_errors);
    }

    fn s_compile_ok(self, validator: impl Fn(&Env)) {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        let compile_src = compiler.sources.add(TEST_PKG, "str".into(), self.into());

        match compile_src.clone().s_compile(&mut compiler, TEST_PKG) {
            Ok(()) => {
                validator(&compiler.into_env());
            }
            Err(errors) => {
                util::diff_errors(self, compile_src, errors, ";; ERROR");
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
    }

    fn s_compile_fail_then(self, validator: impl Fn(Vec<AnnotatedCompileError>)) {
        let mut mem = Mem::default();
        let mut compiler = Compiler::new(&mut mem).with_core();
        let compile_src = compiler
            .sources
            .add(PackageId(666), "str".into(), self.into());

        let Err(errors) = compile_src.clone().s_compile(&mut compiler, PackageId(1)) else {
            panic!("Script did not fail to compile");
        };

        let annotated_errors = util::diff_errors(self, compile_src, errors, ";; ERROR");
        validator(annotated_errors);
    }
}

#[test]
#[should_panic(expected = "it works")]
fn ok_validator_must_run() {
    "".s_compile_ok(|_| {
        panic!("it works");
    })
}

#[test]
#[should_panic(expected = "it works")]
fn failure_validator_must_run() {
    "( ;; ERROR lex error".s_compile_fail_then(|_| {
        panic!("it works");
    })
}

fn main() {}
