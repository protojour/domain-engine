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

macro_rules! assert_json_io_matches {
    ($binding:expr, $json:expr) => {
        let input = $json;
        let value = match $binding.deserialize_value(input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = crate::util::serialize_json($binding.env(), &value);

        pretty_assertions::assert_eq!(input, output);
    };
}

pub(crate) use assert_json_io_matches;

trait TestCompile {
    fn compile_ok(self, validator: impl Fn(&Env));
    fn compile_fail(self);
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
                util::diff_errors(self, compile_src, errors);
                panic!("Compile failed, but the test used compile_ok(), so it should not fail.");
            }
        }
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

        util::diff_errors(self, compile_src, errors);
    }
}

fn main() {}
