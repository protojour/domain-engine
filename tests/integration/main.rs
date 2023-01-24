use ontol_lang::{env::Env, mem::Mem, Compile, PackageId};

mod compile_errors;

fn compile_ok(domain_script: &str, validator: impl Fn(Env)) {
    let mut mem = Mem::default();
    let mut env = Env::new(&mut mem);
    domain_script.compile(&mut env, PackageId(1)).unwrap();

    validator(env);
}

fn compile_fail(domain_script: &str) {
    let mut mem = Mem::default();
    let mut env = Env::new(&mut mem);
    domain_script.compile(&mut env, PackageId(1)).unwrap();
}

fn main() {}
