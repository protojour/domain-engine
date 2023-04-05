use ontol_compiler::{mem::Mem, Compiler};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn test_run_compiler() {
    let mem = Mem::default();
    let compiler = Compiler::new(&mem, Default::default());
    let _env = compiler.into_env();
}

#[cfg(test)]
mod tests {
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn it_works() {
        println!("it works lol");
    }
}
