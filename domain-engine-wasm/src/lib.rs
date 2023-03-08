use ontol_compiler::{mem::Mem, Compiler};
use wasm_bindgen::prelude::*;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub fn test_run_compiler() {
    let mem = Mem::default();
    let compiler = Compiler::new(&mem, Default::default());
    let _env = compiler.into_env();
}
