use std::{env, fs, path::Path};

pub const DATA_STORE_TESTS: &str = include_str!("../templates/data_store_tests.rs");

pub fn include_data_store_tests() {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("data_store_tests.rs");
    fs::write(&dest_path, DATA_STORE_TESTS).unwrap();
    println!("cargo:rerun-if-changed=build.rs");
}
