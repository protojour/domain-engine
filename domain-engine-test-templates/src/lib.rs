#![forbid(unsafe_code)]

use std::{env, fs, path::Path};

pub fn include_data_store_tests() {
    output_tests(
        "data_store_tests.rs",
        include_str!("../templates/data_store_tests.rs"),
    );
}

pub fn include_graphql_conduit_tests() {
    output_tests(
        "graphql_conduit_tests.rs",
        include_str!("../templates/graphql_conduit_tests.rs"),
    );
}

pub fn include_graphql_misc_tests() {
    output_tests(
        "graphql_misc_tests.rs",
        include_str!("../templates/graphql_misc_tests.rs"),
    );
}

fn output_tests(name: &str, source: &str) {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join(name);
    fs::write(dest_path, source).unwrap();
}
