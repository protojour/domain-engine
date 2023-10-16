fn main() {
    domain_engine_test_templates::include_data_store_tests();
    println!("cargo:rerun-if-changed=build.rs");
}
