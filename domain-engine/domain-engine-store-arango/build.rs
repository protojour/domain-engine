fn main() {
    domain_engine_test_templates::include_data_store_tests();
    domain_engine_test_templates::include_graphql_conduit_tests();
    domain_engine_test_templates::include_graphql_misc_tests();
    println!("cargo:rerun-if-changed=build.rs");
}
