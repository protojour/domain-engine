#[cfg(feature = "test-arango")]
mod data_store_tests {
    include!(concat!(env!("OUT_DIR"), "/data_store_tests.rs"));
}

#[cfg(feature = "test-arango")]
mod graphql_conduit_tests {
    include!(concat!(env!("OUT_DIR"), "/graphql_conduit_tests.rs"));
}

#[cfg(feature = "test-arango")]
mod graphql_misc_tests {
    include!(concat!(env!("OUT_DIR"), "/graphql_misc_tests.rs"));
}
