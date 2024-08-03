mod json_experiment;

fn config(dbname: &str) -> tokio_postgres::Config {
    let mut config = tokio_postgres::Config::default();
    config
        .host("localhost")
        .port(5432)
        .dbname(dbname)
        .user("postgres")
        .password("postgres");
    config
}
