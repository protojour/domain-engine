use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod};

#[allow(unused)]
fn test_config() -> deadpool_postgres::Config {
    let mut config = Config::new();
    config.host = Some("localhost".to_string());
    config.port = Some(5432);
    config.dbname = Some("memoriam".to_string());
    config.user = Some("memoriam".to_string());
    config.password = Some("memoriam".to_string());
    config.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    config
}

#[tokio::test]
async fn pg_test() {
    if ::std::env::var("DOMAIN_ENGINE_SKIP_PG_TESTS").is_ok() {
        return;
    }

    let config = test_config();
    let pool = config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .unwrap();

    let client = pool.get().await.unwrap();
    let stmt = client.prepare_cached("SELECT 1 + $1").await.unwrap();

    let param: i32 = 41;
    let rows = client.query(&stmt, &[&param]).await.unwrap();
    let value: i32 = rows[0].get(0);

    assert_eq!(42, value);
}
