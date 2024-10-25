use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreFactory, system::mock_current_time_monotonic,
    unimock::Unimock,
};
use http::{header::CONTENT_TYPE, StatusCode};
use ontol_macros::datastore_test;
use ontol_test_utils::{default_short_name, TestCompile};
use serde_json::json;
use tokio::task::JoinSet;

use crate::{jsonlines_stream, make_domain_engine, MakeTestRouter};

#[datastore_test(tokio::test)]
async fn test_httpjson_put_stream_transaction_error(ds: &str) {
    let test = ontol_examples::artist_and_instrument().1.compile();
    let engine = make_domain_engine(
        test.ontology_owned(),
        DynamicDataStoreFactory::new(ds),
        Box::new(Unimock::new(mock_current_time_monotonic())),
    )
    .await;
    let router = test.make_test_router(engine, default_short_name());

    let mut artist_docs = vec![
        json!({
            "ID": "artist/67e55044-10b1-426f-9247-bb680e5fe0c8",
            "name": "Radiohead"
        }),
        // The error happens here:
        json!({
            "ID": "artist/61be8d03-ca2a-4b9c-935d-b861f37ccd1c",
            "TYPO HERE": 42
        }),
    ];

    // Then a lot of documents following it:
    for _ in 0..1000 {
        artist_docs.push(json!({
            "ID": "artist/61be8d03-ca2a-4b9c-935d-b861f37ccd1c",
            "name": "Supergrass",
        }));
    }

    let tcp_listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = tcp_listener.local_addr().unwrap().port();
    let mut join_set = JoinSet::new();
    join_set.spawn(async move {
        axum::serve(tcp_listener, router).await.unwrap();
    });

    let response = reqwest::Client::new()
        .put(format!("http://localhost:{port}/artist"))
        .header(CONTENT_TYPE, "application/json-lines")
        .body(reqwest::Body::wrap_stream(jsonlines_stream(artist_docs)))
        .send()
        .await
        .unwrap();

    // Should not get "Broken Pipe" or anything,
    // even if the body did not get fully sent
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
