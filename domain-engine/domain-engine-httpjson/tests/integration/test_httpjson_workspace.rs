use domain_engine_core::system::SystemApiMock;
use domain_engine_test_utils::system::mock_current_time_monotonic;
use domain_engine_test_utils::unimock::*;
use http::{
    header::{self, CONTENT_TYPE},
    Request, StatusCode,
};
use ontol_examples::workspaces;
use ontol_macros::datastore_test;
use ontol_test_utils::{expect_eq, TestCompile};
use serde_json::json;
use tower::ServiceExt;
use tracing::info;

use crate::{fetch_body_assert_status, json_body, make_domain_engine, MakeTestRouter};

#[datastore_test(tokio::test)]
async fn test_workspaces(ds: &str) {
    let test = vec![workspaces()].compile();
    let engine = make_domain_engine(
        test.ontology_owned(),
        ds,
        (
            mock_current_time_monotonic(),
            SystemApiMock::automerge_system_actor
                .some_call(matching!())
                .returns(b"test-actor".to_vec()),
        ),
    )
    .await;
    let router = test.make_test_router(engine, "workspaces");

    let post_response = router
        .clone()
        .oneshot(
            Request::post("/Workspace")
                .header(CONTENT_TYPE, "application/json")
                .body(json_body(json!({
                    "data": {
                        "title": "untitled",
                        "files": [
                            {
                                "path": "README.md",
                                "contents": "# Extremely interesting"
                            }
                        ]
                    }
                })))
                .unwrap(),
        )
        .await
        .unwrap();

    let location = post_response
        .headers()
        .get(header::LOCATION)
        .cloned()
        .unwrap();

    fetch_body_assert_status::<String>(post_response, StatusCode::CREATED)
        .await
        .unwrap();

    let workspace_id = location.to_str().unwrap().rsplit_once('/').unwrap().1;

    info!(%workspace_id, "workspace created, now try to GET it");

    let get_repsonse = router
        .clone()
        .oneshot(
            Request::get(format!("/Workspace/id/{workspace_id}"))
                .body(axum::body::Body::default())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = fetch_body_assert_status::<serde_json::Value>(get_repsonse, StatusCode::OK)
        .await
        .unwrap();

    expect_eq!(
        actual = body,
        expected = json!({
            "id": workspace_id,
            "data": {
                "title": "untitled",
                "files": [
                    {
                        "path": "README.md",
                        "contents": "# Extremely interesting"
                    }
                ]
            }
        })
    );
}
