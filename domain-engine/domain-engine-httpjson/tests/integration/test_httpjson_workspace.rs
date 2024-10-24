use automerge::{transaction::Transactable, ReadDoc};
use domain_engine_core::{system::SystemApiMock, CrdtActor, Session};
use domain_engine_httpjson::crdt::{doc_repository::DocRepository, DocAddr};
use domain_engine_test_utils::system::mock_current_time_monotonic;
use domain_engine_test_utils::unimock::*;
use http::{
    header::{self, CONTENT_TYPE},
    Request, StatusCode,
};
use ontol_examples::workspaces;
use ontol_macros::datastore_test;
use ontol_runtime::value::Value;
use ontol_test_utils::{expect_eq, TestCompile};
use serde_json::json;
use tower::ServiceExt;
use tracing::info;
use ulid::Ulid;

use crate::{fetch_body_assert_status, json_body, make_domain_engine, MakeTestRouter};

/// id of the workspaces domain under test
const DOMAIN_ID: &str = "01JAP41VG1STK1VZPWXV26SPNM";

#[datastore_test(tokio::test)]
async fn test_workspaces(ds: &str) {
    let test = vec![workspaces()].compile();
    let engine = make_domain_engine(
        test.ontology_owned(),
        ds,
        (
            mock_current_time_monotonic(),
            SystemApiMock::crdt_system_actor
                .each_call(matching!())
                .returns(CrdtActor {
                    user_id: "testuser".to_string(),
                    actor_id: Default::default(),
                }),
        ),
    )
    .await;
    let router = test.make_test_router(engine.clone(), "workspaces");

    let [workspace] = test.bind(["Workspace"]);
    let workspace_data_prop_id = workspace
        .def
        .data_relationship_by_name("data", test.ontology())
        .unwrap()
        .0;
    let doc_repository = DocRepository::from(engine.clone());

    info!("create an example workspace");

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
    let workspace_ulid = Value::octet_sequence(
        workspace_id
            .parse::<Ulid>()
            .unwrap()
            .to_bytes()
            .into_iter()
            .collect(),
    );

    info!(%workspace_id, "workspace created, now try to GET it");

    expect_eq!(
        actual = http_get_workspace_entity(workspace_id, &router).await,
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

    info!("fetch the full CRDT payload");

    let workspace_vertex_addr = doc_repository
        .fetch_vertex_addr(workspace.def_id(), workspace_ulid, Session::default())
        .await
        .unwrap()
        .unwrap();
    let doc_addr = DocAddr(workspace_vertex_addr.clone(), workspace_data_prop_id);

    let mut workspace_data = doc_repository
        .load(doc_addr.clone(), Session::default())
        .await
        .unwrap()
        .unwrap()
        .with_actor(b"testuser".into());

    let original_heads = workspace_data.get_heads();

    info!("make a change to the workspace");

    {
        let mut txn = workspace_data.transaction();

        let title_obj = txn
            .get(automerge::ROOT, format!("{DOMAIN_ID}_2_0"))
            .unwrap()
            .expect("no property called that");

        let current_title = txn.text(&title_obj.1).unwrap();
        assert_eq!(current_title, "untitled");

        info!("...change title to `Not Untitled`");
        txn.splice_text(&title_obj.1, 0, 1, "Not U").unwrap();

        txn.commit();
    }

    info!("save the incremental diff to data store");

    doc_repository
        .save_incremental(
            doc_addr,
            workspace_data.save_after(&original_heads),
            Session::default(),
        )
        .await
        .unwrap();

    info!("assert that the new JSON encoding of the workspace reflects the previous change transaction");

    expect_eq!(
        actual = http_get_workspace_entity(workspace_id, &router).await,
        expected = json!({
            "id": workspace_id,
            "data": {
                "title": "Not Untitled",
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

async fn http_get_workspace_entity(workspace_id: &str, router: &axum::Router) -> serde_json::Value {
    let get_repsonse = router
        .clone()
        .oneshot(
            Request::get(format!("/Workspace/id/{workspace_id}"))
                .body(axum::body::Body::default())
                .unwrap(),
        )
        .await
        .unwrap();

    fetch_body_assert_status::<serde_json::Value>(get_repsonse, StatusCode::OK)
        .await
        .unwrap()
}
