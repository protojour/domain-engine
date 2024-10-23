use automerge::{transaction::Transactable, ReadDoc};
use domain_engine_core::{
    system::SystemApiMock,
    transact::{AccumulateSequences, ReqMessage, TransactionMode},
    DomainEngine, Session,
};
use domain_engine_test_utils::system::mock_current_time_monotonic;
use domain_engine_test_utils::unimock::*;
use futures_util::{StreamExt, TryStreamExt};
use http::{
    header::{self, CONTENT_TYPE},
    Request, StatusCode,
};
use ontol_examples::workspaces;
use ontol_macros::datastore_test;
use ontol_runtime::{
    attr::Attr,
    crdt::Automerge,
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    value::{OctetSequence, Value},
    DefId, OntolDefTag, PropId,
};
use ontol_test_utils::{expect_eq, TestCompile};
use serde_json::json;
use tower::ServiceExt;
use tracing::info;

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
            SystemApiMock::automerge_system_actor
                .some_call(matching!())
                .returns(b"test-actor".to_vec()),
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

    let workspace_vertex_addr = fetch_vertex_addr(workspace.def_id(), &engine).await;

    let mut workspace_data = Automerge::load(
        &fetch_crdt_payload(
            workspace_vertex_addr.clone(),
            workspace_data_prop_id,
            &engine,
        )
        .await
        .0,
    )
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

    info!("save the diff to data store");

    engine
        .get_data_store()
        .unwrap()
        .api()
        .transact(
            TransactionMode::ReadWrite,
            futures_util::stream::iter([Ok(ReqMessage::CrdtSaveIncremental(
                workspace_vertex_addr.0.iter().copied().collect(),
                workspace_data_prop_id,
                workspace_data
                    .get_heads()
                    .iter()
                    .map(|hash| hash.0.to_vec())
                    .collect(),
                workspace_data.save_after(&original_heads),
            ))])
            .boxed(),
            Session::default(),
        )
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
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

async fn fetch_crdt_payload(
    vertex_addr: OctetSequence,
    workspace_data_prop_id: PropId,
    engine: &DomainEngine,
) -> OctetSequence {
    let Value::OctetSequence(payload, _) = engine
        .get_data_store()
        .unwrap()
        .api()
        .transact(
            TransactionMode::ReadOnly,
            futures_util::stream::iter([Ok(ReqMessage::CrdtGet(
                vertex_addr.0.into_iter().collect(),
                workspace_data_prop_id,
            ))])
            .boxed(),
            Session::default(),
        )
        .await
        .unwrap()
        .accumulate_one_sequence()
        .await
        .unwrap()
        .into_first()
        .unwrap()
    else {
        panic!("not a binary payload");
    };

    payload
}

async fn fetch_vertex_addr(def_id: DefId, engine: &DomainEngine) -> OctetSequence {
    let vertex = engine
        .get_data_store()
        .unwrap()
        .api()
        .transact(
            TransactionMode::ReadOnly,
            futures_util::stream::iter([Ok(ReqMessage::Query(
                0,
                EntitySelect {
                    source: StructOrUnionSelect::Struct(StructSelect {
                        def_id,
                        properties: From::from([(
                            OntolDefTag::RelationDataStoreAddress.prop_id_0(),
                            Select::Unit,
                        )]),
                    }),
                    filter: Filter::default_for_datastore(),
                    limit: None,
                    after_cursor: None,
                    include_total_len: false,
                },
            ))])
            .boxed(),
            Session::default(),
        )
        .await
        .unwrap()
        .accumulate_one_sequence()
        .await
        .unwrap()
        .into_first()
        .unwrap();

    let Value::Struct(mut attrs, _) = vertex else {
        panic!();
    };

    let Attr::Unit(Value::OctetSequence(addr, _)) = attrs
        .remove(&OntolDefTag::RelationDataStoreAddress.prop_id_0())
        .unwrap()
    else {
        panic!()
    };
    addr
}
