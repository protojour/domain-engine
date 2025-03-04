use crate::ResponseExt;
use automerge::{ReadDoc, transaction::Transactable};
use domain_engine_core::Session;
use domain_engine_httpjson::crdt::{DocAddr, doc_repository::DocRepository};
use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreFactory, system::MonotonicClockSystemApi,
};
use http::header::{self};
use ontol_examples::workspaces;
use ontol_macros::datastore_test;
use ontol_runtime::value::Value;
use ontol_test_utils::{TestCompile, expect_eq};
use reqwest_websocket::CloseCode;
use serde_json::json;
use test_sync_client::Dir;
use tokio_util::task::AbortOnDropHandle;
use tracing::{Instrument, info, info_span};
use ulid::Ulid;

use crate::{MakeTestRouter, TestHttpError, make_domain_engine};

/// id of the workspaces domain under test
const DOMAIN_ID: &str = "01JAP41VG1STK1VZPWXV26SPNM";

#[datastore_test(tokio::test)]
async fn test_workspaces_rest_api_with_edit(ds: &str) {
    let test = vec![workspaces()].compile();
    let [workspace] = test.bind(["Workspace"]);
    let engine = make_domain_engine(
        test.ontology_owned(),
        DynamicDataStoreFactory::new(ds),
        Box::new(MonotonicClockSystemApi::default()),
    )
    .await;
    let router = test.make_test_router(engine.clone(), "workspaces");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _handle = AbortOnDropHandle::new(tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    }));

    let workspace_data_prop_id = workspace
        .def
        .data_relationship_by_name("data", test.ontology())
        .unwrap()
        .0;
    let doc_repository = DocRepository::from(engine.clone());

    info!("create an example workspace");

    let workspace_id = http_post_workspace_entity(
        port,
        json!({
            "data": {
                "title": "untitled",
                "files": [
                    {
                        "path": "README.md",
                        "contents": "# Extremely interesting"
                    }
                ]
            },
            "title": "untitled",
        }),
    )
    .await;

    let workspace_ulid = Value::octet_sequence(
        workspace_id
            .parse::<Ulid>()
            .unwrap()
            .to_bytes()
            .into_iter()
            .collect(),
    );

    info!(%workspace_id, "workspace created, now update title");

    http_put_workspace_entity(
        port,
        json!({
            "id": workspace_id,
            "title": "new title",
        }),
    )
    .await
    .unwrap();

    {
        info!("data as a CRDT should not be directly updatable through REST");

        expect_eq!(
            actual = http_put_workspace_entity(
                port,
                json!({
                    "id": workspace_id,
                    "data": {
                        "title": "(DEPRECATED)",
                        "files": []
                    }
                }),
            )
            .await
            .unwrap_err()
            .to_string(),
            expected = r#"status: Some(422), body: {"message":"property `data` not available in this context at line 1 column 7"}"#,
        );
    }

    info!("now try to GET it");

    let expected_created_at = if ds == "inmemory" {
        // BUG/FIXME: inmemory always updates this on PUT, it should be 1971
        "1974-01-01T00:00:00Z"
    } else {
        "1971-01-01T00:00:00Z"
    };

    expect_eq!(
        actual = http_get_workspace_entity(port, &workspace_id)
            .await
            .unwrap(),
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
            },
            "title": "new title",
            "created_at": expected_created_at,
            "updated_at": "1974-01-01T00:00:00Z",
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
        .load(workspace.def_id(), doc_addr.clone(), Session::default())
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
            workspace.def_id(),
            doc_addr,
            workspace_data.save_after(&original_heads),
            Session::default(),
        )
        .await
        .unwrap();

    info!(
        "assert that the new JSON encoding of the workspace reflects the previous change transaction"
    );

    expect_eq!(
        actual = http_get_workspace_entity(port, &workspace_id)
            .await
            .unwrap(),
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
            },
            "title": "new title",
            "created_at": expected_created_at,
            "updated_at": "1977-01-01T00:00:00Z",
        })
    );

    info!("list most recent workspaces");

    expect_eq!(
        actual = list_most_recent_workspaces(port).await.unwrap(),
        expected = json!([{
            "id": workspace_id,
            "created_at": expected_created_at,
            "updated_at": "1977-01-01T00:00:00Z",
            "title": "new title",
        }])
    );
}

/// This test runs with a real webserver because of the websockets,
/// not sure how to fake those.
#[datastore_test(tokio::test)]
#[ignore]
async fn test_workspace_sync(ds: &str) {
    let test = vec![workspaces()].compile();
    let engine = make_domain_engine(
        test.ontology_owned(),
        DynamicDataStoreFactory::new(ds).crdt_compaction_threshold(3),
        Box::new(MonotonicClockSystemApi::default()),
    )
    .await;
    let router = test.make_test_router(engine.clone(), "workspaces");

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _handle = AbortOnDropHandle::new(tokio::spawn(async move {
        let _ = axum::serve(listener, router.clone()).await;
    }));

    info!("create an example workspace");

    let workspace_id = http_post_workspace_entity(
        port,
        json!({
            "data": {
                "files": [
                    {
                        "path": "README.md",
                        "contents": "# Extremely interesting"
                    }
                ]
            }
        }),
    )
    .await;

    info!(%workspace_id, "workspace created, create two clients");

    let actor1 = acquire_actor(&workspace_id, port).await;
    let actor2 = acquire_actor(&workspace_id, port).await;
    let mut client1 = test_sync_client::Client::connect(&workspace_id, &actor1, port).await;
    let mut client2 = test_sync_client::Client::connect(&workspace_id, &actor2, port).await;

    client1.join().instrument(info_span!("client1 join")).await;
    client2.join().instrument(info_span!("client2 join")).await;

    info!("client1: change the document");

    {
        let mut txn = client1.automerge.transaction();

        let file_list = txn
            .get(automerge::ROOT, format!("{DOMAIN_ID}_2_1"))
            .unwrap()
            .expect("no property called that");
        let first_file_obj = txn.values(&file_list.1).next().unwrap();
        let file_contents_obj = txn
            .get(&first_file_obj.1, format!("{DOMAIN_ID}_3_1"))
            .unwrap()
            .expect("no property called that");

        let current_contents = txn.text(&file_contents_obj.1).unwrap();
        assert_eq!(current_contents, "# Extremely interesting");

        info!("...change title to `# Extremely uninteresting`");
        txn.splice_text(&file_contents_obj.1, 12, 0, "un").unwrap();

        txn.commit();
    }

    client1
        .sync_loop(Dir::Outgoing)
        .instrument(info_span!("client1"))
        .await;

    client2
        .sync_loop(Dir::Incoming)
        .instrument(info_span!("client2"))
        .await;

    info!("client2: should see reflected changes");
    {
        let file_list = client2
            .automerge
            .get(automerge::ROOT, format!("{DOMAIN_ID}_2_1"))
            .unwrap()
            .expect("no property called that");
        let first_file_obj = client2.automerge.values(&file_list.1).next().unwrap();
        let file_contents_obj = client2
            .automerge
            .get(&first_file_obj.1, format!("{DOMAIN_ID}_3_1"))
            .unwrap()
            .expect("no property called that");

        let current_contents = client2.automerge.text(&file_contents_obj.1).unwrap();
        assert_eq!(current_contents, "# Extremely uninteresting");
    }

    client1.ws.close(CloseCode::Normal, None).await.unwrap();
    client2.ws.close(CloseCode::Normal, None).await.unwrap();

    info!("should see changes reflected in domain entity");
    expect_eq!(
        actual = http_get_workspace_entity(port, &workspace_id)
            .await
            .unwrap(),
        expected = json!({
            "id": workspace_id,
            "data": {
                "files": [
                    {
                        "path": "README.md",
                        "contents": "# Extremely uninteresting"
                    }
                ]
            },
            "created_at": "1971-01-01T00:00:00Z",
            "updated_at": "1974-01-01T00:00:00Z",
        })
    );
}

/// POST workspace, return ULID
async fn http_post_workspace_entity(port: u16, json: serde_json::Value) -> String {
    let response = reqwest::Client::new()
        .post(format!("http://localhost:{port}/Workspace"))
        .json(&json)
        .send()
        .await
        .unwrap();

    info!(?response, "response");

    let location = response.headers().get(header::LOCATION).cloned().unwrap();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(bytes.len(), 0);

    location
        .to_str()
        .unwrap()
        .rsplit_once('/')
        .unwrap()
        .1
        .to_string()
}

async fn http_get_workspace_entity(
    port: u16,
    workspace_id: &str,
) -> Result<serde_json::Value, TestHttpError> {
    Ok(reqwest::Client::new()
        .get(format!(
            "http://localhost:{port}/Workspace/id/{workspace_id}"
        ))
        .send()
        .await?
        .json()
        .await?)
}

async fn http_put_workspace_entity(
    port: u16,
    json: serde_json::Value,
) -> Result<(), TestHttpError> {
    reqwest::Client::new()
        .put(format!("http://localhost:{port}/Workspace"))
        .json(&json)
        .send()
        .await?
        .raise_for_status_with_body()
        .await?
        .text()
        .await?;
    Ok(())
}

async fn list_most_recent_workspaces(port: u16) -> Result<serde_json::Value, TestHttpError> {
    Ok(reqwest::Client::new()
        .get(format!("http://localhost:{port}/listMostRecentWorkspaces"))
        .send()
        .await?
        .json()
        .await?)
}

async fn acquire_actor(workspace_id: &str, port: u16) -> String {
    reqwest::Client::new()
        .post(format!(
            "http://localhost:{port}/Workspace/id/{workspace_id}/data/actor"
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

/// A test client to sync automerge documents over websocket on demand
mod test_sync_client {
    use std::time::Duration;

    use automerge::{
        Automerge,
        sync::{Message as SyncMessage, State, SyncDoc},
    };
    use domain_engine_httpjson::crdt::ws_codec::Message;
    use futures_util::{SinkExt, StreamExt};
    use reqwest_websocket::{Message as WsMessage, RequestBuilderExt, WebSocket};
    use tracing::debug;

    #[derive(Clone, Copy)]
    pub enum Dir {
        Incoming,
        Outgoing,
    }

    impl Dir {
        fn invert(self) -> Dir {
            match self {
                Dir::Incoming => Self::Outgoing,
                Dir::Outgoing => Self::Incoming,
            }
        }
    }

    pub struct Client {
        pub ws: WebSocket,
        pub automerge: Automerge,
        sync_state: State,
    }

    impl Client {
        pub async fn connect(workspace_id: &str, actor: &str, port: u16) -> Self {
            let ws = reqwest::Client::new()
                .get(format!(
                    "http://localhost:{port}/Workspace/id/{workspace_id}/data?actor={actor}"
                ))
                .upgrade()
                .send()
                .await
                .unwrap()
                .into_websocket()
                .await
                .unwrap();

            Self {
                ws,
                automerge: Default::default(),
                sync_state: Default::default(),
            }
        }

        /// send the "join" message and perform handshake
        pub async fn join(&mut self) {
            self.send(Message::Join).await;
            self.sync_loop(Dir::Incoming).await;
        }

        pub async fn sync_loop(&mut self, mut dir: Dir) {
            loop {
                match dir {
                    Dir::Incoming => {
                        // wait for incoming message for, if not assume the sync is done.
                        // the wait is quite long because of slow CI
                        let timeout = tokio::time::sleep(Duration::from_millis(500));

                        tokio::select! {
                            _ = self.sync_incoming_once() => {}
                            _ = timeout => {
                                debug!("timeout: sync done");
                                return;
                            }
                        }
                    }
                    Dir::Outgoing => self.sync_outgoing_once().await,
                }

                dir = dir.invert();
            }
        }

        async fn sync_outgoing_once(&mut self) {
            debug!("outgoing");
            if let Some(outgoing) = self.automerge.generate_sync_message(&mut self.sync_state) {
                self.send(Message::Sync(outgoing.encode())).await;
            }
        }

        async fn sync_incoming_once(&mut self) {
            debug!("incoming");
            let Message::Sync(buf) = self.recv().await else {
                panic!("invalid message");
            };

            self.automerge
                .receive_sync_message(&mut self.sync_state, SyncMessage::decode(&buf).unwrap())
                .unwrap();
        }

        async fn send(&mut self, message: Message) {
            self.ws
                .send(WsMessage::Binary(message.encode_cbor()))
                .await
                .unwrap();
        }

        async fn recv(&mut self) -> Message {
            match self.ws.next().await.unwrap().unwrap() {
                WsMessage::Binary(buf) => Message::decode_cbor(&buf).unwrap(),
                _other => panic!("unexpected ws message"),
            }
        }
    }
}
