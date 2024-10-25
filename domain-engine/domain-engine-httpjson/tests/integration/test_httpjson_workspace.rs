use std::time::Duration;

use automerge::{transaction::Transactable, ReadDoc};
use domain_engine_core::Session;
use domain_engine_httpjson::crdt::{doc_repository::DocRepository, DocAddr};
use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreFactory, system::MonotonicClockSystemApi,
};
use http::header::{self};
use ontol_examples::workspaces;
use ontol_macros::datastore_test;
use ontol_runtime::value::Value;
use ontol_test_utils::{expect_eq, TestCompile};
use reqwest_websocket::CloseCode;
use serde_json::json;
use test_sync_client::Dir;
use tokio_util::task::AbortOnDropHandle;
use tracing::{info, info_span, Instrument};
use ulid::Ulid;

use crate::{make_domain_engine, MakeTestRouter};

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
        json!({
            "data": {
                "title": "untitled",
                "files": [
                    {
                        "path": "README.md",
                        "contents": "# Extremely interesting"
                    }
                ]
            }
        }),
        port,
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

    info!(%workspace_id, "workspace created, now try to GET it");

    expect_eq!(
        actual = http_get_workspace_entity(&workspace_id, port).await,
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

    info!("assert that the new JSON encoding of the workspace reflects the previous change transaction");

    expect_eq!(
        actual = http_get_workspace_entity(&workspace_id, port).await,
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
        json!({
            "data": {
                "title": "untitled",
                "files": [
                    {
                        "path": "README.md",
                        "contents": "# Extremely interesting"
                    }
                ]
            }
        }),
        port,
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

    client1
        .sync_loop(Dir::Outgoing, 3)
        .instrument(info_span!("client1"))
        .await;

    // unfortunately this is timing sensitive,
    // apparently need to wait for broker broadcasting
    tokio::time::sleep(Duration::from_millis(200)).await;

    client2
        .sync_loop(Dir::Incoming, 4)
        .instrument(info_span!("client2"))
        .await;

    info!("client2: should see reflected changes");
    {
        let title_obj = client2
            .automerge
            .get(automerge::ROOT, format!("{DOMAIN_ID}_2_0"))
            .unwrap()
            .unwrap();

        let current_title = client2.automerge.text(&title_obj.1).unwrap();
        assert_eq!(current_title, "Not Untitled");
    }

    client1.ws.close(CloseCode::Normal, None).await.unwrap();
    client2.ws.close(CloseCode::Normal, None).await.unwrap();

    info!("should see changes reflected in domain entity");
    expect_eq!(
        actual = http_get_workspace_entity(&workspace_id, port).await,
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

/// POST workspace, return ULID
async fn http_post_workspace_entity(json: serde_json::Value, port: u16) -> String {
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

async fn http_get_workspace_entity(workspace_id: &str, port: u16) -> serde_json::Value {
    reqwest::Client::new()
        .get(format!(
            "http://localhost:{port}/Workspace/id/{workspace_id}"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
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
    use automerge::{
        sync::{Message as SyncMessage, State, SyncDoc},
        Automerge,
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
            self.sync_loop(Dir::Incoming, 4).await;
        }

        pub async fn sync_loop(&mut self, mut dir: Dir, iterations: usize) {
            for _ in 0..iterations {
                match dir {
                    Dir::Incoming => self.sync_incoming_once().await,
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
