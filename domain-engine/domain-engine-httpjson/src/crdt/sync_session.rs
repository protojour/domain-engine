use automerge::{sync::Message as AmMessage, ActorId};
use axum::extract::ws::{Message as WsMessage, WebSocket};
use domain_engine_core::{DomainError, DomainResult, Session};
use ontol_runtime::DefId;
use tracing::{debug, error, info, warn};

use super::broker::BrokerHandle;
use super::doc_repository::DocRepository;
use super::ws_codec::Message;
use super::DocAddr;

pub struct SyncSession {
    pub resource_def_id: DefId,
    pub doc_addr: DocAddr,
    pub actor: ActorId,
    pub socket: WebSocket,
    pub broker_handle: BrokerHandle,
    pub doc_repository: DocRepository,
    pub session: Session,
}

impl SyncSession {
    pub async fn run(mut self) -> DomainResult<()> {
        // A message channel for broker broadcasting
        let (sync_tx, mut sync_rx) = tokio::sync::mpsc::channel(8);

        loop {
            tokio::select! {
                incoming = self.socket.recv() => {
                    let Some(Ok(incoming)) = incoming else {
                        return Ok(());
                    };

                    let Some(message) = try_decode_ws_message(incoming) else {
                        continue;
                    };

                    match message {
                        Message::Join => {
                            debug!("session: client join");

                            let outgoing = self.broker_handle.broker()
                                .await
                                .add_client(self.actor.clone(), sync_tx.clone())?;

                            if let Some(outgoing) = outgoing {
                                debug!("session(join): will send sync message");
                                let ws_response = Message::Sync(outgoing.encode()).encode_cbor();

                                self.socket.send(WsMessage::Binary(ws_response)).await
                                    .map_err(|err| DomainError::protocol(format!("ws: {err:?}")))?;
                            }
                        },
                        Message::Peer => {}
                        Message::Sync(payload) => {
                            match AmMessage::decode(&payload) {
                                Ok(message) => {
                                    self.handle_incoming_sync(message).await?;
                                }
                                Err(err) => {
                                    warn!(?err, "unable to decode automerge sync message");
                                }
                            }
                        },
                    }
                }
                outgoing = sync_rx.recv() => {
                    let Some(message) = outgoing else {
                        return Ok(());
                    };

                    let ws_message = Message::Sync(message.encode()).encode_cbor();

                    self.socket.send(WsMessage::Binary(ws_message)).await
                        .map_err(|err| DomainError::protocol(format!("ws: {err:?}")))?;
                }
            }
        }
    }

    async fn handle_incoming_sync(&mut self, message: AmMessage) -> DomainResult<()> {
        let Some(dispatch) = self
            .broker_handle
            .broker()
            .await
            .dispatch(&self.actor, message)?
        else {
            return Ok(());
        };

        // handle broadcasting to other connected peers
        if !dispatch.broadcasts.is_empty() {
            let broadcasts = dispatch.broadcasts;
            tokio::spawn(async move {
                for broadcast in broadcasts {
                    let _ = broadcast.sync_tx.send(broadcast.message).await;
                }
            });
        }

        if let Some(message) = dispatch.response {
            let ws_message = Message::Sync(message.encode()).encode_cbor();

            self.socket
                .send(WsMessage::Binary(ws_message))
                .await
                .map_err(|err| DomainError::protocol(format!("ws: {err:?}")))?;
        }

        let save_result = self
            .doc_repository
            .save_incremental(
                self.resource_def_id,
                self.doc_addr.clone(),
                dispatch.incremental_changes,
                self.session.clone(),
            )
            .await;

        if let Err(err) = save_result {
            error!("document not saved: {err:?}");
        }

        Ok(())
    }
}

fn try_decode_ws_message(ws_messsage: WsMessage) -> Option<Message> {
    let WsMessage::Binary(buf) = ws_messsage else {
        return None;
    };

    match Message::decode_cbor(&buf) {
        Ok(message) => Some(message),
        Err(err) => {
            let err: DomainError = err.into();
            info!("invalid ws binary message: {err:?}");
            None
        }
    }
}
