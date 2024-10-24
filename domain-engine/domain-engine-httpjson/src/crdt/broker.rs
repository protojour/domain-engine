use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use automerge::{
    sync::{Message as AmMessage, State, SyncDoc},
    ActorId, Automerge,
};
use domain_engine_core::{DomainError, DomainResult, Session};
use tokio::sync::{Mutex, MutexGuard};
use tracing::info;

use super::{doc_repository::DocRepository, DocAddr};

#[derive(Default)]
pub struct BrokerManager {
    pub(super) brokers: BTreeMap<DocAddr, Arc<Mutex<Broker>>>,
}

/// A handle to a document broker represents a connected actor
pub struct BrokerHandle {
    doc_addr: DocAddr,
    actor: ActorId,
    broker: Arc<Mutex<Broker>>,
    manager: Option<Arc<Mutex<BrokerManager>>>,
}

/// An automerge sync for one document broker with a set of clients
pub struct Broker {
    automerge: Automerge,
    clients: BTreeMap<ActorId, BrokerClient>,
}

pub struct BrokerClient {
    pub actor: ActorId,
    pub state: State,
    pub sync_tx: tokio::sync::mpsc::Sender<AmMessage>,
}

pub struct Dispatch {
    pub response: Option<AmMessage>,

    pub broadcasts: Vec<MessageBroadcast>,

    /// incremental changes to be persisted to data store
    pub incremental_changes: Vec<u8>,
}

pub struct MessageBroadcast {
    pub actor: ActorId,
    pub message: AmMessage,
    pub sync_tx: tokio::sync::mpsc::Sender<AmMessage>,
}

/// Load broker for the given document address.
///
/// A live BrokerHandle keeps the associated broker live in memory, with the associated Automerge CRDT document.
pub async fn load_broker(
    doc_addr: DocAddr,
    actor: ActorId,
    manager: Arc<Mutex<BrokerManager>>,
    doc_repository: DocRepository,
    session: Session,
) -> DomainResult<Option<BrokerHandle>> {
    let broker = {
        let mut manager = manager.lock().await;
        match manager.brokers.entry(doc_addr.clone()) {
            Entry::Vacant(vacant) => {
                info!(?doc_addr, %actor, "first actor connected, loading document and broker");

                let Some(automerge) = doc_repository.load(doc_addr.clone(), session).await? else {
                    return Ok(None);
                };

                vacant
                    .insert(Arc::new(Mutex::new(Broker::new(automerge))))
                    .clone()
            }
            Entry::Occupied(occupied) => {
                info!(?doc_addr, %actor, "another actor connected, reusing broker");
                occupied.get().clone()
            }
        }
    };

    Ok(Some(BrokerHandle {
        doc_addr,
        actor,
        broker,
        manager: Some(manager),
    }))
}

impl BrokerHandle {
    pub async fn broker(&self) -> MutexGuard<'_, Broker> {
        self.broker.lock().await
    }
}

impl Drop for BrokerHandle {
    fn drop(&mut self) {
        let doc_addr = self.doc_addr.clone();
        let actor = std::mem::replace(&mut self.actor, b"".into());
        let manager = self.manager.take().unwrap();
        let broker = self.broker.clone();
        tokio::spawn(async move {
            let mut broker = broker.lock().await;
            let mut manager = manager.lock().await;

            if broker.remove_client(&actor) == 0 {
                info!(?doc_addr, %actor, "last actor disconnected, freeing up broker");
                manager.brokers.remove(&doc_addr);
            }
        });
    }
}

impl Broker {
    pub fn new(automerge: Automerge) -> Self {
        Self {
            automerge,
            clients: Default::default(),
        }
    }

    /// Generates a sync message to send back to the client
    pub fn add_client(
        &mut self,
        actor: ActorId,
        sync_tx: tokio::sync::mpsc::Sender<AmMessage>,
    ) -> DomainResult<Option<AmMessage>> {
        let mut client = BrokerClient {
            actor,
            state: State::new(),
            sync_tx,
        };
        let message = self.automerge.generate_sync_message(&mut client.state);

        self.clients.insert(client.actor.clone(), client);

        Ok(message)
    }

    /// Returns the number of remaining clients
    pub fn remove_client(&mut self, actor: &ActorId) -> usize {
        self.clients.remove(actor);
        self.clients.len()
    }

    /// TODO: can we validate that the new changes from an actor only originates from that ActorId?
    pub fn dispatch(
        &mut self,
        actor: &ActorId,
        message: AmMessage,
    ) -> DomainResult<Option<Dispatch>> {
        let Some(client) = self.clients.get_mut(actor) else {
            return Ok(None);
        };

        let heads = self.automerge.get_heads();

        self.automerge
            .receive_sync_message(&mut client.state, message)
            .map_err(|_err| DomainError::data_store("not syncable"))?;

        let mut response = Dispatch {
            response: None,
            broadcasts: vec![],
            incremental_changes: self.automerge.save_after(&heads),
        };

        for (client_actor_id, client) in &mut self.clients {
            let Some(message) = self.automerge.generate_sync_message(&mut client.state) else {
                continue;
            };

            if client_actor_id == actor {
                response.response = Some(message);
            } else {
                response.broadcasts.push(MessageBroadcast {
                    actor: client_actor_id.clone(),
                    message,
                    sync_tx: client.sync_tx.clone(),
                });
            }
        }

        Ok(Some(response))
    }
}
