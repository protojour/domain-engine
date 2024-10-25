use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use automerge::{
    sync::{Message as AmMessage, State, SyncDoc},
    ActorId, Automerge,
};
use domain_engine_core::{domain_error::DomainErrorKind, DomainError, DomainResult, Session};
use ontol_runtime::DefId;
use tokio::sync::{Mutex, MutexGuard};
use tracing::info;

use super::{doc_repository::DocRepository, DocAddr};

#[derive(Clone, Default)]
pub struct BrokerManagerHandle {
    pub(super) manager: Arc<Mutex<BrokerManager>>,
}

#[derive(Default)]
pub struct BrokerManager {
    pub(super) brokers: BTreeMap<DocAddr, Arc<Mutex<Broker>>>,
}

/// A handle to a document broker represents a connected actor
pub struct BrokerHandle {
    actor: ActorId,
    broker: Arc<Mutex<Broker>>,
    manager: Option<BrokerManagerHandle>,
}

/// An automerge sync for one document broker with a set of clients
pub struct Broker {
    doc_addr: DocAddr,
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
    resource_def_id: DefId,
    doc_addr: DocAddr,
    actor: ActorId,
    manager: BrokerManagerHandle,
    doc_repository: DocRepository,
    session: Session,
) -> DomainResult<Option<BrokerHandle>> {
    let broker = {
        let mut manager = manager.manager.lock().await;
        match manager.brokers.entry(doc_addr.clone()) {
            Entry::Vacant(vacant) => {
                info!("broker: first actor connected, loading document and broker");

                let Some(automerge) = doc_repository
                    .load(resource_def_id, doc_addr.clone(), session)
                    .await?
                else {
                    return Ok(None);
                };

                vacant
                    .insert(Arc::new(Mutex::new(Broker::new(doc_addr, automerge))))
                    .clone()
            }
            Entry::Occupied(occupied) => {
                info!("broker: another actor connected, reusing broker");
                occupied.get().clone()
            }
        }
    };

    {
        let broker = broker.lock().await;
        if broker.clients.contains_key(&actor) {
            return Err(DomainErrorKind::BadInputData(format!(
                "actor {actor} was already registered"
            ))
            .into_error());
        }
    }

    Ok(Some(BrokerHandle {
        actor,
        broker,
        manager: Some(manager),
    }))
}

impl BrokerHandle {
    pub async fn broker(&self) -> MutexGuard<'_, Broker> {
        self.broker.lock().await
    }

    /// must call this to clean up the broker
    /// It's not an `impl Drop` for two reasons:
    /// 1. Drop can not run async code
    /// 2. Drop runs when Rust is freeing up stuff,
    ///    tracing spans also get freed up and sometimes that means the spans are gone.
    pub async fn async_drop(mut self) {
        let actor = std::mem::replace(&mut self.actor, b"".into());
        let manager = self.manager.take().unwrap();
        let broker = self.broker.clone();
        let mut broker = broker.lock().await;
        let mut manager = manager.manager.lock().await;

        if broker.remove_client(&actor) == 0 {
            info!("broker: last actor disconnected, freeing up broker");
            manager.brokers.remove(&broker.doc_addr);
        }
    }
}

impl Broker {
    pub fn new(doc_addr: DocAddr, automerge: Automerge) -> Self {
        Self {
            doc_addr,
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
        info!("broker: add client");

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
        info!("broker: remove client");
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
