use automerge::ActorId;
use domain_engine_core::VertexAddr;
use ontol_runtime::PropId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod broker;
pub mod doc_repository;
pub mod sync_session;

mod ws_codec;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct DocAddr(pub VertexAddr, pub PropId);

#[derive(Serialize, Deserialize)]
pub struct CrdtActor {
    pub actor_id: Uuid,
    pub user_id: String,
}

impl CrdtActor {
    pub fn deserialize_from_hex(hex: &str) -> Result<Self, ()> {
        let buf = hex::decode(hex).map_err(|_| ())?;
        postcard::from_bytes(&buf).map_err(|_| ())
    }

    pub fn serialize_to_hex(&self) -> String {
        hex::encode(postcard::to_allocvec(&self).unwrap())
    }

    pub fn to_automerge_actor_id(&self) -> ActorId {
        let buf = postcard::to_allocvec(&self).unwrap();
        ActorId::from(buf)
    }
}
