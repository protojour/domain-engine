use std::fmt::Display;

use domain_engine_core::{CrdtActor, VertexAddr};
use ontol_runtime::PropId;

pub mod broker;
pub mod doc_repository;
pub mod sync_session;
pub mod ws_codec;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct DocAddr(pub VertexAddr, pub PropId);

impl Display for DocAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{:?}", hex::encode(&self.0), self.1)
    }
}

pub trait ActorExt: Sized {
    fn deserialize_from_hex(hex: &str) -> Option<Self>;

    fn serialize_to_hex(&self) -> String;
}

impl ActorExt for CrdtActor {
    fn deserialize_from_hex(hex: &str) -> Option<Self> {
        let buf = hex::decode(hex).ok()?;
        postcard::from_bytes(&buf).ok()
    }

    fn serialize_to_hex(&self) -> String {
        hex::encode(postcard::to_allocvec(&self).unwrap())
    }
}
