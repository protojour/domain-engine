use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arcstr::ArcStr;
use ontol_parser::lexer::kind::Kind;
use thin_vec::{ThinVec, thin_vec};

struct Registry {
    child_buckets: HashMap<u64, Vec<Arc<ChildList>>>,
    empty_children: Arc<ChildList>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            child_buckets: Default::default(),
            empty_children: Arc::new(ChildList {
                list: thin_vec![],
                hash: 0,
                index: 0,
            }),
        }
    }
}

#[derive(Clone)]
pub struct Node {
    kind: Kind,
    children: Arc<ChildList>,
    registry: Arc<Mutex<Registry>>,
}

pub enum NodeOrToken {
    Node(Arc<Node>),
    Token(ArcStr),
}

struct ChildList {
    list: ThinVec<NodeOrToken>,
    hash: u64,
    index: u16,
}

impl Node {
    fn children(&self) -> &[NodeOrToken] {
        &self.children.list
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let c_hash = self.children.hash;
        let c_index = self.children.index;

        let mut registry = self.registry.lock().unwrap();
        self.children = registry.empty_children.clone();

        if let Some(bucket) = registry.child_buckets.get_mut(&c_hash) {
            bucket.retain(|list| Arc::strong_count(list) > 1);
        }
    }
}
