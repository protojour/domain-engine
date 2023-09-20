use crate::{Kind, Lang, Node, RootNode};

#[derive(Clone)]
pub struct Arena<'a, L: Lang> {
    entries: Vec<L::Data<'a, Kind<'a, L>>>,
}

impl<'a, L: Lang> Arena<'a, L> {
    pub fn add(&mut self, kind: L::Data<'a, Kind<'a, L>>) -> Node {
        let idx = self.entries.len();
        self.entries.push(kind);
        Node(idx as u32)
    }

    pub fn add_root(mut self, kind: L::Data<'a, Kind<'a, L>>) -> RootNode<'a, L> {
        let node = self.add(kind);
        RootNode::new(node, self)
    }

    pub fn pre_allocator(&self) -> PreAllocator {
        PreAllocator {
            node: Node(self.entries.len() as u32),
        }
    }

    pub fn kind(&self, node: Node) -> &Kind<'a, L> {
        L::inner(&self[node])
    }

    pub fn node_ref(&self, node: Node) -> NodeRef<'_, 'a, L> {
        NodeRef { arena: self, node }
    }

    pub fn iter(&self) -> impl Iterator<Item = &L::Data<'a, Kind<'a, L>>> {
        self.entries.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut L::Data<'a, Kind<'a, L>>> {
        self.entries.iter_mut()
    }

    pub fn refs<'n>(
        &self,
        nodes: impl IntoIterator<Item = &'n Node>,
    ) -> impl Iterator<Item = NodeRef<'_, 'a, L>> {
        nodes.into_iter().cloned().map(|node| self.node_ref(node))
    }
}

impl<'a, L: Lang> std::ops::Index<Node> for Arena<'a, L> {
    type Output = L::Data<'a, Kind<'a, L>>;

    fn index(&self, node: Node) -> &Self::Output {
        &self.entries[node.0 as usize]
    }
}

impl<'a, L: Lang> std::ops::IndexMut<Node> for Arena<'a, L> {
    fn index_mut(&mut self, node: Node) -> &mut Self::Output {
        &mut self.entries[node.0 as usize]
    }
}

pub struct PreAllocator {
    node: Node,
}

impl PreAllocator {
    pub fn prealloc_node(&mut self) -> Node {
        let node = self.node;
        self.node.0 += 1;
        node
    }
}

impl<'a, L: Lang> Default for Arena<'a, L> {
    fn default() -> Self {
        Arena {
            entries: Vec::with_capacity(16),
        }
    }
}

#[derive(Clone, Copy)]
pub struct NodeRef<'h, 'a, L: Lang> {
    pub(crate) arena: &'h Arena<'a, L>,
    pub(crate) node: Node,
}

impl<'h, 'a, L: Lang> NodeRef<'h, 'a, L> {
    pub fn arena(&self) -> &'h Arena<'a, L> {
        self.arena
    }

    pub fn node(&self) -> Node {
        self.node
    }
}

impl<'h, 'a, L: Lang> std::ops::Deref for NodeRef<'h, 'a, L> {
    type Target = L::Data<'a, Kind<'a, L>>;

    fn deref(&self) -> &Self::Target {
        &self.arena.entries[self.node.0 as usize]
    }
}
