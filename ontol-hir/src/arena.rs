use crate::{Kind, Lang, Node};

#[derive(Clone)]
pub struct Arena<'a, L: Lang> {
    nodes: Vec<L::Meta<'a, Kind<'a, L>>>,
}

impl<'a, L: Lang> Arena<'a, L> {
    pub fn add(&mut self, kind: L::Meta<'a, Kind<'a, L>>) -> Node {
        let idx = self.nodes.len();
        self.nodes.push(kind);
        Node(idx as u32)
    }

    pub fn pre_allocator(&self) -> PreAllocator {
        PreAllocator {
            node: Node(self.nodes.len() as u32),
        }
    }

    pub fn node_ref(&self, node: Node) -> NodeRef<'_, 'a, L> {
        NodeRef { arena: self, node }
    }

    pub fn node_mut(&mut self, node: Node) -> NodeMut<'_, 'a, L> {
        NodeMut { arena: self, node }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut L::Meta<'a, Kind<'a, L>>> {
        self.nodes.iter_mut()
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
        Arena { nodes: vec![] }
    }
}

pub struct NodeRef<'h, 'a, L: Lang> {
    pub arena: &'h Arena<'a, L>,
    node: Node,
}

impl<'h, 'a, L: Lang> NodeRef<'h, 'a, L> {
    pub fn meta(&self) -> &'h L::Meta<'a, Kind<'a, L>> {
        &self.arena.nodes[self.node.0 as usize]
    }

    pub fn kind(&self) -> &'h Kind<'a, L> {
        L::inner(&self.arena.nodes[self.node.0 as usize])
    }
}

pub struct NodeMut<'h, 'a, L: Lang> {
    pub arena: &'h mut Arena<'a, L>,
    node: Node,
}

impl<'h, 'a, L: Lang> NodeMut<'h, 'a, L> {
    pub fn meta(&mut self) -> &mut L::Meta<'a, Kind<'a, L>> {
        &mut self.arena.nodes[self.node.0 as usize]
    }
}
