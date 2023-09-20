use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};
use smartstring::alias::String;

use crate::{Attribute, HasDefault, Iter, Label, Optional, StructFlags, Var};

pub trait Lang: Sized + Copy {
    type Meta<'a, T>: Clone
    where
        T: Clone;
}

pub struct RootNode<'a, L: Lang> {
    arena: Arena<'a, L>,
    node: Node,
}

impl<'a, L: Lang> RootNode<'a, L> {
    pub fn new(node: Node, arena: Arena<'a, L>) -> Self {
        Self { arena, node }
    }

    pub fn node(&self) -> Node {
        self.node
    }

    pub fn as_ref(&self) -> NodeRef<'a, '_, L> {
        self.arena.node_ref(self.node)
    }

    pub fn arena_mut(&mut self) -> &mut Arena<'a, L> {
        &mut self.arena
    }
}

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

    pub fn node_ref(&self, node: Node) -> NodeRef<'a, '_, L> {
        NodeRef { arena: self, node }
    }

    pub fn node_mut(&mut self, node: Node) -> NodeMut<'a, '_, L> {
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

pub struct NodeRef<'a, 'b, L: Lang> {
    pub arena: &'b Arena<'a, L>,
    node: Node,
}

impl<'a, 'b, L: Lang> std::ops::Deref for NodeRef<'a, 'b, L> {
    type Target = L::Meta<'a, Kind<'a, L>>;

    fn deref(&self) -> &Self::Target {
        &self.arena.nodes[self.node.0 as usize]
    }
}

pub struct NodeMut<'a, 'b, L: Lang> {
    pub arena: &'b mut Arena<'a, L>,
    node: Node,
}

impl<'a, 'b, L: Lang> std::ops::Deref for NodeMut<'a, 'b, L> {
    type Target = L::Meta<'a, Kind<'a, L>>;

    fn deref(&self) -> &Self::Target {
        &self.arena.nodes[self.node.0 as usize]
    }
}

impl<'a, 'b, L: Lang> std::ops::DerefMut for NodeMut<'a, 'b, L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.arena.nodes[self.node.0 as usize]
    }
}

#[derive(Clone, Copy)]
pub struct Node(u32);

#[derive(Clone, Copy)]
pub struct Binder {
    pub var: Var,
}

impl From<Var> for Binder {
    fn from(value: Var) -> Self {
        Self { var: value }
    }
}

/// The syntax kind of a node.
#[derive(Clone)]
pub enum Kind<'a, L: Lang> {
    /// A variable reference.
    Var(Var),
    /// A unit value.
    Unit,
    /// A 64 bit signed integer
    I64(i64),
    /// A 64 bit float
    F64(f64),
    /// A string
    Text(String),
    /// Const procedure
    Const(DefId),
    /// A let expression
    Let(L::Meta<'a, Binder>, Node, Vec<Node>),
    /// A function call
    Call(BuiltinProc, Vec<Node>),
    /// A map call
    Map(Node),
    /// Standalone sequence in declarative mode.
    DeclSeq(L::Meta<'a, Label>, Attribute<Node>),
    /// A struct with associated binder. The value is the struct.
    Struct(L::Meta<'a, Binder>, StructFlags, Vec<Node>),
    // /// A property definition associated with a struct var in scope
    Prop(Optional, Var, PropertyId, Vec<PropVariant<'a, L>>),

    // /// A property matcher/unpacker associated with a struct var
    MatchProp(Var, PropertyId, Vec<(PropPattern<'a, L>, Vec<Node>)>),
    /// A sequence with associated binder. The value is the sequence.
    /// TODO: This can be done with Let!
    Sequence(L::Meta<'a, Binder>, Vec<Node>),
    /// Iterate attributes in sequence var,
    ForEach(Var, (Binding<'a, L>, Binding<'a, L>), Vec<Node>),
    /// Push an attribute to the end of a sequence
    SeqPush(Var, Attribute<Node>),
    /// Push the second string at the end of the first string
    StringPush(Var, Node),
    /// Declarative regex w/captures.
    /// If the label is defined, it is a looping regex
    Regex(
        Option<L::Meta<'a, Label>>,
        DefId,
        Vec<Vec<CaptureGroup<'a, L>>>,
    ),
    /// A regex matcher/unpacker
    MatchRegex(Iter, Var, DefId, Vec<CaptureMatchArm<'a, L>>),
}

#[derive(Clone)]
pub enum PropVariant<'a, L: Lang> {
    Singleton(Attribute<Node>),
    Seq(SeqPropertyVariant<'a, L>),
}

#[derive(Clone)]
pub struct SeqPropertyVariant<'a, L: Lang> {
    pub label: L::Meta<'a, Label>,
    pub has_default: HasDefault,
    pub elements: Vec<(Iter, Attribute<Node>)>,
}

#[derive(Clone)]
pub enum PropPattern<'a, L: Lang> {
    /// ($rel $val)
    Attr(Binding<'a, L>, Binding<'a, L>),
    /// (seq $val)
    /// The sequence is captured in $val, relation is ignored
    Seq(Binding<'a, L>, HasDefault),
    /// The property is absent
    Absent,
}

#[derive(Clone)]
pub enum Binding<'a, L: Lang> {
    Wildcard,
    Binder(L::Meta<'a, Binder>),
}

#[derive(Clone, Debug)]
pub struct CaptureGroup<'a, L: Lang> {
    pub index: u32,
    pub binder: L::Meta<'a, Binder>,
}

#[derive(Clone)]
pub struct CaptureMatchArm<'a, L: Lang> {
    pub capture_groups: Vec<CaptureGroup<'a, L>>,
    pub nodes: Vec<Node>,
}
