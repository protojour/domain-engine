use std::{fmt::Debug, ops::Index};

use arena::{Arena, NodeRef};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};
use smallvec::SmallVec;
use smartstring::alias::String;

pub mod arena;
pub mod display;
pub mod parse;
pub mod visitor;

#[cfg(test)]
mod tests;

pub trait Lang: Sized + Copy {
    /// The data to attach to each piece of ontol-hir information:
    type Data<'a, T>: Clone
    where
        T: Clone;

    fn default_data<'a, T: Clone>(&self, inner: T) -> Self::Data<'a, T>;

    fn inner<'m, T: Clone>(data: &'m Self::Data<'_, T>) -> &'m T;
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Var(pub u32);

impl From<u32> for Var {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<Label> for Var {
    fn from(value: Label) -> Self {
        Var(value.0)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct Label(pub u32);

#[derive(Default, Clone, Copy)]
pub struct Optional(pub bool);

impl Debug for Optional {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 {
            write!(f, "opt=t")
        } else {
            write!(f, "opt=f")
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct HasDefault(pub bool);

/// An attribute existing of (relation parameter, value)
#[derive(Clone, Copy, Debug)]
pub struct Attribute<T> {
    pub rel: T,
    pub val: T,
}

impl<R, V, T> From<(R, V)> for Attribute<T>
where
    T: From<R> + From<V>,
{
    fn from((rel, val): (R, V)) -> Self {
        Self {
            rel: rel.into(),
            val: val.into(),
        }
    }
}

impl<T> Index<usize> for Attribute<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        match index {
            0 => &self.rel,
            1 => &self.val,
            _ => panic!("Out of bounds for Attribute"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Iter(pub bool);

/// A RootNode owns its own Arena
#[derive(Clone)]
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

    pub fn data(&self) -> &L::Data<'a, Kind<'a, L>> {
        &self.arena[self.node]
    }

    pub fn split(self) -> (Arena<'a, L>, Node) {
        (self.arena, self.node)
    }

    pub fn as_ref(&self) -> NodeRef<'_, 'a, L> {
        self.arena.node_ref(self.node)
    }

    pub fn arena(&self) -> &Arena<'a, L> {
        &self.arena
    }

    pub fn arena_mut(&mut self) -> &mut Arena<'a, L> {
        &mut self.arena
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Node(u32);

#[derive(Clone, Copy, Debug)]
pub struct Binder {
    pub var: Var,
}

impl From<Var> for Binder {
    fn from(value: Var) -> Self {
        Self { var: value }
    }
}

pub type Nodes = SmallVec<[Node; 2]>;

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
    Let(L::Data<'a, Binder>, Node, Nodes),
    /// A function call
    Call(BuiltinProc, Nodes),
    /// A map call
    Map(Node),
    /// Standalone sequence in declarative mode.
    DeclSeq(L::Data<'a, Label>, Attribute<Node>),
    /// A struct with associated binder. The value is the struct.
    Struct(L::Data<'a, Binder>, StructFlags, Nodes),
    /// A property definition associated with a struct var in scope
    Prop(Optional, Var, PropertyId, SmallVec<[PropVariant<'a, L>; 1]>),
    /// A property matcher/unpacker associated with a struct var
    MatchProp(Var, PropertyId, SmallVec<[(PropPattern<'a, L>, Nodes); 1]>),
    /// A sequence with associated binder. The value is the sequence.
    /// TODO: This can be done with Let!
    Sequence(L::Data<'a, Binder>, Nodes),
    /// Iterate attributes in sequence var,
    ForEach(Var, (Binding<'a, L>, Binding<'a, L>), Nodes),
    /// Push an attribute to the end of a sequence
    SeqPush(Var, Attribute<Node>),
    /// Push the second string at the end of the first string
    StringPush(Var, Node),
    /// Declarative regex w/captures.
    /// If the label is defined, it is a looping regex
    Regex(
        Option<L::Data<'a, Label>>,
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
    pub label: L::Data<'a, Label>,
    pub has_default: HasDefault,
    pub elements: SmallVec<[(Iter, Attribute<Node>); 1]>,
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

#[derive(Clone, Copy)]
pub enum Binding<'a, L: Lang> {
    Wildcard,
    Binder(L::Data<'a, Binder>),
}

#[derive(Clone, Debug)]
pub struct CaptureGroup<'a, L: Lang> {
    pub index: u32,
    pub binder: L::Data<'a, Binder>,
}

#[derive(Clone)]
pub struct CaptureMatchArm<'a, L: Lang> {
    pub capture_groups: Vec<CaptureGroup<'a, L>>,
    pub nodes: Nodes,
}

#[derive(Debug)]
pub struct VarAllocator {
    next: Var,
}

impl VarAllocator {
    pub fn alloc(&mut self) -> Var {
        let next = self.next;
        self.next.0 += 1;
        next
    }

    pub fn peek_next(&self) -> &Var {
        &self.next
    }
}

impl Default for VarAllocator {
    fn default() -> Self {
        Self { next: Var(0) }
    }
}

impl From<Var> for VarAllocator {
    fn from(value: Var) -> Self {
        Self { next: value }
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct StructFlags: u32 {
        const MATCH = 0b00000001;
    }
}
