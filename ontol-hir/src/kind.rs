use std::ops::Index;

use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{Binder, Label, Lang, Node, Var};

type Nodes<'a, L> = Vec<<L as Lang>::Node<'a>>;

#[derive(Clone)]
pub enum NodeKind<'a, L: Lang> {
    Var(Var),
    Unit,
    Int(i64),
    Let(Binder, Box<L::Node<'a>>, Nodes<'a, L>),
    Call(BuiltinProc, Nodes<'a, L>),
    Map(Box<L::Node<'a>>),
    Seq(Label, Attribute<Box<L::Node<'a>>>),
    Struct(Binder, Nodes<'a, L>),
    Prop(Optional, Var, PropertyId, Vec<PropVariant<'a, L>>),
    MatchProp(Var, PropertyId, Vec<MatchArm<'a, L>>),
    Gen(Var, IterBinder, Nodes<'a, L>),
    Iter(Var, IterBinder, Nodes<'a, L>),
    Push(Var, Attribute<Box<L::Node<'a>>>),
}

impl<'a, L: Lang> Node<'a, L> for NodeKind<'a, L> {
    fn kind(&self) -> &NodeKind<'a, L> {
        self
    }

    fn kind_mut(&mut self) -> &mut NodeKind<'a, L> {
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Optional(pub bool);

#[derive(Clone)]
pub struct PropVariant<'a, L: Lang> {
    pub dimension: Dimension,
    pub attr: Attribute<Box<L::Node<'a>>>,
}

#[derive(Copy, Clone, Debug)]
pub enum Dimension {
    Singular,
    Seq(Label),
}

/// An attribute existing of (relation parameter, value)
#[derive(Clone, Debug)]
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

pub struct MatchArm<'a, L: Lang> {
    pub pattern: PropPattern,
    pub nodes: Vec<<L as Lang>::Node<'a>>,
}

// BUG: Why can't this be derived?
impl<'a, L: Lang> Clone for MatchArm<'a, L>
where
    <L as Lang>::Node<'a>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            pattern: self.pattern.clone(),
            nodes: self.nodes.clone(),
        }
    }
}

#[derive(Clone)]
pub enum PropPattern {
    /// ($rel $val)
    Attr(PatternBinding, PatternBinding),
    /// (seq $val)
    /// The sequence is captured in $val, relation is ignored
    Seq(PatternBinding),
    /// The property is absent
    Absent,
}

#[derive(Clone, Copy, Debug)]
pub enum PatternBinding {
    Wildcard,
    Binder(Var),
}

#[derive(Clone)]
pub struct IterBinder {
    pub seq: PatternBinding,
    pub rel: PatternBinding,
    pub val: PatternBinding,
}
