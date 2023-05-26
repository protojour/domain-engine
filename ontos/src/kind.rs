use std::fmt::Display;

use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{Binder, Label, Lang, Node, Variable};

type Nodes<'a, L> = Vec<<L as Lang>::Node<'a>>;

#[derive(Clone)]
pub enum NodeKind<'a, L: Lang> {
    VariableRef(Variable),
    Unit,
    Int(i64),
    Let(Binder, Box<L::Node<'a>>, Nodes<'a, L>),
    Call(BuiltinProc, Nodes<'a, L>),
    Map(Box<L::Node<'a>>),
    Seq(Label, Attribute<Box<L::Node<'a>>>),
    Struct(Binder, Nodes<'a, L>),
    Prop(Variable, PropertyId, Vec<PropVariant<'a, L>>),
    MatchProp(Variable, PropertyId, Vec<MatchArm<'a, L>>),
    Gen(Variable, IterBinder, Nodes<'a, L>),
    Iter(Variable, IterBinder, Nodes<'a, L>),
    Push(Variable, Attribute<Box<L::Node<'a>>>),
}

impl<'a, L: Lang> Node<'a, L> for NodeKind<'a, L> {
    fn kind(&self) -> &NodeKind<'a, L> {
        self
    }

    fn kind_mut(&mut self) -> &mut NodeKind<'a, L> {
        self
    }
}

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

#[derive(Clone)]
pub struct Attribute<T> {
    pub rel: T,
    pub val: T,
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
    Present(Option<Seq>, PatternBinding, PatternBinding),
    NotPresent,
}

#[derive(Clone)]
pub struct Seq;

#[derive(Clone, Debug)]
pub enum PatternBinding {
    Wildcard,
    Binder(Variable),
}

#[derive(Clone)]
pub struct IterBinder {
    pub seq: Binder,
    pub rel: Binder,
    pub val: Binder,
}

impl Display for IterBinder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {}, {})", self.seq.0, self.rel.0, self.val.0)
    }
}
