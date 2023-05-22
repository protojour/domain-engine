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
    Seq(Binder, Nodes<'a, L>),
    Struct(Binder, Nodes<'a, L>),
    Prop(Variable, PropertyId, PropVariant<'a, L>),
    MapSeq(Variable, Binder, Nodes<'a, L>),
    MatchProp(Variable, PropertyId, Vec<MatchArm<'a, L>>),
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
    pub rel: Box<L::Node<'a>>,
    pub val: Box<L::Node<'a>>,
}

#[derive(Clone)]
pub enum Dimension {
    Singular,
    Sequence(Label),
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
    Present(PatternBinding, PatternBinding),
    NotPresent,
}

#[derive(Clone, Debug)]
pub enum PatternBinding {
    Wildcard,
    Binder(Variable),
}
