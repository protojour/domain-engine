use ontol_runtime::vm::proc::BuiltinProc;
use smartstring::alias::String;

use crate::{Lang, Node};

type Nodes<'a, O> = Vec<<O as Lang>::Node<'a>>;

pub enum NodeKind<'a, L: Lang> {
    VariableRef(Variable),
    Unit,
    Int(i64),
    Call(BuiltinProc, Nodes<'a, L>),
    MapSeq(Variable, Binder, Nodes<'a, L>),
    Struct(Binder, Nodes<'a, L>),
    Prop(Variable, String, Box<L::Node<'a>>, Box<L::Node<'a>>),
    Destruct(Variable, Nodes<'a, L>),
    MatchProp(Variable, String, Vec<MatchArm<'a, L>>),
}

impl<'a, L: Lang> Node<'a, L> for NodeKind<'a, L> {
    fn kind(&self) -> &NodeKind<'a, L> {
        self
    }
}

#[derive(Debug)]
pub struct Variable(pub u32);

#[derive(Debug)]
pub struct Binder(pub Variable);

#[derive(Debug)]
pub struct MatchArm<'a, L: Lang> {
    pub pattern: PropPattern,
    pub node: L::Node<'a>,
}

#[derive(Debug)]
pub enum PropPattern {
    Present(PatternBinding, PatternBinding),
    NotPresent,
}

#[derive(Debug)]
pub enum PatternBinding {
    Wildcard,
    Binder(Variable),
}
