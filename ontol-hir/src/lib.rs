use std::{fmt::Debug, ops::Index};

use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use smartstring::alias::String;

pub mod display;
pub mod parse;
pub mod visitor;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Var(pub u32);

impl From<u32> for Var {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct Label(pub u32);

/// ontol_hir is a generic language, it does not specify the type of its language nodes.
///
/// Implementing this trait makes an ontol_hir "dialect" - the implementor may freely choose
/// what kind of metadata to attach to each node.
pub trait Lang: Sized + Copy {
    type Node<'a>: Sized + GetKind<'a, Self>;
    type Binder<'a>: Sized + GetVar<'a, Self>;

    fn make_node<'a>(&self, kind: Kind<'a, Self>) -> Self::Node<'a>;
    fn make_binder<'a>(&self, var: Var) -> Self::Binder<'a>;
}

pub trait GetKind<'a, L: Lang> {
    fn kind(&self) -> &Kind<'a, L>;
    fn kind_mut(&mut self) -> &mut Kind<'a, L>;
}

pub trait GetVar<'a, L: Lang> {
    fn var(&self) -> &Var;
    fn var_mut(&mut self) -> &mut Var;
}

type Nodes<'a, L> = Vec<<L as Lang>::Node<'a>>;

/// The syntax kind of a node.
#[derive(Clone)]
pub enum Kind<'a, L: Lang> {
    Var(Var),
    Unit,
    Int(i64),
    String(String),
    Let(L::Binder<'a>, Box<L::Node<'a>>, Nodes<'a, L>),
    Call(BuiltinProc, Nodes<'a, L>),
    Map(Box<L::Node<'a>>),
    Seq(Label, Attribute<Box<L::Node<'a>>>),
    Struct(L::Binder<'a>, Nodes<'a, L>),
    Prop(Optional, Var, PropertyId, Vec<PropVariant<'a, L>>),
    MatchProp(Var, PropertyId, Vec<MatchArm<'a, L>>),
    Gen(Var, IterBinder<'a, L>, Nodes<'a, L>),
    Iter(Var, IterBinder<'a, L>, Nodes<'a, L>),
    Push(Var, Attribute<Box<L::Node<'a>>>),
}

impl<'a, L: Lang> GetKind<'a, L> for Kind<'a, L> {
    fn kind(&self) -> &Kind<'a, L> {
        self
    }

    fn kind_mut(&mut self) -> &mut Kind<'a, L> {
        self
    }
}

impl<'a, L: Lang> GetVar<'a, L> for Var {
    fn var(&self) -> &Var {
        self
    }

    fn var_mut(&mut self) -> &mut Var {
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
    pub pattern: PropPattern<'a, L>,
    pub nodes: Vec<<L as Lang>::Node<'a>>,
}

impl<'a, L: Lang> Clone for MatchArm<'a, L>
where
    <L as Lang>::Binder<'a>: Clone,
    <L as Lang>::Node<'a>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            pattern: self.pattern.clone(),
            nodes: self.nodes.clone(),
        }
    }
}

pub enum PropPattern<'a, L: Lang> {
    /// ($rel $val)
    Attr(Binding<'a, L>, Binding<'a, L>),
    /// (seq $val)
    /// The sequence is captured in $val, relation is ignored
    Seq(Binding<'a, L>),
    /// The property is absent
    Absent,
}

impl<'a, L: Lang> Clone for PropPattern<'a, L>
where
    <L as Lang>::Binder<'a>: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Attr(rel, val) => Self::Attr(rel.clone(), val.clone()),
            Self::Seq(b) => Self::Seq(b.clone()),
            Self::Absent => Self::Absent,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Binding<'a, L: Lang> {
    Wildcard,
    Binder(L::Binder<'a>),
}

pub struct IterBinder<'a, L: Lang> {
    pub seq: Binding<'a, L>,
    pub rel: Binding<'a, L>,
    pub val: Binding<'a, L>,
}

impl<'a, L: Lang> Clone for IterBinder<'a, L>
where
    <L as Lang>::Binder<'a>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            seq: self.seq.clone(),
            rel: self.rel.clone(),
            val: self.val.clone(),
        }
    }
}

pub struct VarAllocator {
    next: Var,
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

impl VarAllocator {
    pub fn alloc(&mut self) -> Var {
        let next = self.next;
        self.next.0 += 1;
        next
    }
}
