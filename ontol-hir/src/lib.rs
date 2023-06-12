use std::{fmt::Debug, ops::Index};

use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

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

#[derive(Clone, Copy, Debug)]
pub struct Binder(pub Var);

impl From<Var> for Binder {
    fn from(value: Var) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct Label(pub u32);

pub trait Lang: Sized + Copy {
    type Node<'a>: Sized + Node<'a, Self>;

    fn make_node<'a>(&self, kind: Kind<'a, Self>) -> Self::Node<'a>;
}

pub trait Node<'a, L: Lang> {
    fn kind(&self) -> &Kind<'a, L>;
    fn kind_mut(&mut self) -> &mut Kind<'a, L>;
}

type Nodes<'a, L> = Vec<<L as Lang>::Node<'a>>;

#[derive(Clone)]
pub enum Kind<'a, L: Lang> {
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

impl<'a, L: Lang> Node<'a, L> for Kind<'a, L> {
    fn kind(&self) -> &Kind<'a, L> {
        self
    }

    fn kind_mut(&mut self) -> &mut Kind<'a, L> {
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
    Attr(Binding, Binding),
    /// (seq $val)
    /// The sequence is captured in $val, relation is ignored
    Seq(Binding),
    /// The property is absent
    Absent,
}

#[derive(Clone, Copy, Debug)]
pub enum Binding {
    Wildcard,
    Binder(Var),
}

#[derive(Clone)]
pub struct IterBinder {
    pub seq: Binding,
    pub rel: Binding,
    pub val: Binding,
}
