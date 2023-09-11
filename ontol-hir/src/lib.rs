use std::{fmt::Debug, ops::Index};

use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};
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

impl From<Label> for Var {
    fn from(value: Label) -> Self {
        Var(value.0)
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
    type Label<'a>: Sized + Clone + GetLabel<'a, Self>;

    fn make_node<'a>(&self, kind: Kind<'a, Self>) -> Self::Node<'a>;
    fn make_binder<'a>(&self, var: Var) -> Self::Binder<'a>;
    fn make_label<'a>(&self, label: Label) -> Self::Label<'a>;
}

pub trait GetKind<'a, L: Lang> {
    fn kind(&self) -> &Kind<'a, L>;
    fn kind_mut(&mut self) -> &mut Kind<'a, L>;
}

pub trait GetVar<'a, L: Lang> {
    fn var(&self) -> &Var;
    fn var_mut(&mut self) -> &mut Var;
}

pub trait GetLabel<'a, L: Lang> {
    fn label(&self) -> &Label;
    fn label_mut(&mut self) -> &mut Label;
}

type Nodes<'a, L> = Vec<<L as Lang>::Node<'a>>;

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
    String(String),
    /// Const procedure
    Const(DefId),
    /// A let expression
    Let(L::Binder<'a>, Box<L::Node<'a>>, Nodes<'a, L>),
    /// A function call
    Call(BuiltinProc, Nodes<'a, L>),
    /// A map call
    Map(Box<L::Node<'a>>),
    /// Standalone sequence in declarative mode.
    DeclSeq(L::Label<'a>, Attribute<Box<L::Node<'a>>>),
    /// A struct with associated binder. The value is the struct.
    Struct(L::Binder<'a>, StructFlags, Nodes<'a, L>),
    /// A property definition associated with a struct var in scope
    Prop(Optional, Var, PropertyId, Vec<PropVariant<'a, L>>),
    /// A property matcher/unpacker associated with a struct var
    MatchProp(Var, PropertyId, Vec<PropMatchArm<'a, L>>),
    /// A sequence with associated binder. The value is the sequence.
    /// TODO: This can be done with Let!
    Sequence(L::Binder<'a>, Nodes<'a, L>),
    /// Iterate attributes in sequence var,
    ForEach(Var, (Binding<'a, L>, Binding<'a, L>), Nodes<'a, L>),
    /// Push an attribute to the end of a sequence
    SeqPush(Var, Attribute<Box<L::Node<'a>>>),
    /// Push the second string at the end of the first string
    StringPush(Var, Box<L::Node<'a>>),
    /// Declarative regex w/captures.
    /// If the label is defined, it is a looping regex
    Regex(Option<L::Label<'a>>, DefId, Vec<Vec<CaptureGroup<'a, L>>>),
    /// A regex matcher/unpacker
    MatchRegex(Var, DefId, Vec<CaptureMatchArm<'a, L>>),
}

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

pub enum PropVariant<'a, L: Lang> {
    Singleton(Attribute<Box<L::Node<'a>>>),
    Seq(SeqPropertyVariant<'a, L>),
}

impl<'a, L: Lang> Clone for PropVariant<'a, L>
where
    L::Node<'a>: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Singleton(attr) => Self::Singleton(attr.clone()),
            Self::Seq(seq) => Self::Seq(seq.clone()),
        }
    }
}

pub struct SeqPropertyVariant<'a, L: Lang> {
    pub label: L::Label<'a>,
    pub has_default: HasDefault,
    pub elements: Vec<SeqPropertyElement<'a, L>>,
}

impl<'a, L: Lang> Clone for SeqPropertyVariant<'a, L>
where
    L::Node<'a>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            label: self.label.clone(),
            has_default: self.has_default,
            elements: self.elements.clone(),
        }
    }
}

pub struct SeqPropertyElement<'a, L: Lang> {
    /// Is this an iterative binding (binds any number of elements)
    pub iter: bool,
    pub attribute: Attribute<L::Node<'a>>,
}

impl<'a, L: Lang> Clone for SeqPropertyElement<'a, L>
where
    L::Node<'a>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            iter: self.iter,
            attribute: self.attribute.clone(),
        }
    }
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

pub struct PropMatchArm<'a, L: Lang> {
    pub pattern: PropPattern<'a, L>,
    pub nodes: Vec<<L as Lang>::Node<'a>>,
}

impl<'a, L: Lang> Clone for PropMatchArm<'a, L>
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
    Seq(Binding<'a, L>, HasDefault),
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
            Self::Seq(b, has_default) => Self::Seq(b.clone(), *has_default),
            Self::Absent => Self::Absent,
        }
    }
}

pub struct CaptureMatchArm<'a, L: Lang> {
    pub capture_groups: Vec<CaptureGroup<'a, L>>,
    pub nodes: Vec<<L as Lang>::Node<'a>>,
}

impl<'a, L: Lang> Clone for CaptureMatchArm<'a, L>
where
    <L as Lang>::Binder<'a>: Clone,
    <L as Lang>::Node<'a>: Clone,
{
    fn clone(&self) -> Self {
        Self {
            capture_groups: self.capture_groups.clone(),
            nodes: self.nodes.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CaptureGroup<'a, L: Lang> {
    pub index: u32,
    pub binder: L::Binder<'a>,
}

#[derive(Clone, Copy, Debug)]
pub enum Binding<'a, L: Lang> {
    Wildcard,
    Binder(L::Binder<'a>),
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

impl<'a, L: Lang> GetLabel<'a, L> for Label {
    fn label(&self) -> &Label {
        self
    }

    fn label_mut(&mut self) -> &mut Label {
        self
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct StructFlags: u32 {
        const MATCH = 0b00000001;
    }
}
