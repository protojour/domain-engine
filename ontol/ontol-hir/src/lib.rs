#![forbid(unsafe_code)]

use std::fmt::{Debug, Display};

use arena::{Arena, NodeRef};
use ontol_runtime::{
    property::PropertyId,
    query::condition::{ClausePair, SetOperator},
    value::Attribute,
    var::Var,
    vm::proc::BuiltinProc,
    DefId,
};
use smallvec::SmallVec;
use thin_vec::ThinVec;

pub mod arena;
pub mod display;
pub mod parse;
pub mod visitor;

#[cfg(test)]
mod tests;

/// ontol_hir is a generic language, it lets the implementor of a "dialect" specify
/// what kind of data to attach to each AST node.
pub trait Lang: Sized + Copy {
    /// The data to attach to each piece of ontol-hir information.
    ///
    /// The data has to be generic, because it can attach to different kinds of ontol_hir nodes.
    type Data<'a, H>: Clone
    where
        H: Clone;

    /// Wrapping the given ontol_hir data T in Lang-specific Data.
    fn default_data<'a, H: Clone>(&self, hir: H) -> Self::Data<'a, H>;

    /// Extract the ontol-hir part of the data
    fn as_hir<'a, H: Clone>(data: &'a Self::Data<'_, H>) -> &'a H;
}

impl From<Label> for Var {
    fn from(value: Label) -> Self {
        Var(value.0)
    }
}

/// A Label is an identifier for some code location.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct Label(pub u32);

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

    pub fn kind(&self) -> &Kind<'a, L> {
        L::as_hir(self.data())
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

/// A binder is syntactic element that introduces a variable,
/// much like a function parameter.
///
/// It's usually within parentheses: `($x)`
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
    /// Do nothing
    NoOp,
    /// A variable reference.
    Var(Var),
    /// Block - a sequence of nodes where the last node is the value.
    /// Blocks support local variable scope.
    Block(Nodes),
    /// Try block
    /// Statements within may refer to the try label to immediately exit the try block
    /// with a #void value
    Catch(Label, Nodes),
    /// Try a variable. Exists labelled try block if #void.
    Try(Label, Var),
    /// Bind expression to a variable.
    Let(L::Data<'a, Binder>, Node),
    /// Tries to bind expression to a variable. Exits labelled try block if #void.
    TryLet(Label, L::Data<'a, Binder>, Node),
    /// Defines two variables from a struct property
    LetProp(Attribute<Binding<'a, L>>, (Var, PropertyId)),
    /// Defines two variables from a struct property, or a default attribute if not present
    LetPropDefault(
        Attribute<Binding<'a, L>>,
        (Var, PropertyId),
        Attribute<Node>,
    ),
    /// Tries to define two variables from a struct propery, and exits to the try label if unsuccessful.
    TryLetProp(Label, Attribute<Binding<'a, L>>, (Var, PropertyId)),
    /// Unpack a tuple-like sequence to bind each element to a variable
    TryLetTup(Label, ThinVec<Binding<'a, L>>, Node),
    LetRegex(ThinVec<ThinVec<CaptureGroup<'a, L>>>, DefId, Var),
    LetRegexIter(
        L::Data<'a, Binder>,
        ThinVec<ThinVec<CaptureGroup<'a, L>>>,
        DefId,
        Var,
    ),
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
    /// A with expression. Introduces one binding and executes a block.
    /// TODO: Superseded by let statements in blocks
    With(L::Data<'a, Binder>, Node, Nodes),
    /// A function call
    Call(BuiltinProc, Nodes),
    /// A map call
    Map(Node),
    /// Narrowing of a type, e.g. selecting a specific union variant.
    Narrow(Node),
    /// A set-builder of set entries
    Set(SmallVec<[SetEntry<'a, L>; 1]>),
    /// A struct with associated binder. The value is the struct.
    Struct(L::Data<'a, Binder>, StructFlags, Nodes),
    /// A property definition associated with a struct var in scope
    Prop(PropFlags, Var, PropertyId, PropVariant),
    /// Move rest of attributes into the first var, from the second var
    MoveRestAttrs(Var, Var),
    /// A sequence with associated binder. The value is the sequence.
    /// TODO: This can be done with Let!
    MakeSeq(L::Data<'a, Binder>, Nodes),
    /// Copy a SubSequence into the the first variable, copied from the second variable
    CopySubSeq(Var, Var),
    /// Iterate attributes in sequence var,
    ForEach(Var, (Binding<'a, L>, Binding<'a, L>), Nodes),
    /// Push an attribute to the end of a sequence
    Insert(Var, Attribute<Node>),
    /// Push the second string at the end of the first string
    StringPush(Var, Node),
    /// Declarative regex w/captures.
    /// If the label is defined, it is a looping regex
    Regex(
        Option<L::Data<'a, Label>>,
        DefId,
        ThinVec<ThinVec<CaptureGroup<'a, L>>>,
    ),
    LetCondVar(Var, Var),
    PushCondClauses(Var, ThinVec<ClausePair<Var, EvalCondTerm>>),
}

#[derive(Clone)]
pub enum PropVariant {
    Value(Attribute<Node>),
    Predicate(SetOperator, Node),
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

/// Evaluated version of ontol_runtime::condition::CondTerm
#[derive(Clone, Copy)]
pub enum EvalCondTerm {
    /// Ignored
    Wildcard,
    /// Quoted var, i.e. not evaluated
    QuoteVar(Var),
    /// Evaluate var into a CondTerm::Value
    Eval(Node),
}

#[derive(Clone)]
pub struct SetEntry<'a, L: Lang>(pub Option<L::Data<'a, Label>>, pub Attribute<Node>);

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct PropFlags: u8 {
        const PAT_OPTIONAL  = 0b00000001;
        const REL_OPTIONAL  = 0b00000010;
    }
}

impl PropFlags {
    pub fn pat_optional(self) -> bool {
        self.contains(Self::PAT_OPTIONAL)
    }

    pub fn rel_optional(self) -> bool {
        self.contains(Self::REL_OPTIONAL)
    }
}

impl Display for PropFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{pat}{rel}",
            pat = if self.pat_optional() { "?" } else { "" },
            rel = if self.rel_optional() { "" } else { "!" }
        )
    }
}

bitflags::bitflags! {
    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Debug)]
    pub struct StructFlags: u8 {
        const MATCH = 0b00000001;
    }
}

pub fn find_value_node<'h, 'a, L: Lang>(
    node_ref: NodeRef<'h, 'a, L>,
) -> Option<NodeRef<'h, 'a, L>> {
    match L::as_hir(&node_ref) {
        Kind::Block(nodes) | Kind::Catch(_, nodes) => {
            let last = nodes.last()?;
            find_value_node(node_ref.arena().node_ref(*last))
        }
        _ => Some(node_ref),
    }
}
