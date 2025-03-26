//! Rowan integration with the NodeView that works with ontol-compiler

use ontol_core::span::U32Span;
use ontol_parser::{
    cst::view::{Item, NodeView, TokenView},
    lexer::kind::Kind,
};
use rowan::{GreenNode, GreenNodeBuilder, TextRange};

use crate::OntolLang;

#[derive(Clone)]
pub struct RowanNodeView(pub rowan::SyntaxNode<OntolLang>);

impl RowanNodeView {
    pub fn new_root(green: GreenNode) -> Self {
        Self(rowan::SyntaxNode::new_root(green))
    }

    pub fn empty(kind: Kind) -> Self {
        let mut builder = GreenNodeBuilder::new();
        builder.start_node(rowan::SyntaxKind(kind as u16));
        builder.finish_node();

        Self(rowan::SyntaxNode::new_root(builder.finish()))
    }

    pub fn syntax(&self) -> &rowan::SyntaxNode<OntolLang> {
        &self.0
    }
}

#[derive(Clone)]
pub struct RowanTokenView(pub rowan::SyntaxToken<OntolLang>);

pub struct RowanChildren(rowan::SyntaxElementChildren<OntolLang>);

impl NodeView for RowanNodeView {
    type Children = RowanChildren;
    type Token = RowanTokenView;

    fn kind(&self) -> Kind {
        self.0.kind()
    }

    fn children(&self) -> Self::Children {
        RowanChildren(self.0.children_with_tokens())
    }

    fn span_start(&self) -> u32 {
        self.0.text_range().start().into()
    }

    fn span(&self) -> U32Span {
        range_to_span(self.0.text_range())
    }
}

impl TokenView for RowanTokenView {
    fn kind(&self) -> Kind {
        self.0.kind()
    }

    fn span(&self) -> U32Span {
        range_to_span(self.0.text_range())
    }

    fn slice(&self) -> &str {
        self.0.text()
    }
}

pub trait View {
    type View;

    fn view(self) -> Self::View;
}

impl View for rowan::SyntaxNode<OntolLang> {
    type View = RowanNodeView;

    fn view(self) -> Self::View {
        RowanNodeView(self)
    }
}

impl View for rowan::SyntaxToken<OntolLang> {
    type View = RowanTokenView;

    fn view(self) -> Self::View {
        RowanTokenView(self)
    }
}

impl View for rowan::SyntaxElement<OntolLang> {
    type View = Item<RowanNodeView>;

    fn view(self) -> Self::View {
        match self {
            rowan::NodeOrToken::Node(node) => Item::Node(RowanNodeView(node)),
            rowan::NodeOrToken::Token(token) => Item::Token(RowanTokenView(token)),
        }
    }
}

impl Iterator for RowanChildren {
    type Item = Item<RowanNodeView>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.0.next()? {
            rowan::NodeOrToken::Node(node) => Item::Node(RowanNodeView(node)),
            rowan::NodeOrToken::Token(token) => Item::Token(RowanTokenView(token)),
        })
    }
}

fn range_to_span(range: TextRange) -> U32Span {
    U32Span {
        start: range.start().into(),
        end: range.end().into(),
    }
}
