//! Rowan integration with the NodeView that works with ontol-compiler

use ontol_core::span::U32Span;
use ontol_parser::{
    cst::view::{Item, NodeView, TokenView},
    lexer::kind::Kind,
};
use rowan::{GreenNode, GreenNodeBuilder, GreenNodeData, GreenTokenData, Language};

use crate::OntolLang;

#[derive(Clone)]
pub struct GreenNodeView<'a>(pub &'a GreenNodeData);

pub fn empty_green_node(kind: Kind) -> GreenNode {
    let mut builder = GreenNodeBuilder::new();
    builder.start_node(rowan::SyntaxKind(kind as u16));
    builder.finish_node();
    builder.finish()
}

impl<'a> GreenNodeView<'a> {
    pub fn new_root(green: &'a GreenNode) -> Self {
        Self(green)
    }
}

#[derive(Clone)]
pub struct GreenTokenView<'a>(pub &'a GreenTokenData);

pub struct GreenChildren<'a>(rowan::Children<'a>);

impl<'a> NodeView for GreenNodeView<'a> {
    type Children = GreenChildren<'a>;
    type Token = GreenTokenView<'a>;

    fn kind(&self) -> Kind {
        OntolLang::kind_from_raw(self.0.kind())
    }

    fn children(&self) -> Self::Children {
        GreenChildren(self.0.children())
    }

    fn span_start(&self) -> u32 {
        0
    }

    fn span(&self) -> U32Span {
        U32Span::default()
    }
}

impl TokenView for GreenTokenView<'_> {
    fn kind(&self) -> Kind {
        OntolLang::kind_from_raw(self.0.kind())
    }

    fn span(&self) -> U32Span {
        U32Span::default()
    }

    fn slice(&self) -> &str {
        self.0.text()
    }
}

pub trait View {
    type View;

    fn view(self) -> Self::View;
}

impl<'a> Iterator for GreenChildren<'a> {
    type Item = Item<GreenNodeView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.0.next()? {
            rowan::NodeOrToken::Node(node) => Item::Node(GreenNodeView(node)),
            rowan::NodeOrToken::Token(token) => Item::Token(GreenTokenView(token)),
        })
    }
}
