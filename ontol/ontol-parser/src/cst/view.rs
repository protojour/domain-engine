use std::fmt::Display;

use crate::{
    lexer::{
        kind::{Kind, KindFilter},
        unescape::{unescape_text_literal, UnescapeTextResult},
    },
    U32Span,
};

use super::inspect::Node;

pub trait NodeView: Sized + Clone {
    type Token: TokenView;
    type Children: Iterator<Item = Item<Self>>;

    fn kind(&self) -> Kind;

    fn span_start(&self) -> u32;

    fn children(&self) -> Self::Children;

    fn span(&self) -> U32Span;
}

pub trait NodeViewExt: NodeView {
    fn node(&self) -> Node<Self> {
        Node::from_view(self.clone()).unwrap()
    }

    fn sub_nodes(self) -> impl Iterator<Item = Self> {
        self.children().filter_map(|item| match item {
            Item::Node(node) => Some(node),
            Item::Token(_) => None,
        })
    }

    fn local_tokens(self) -> impl Iterator<Item = Self::Token> {
        self.children().filter_map(|item| match item {
            Item::Token(token) => Some(token),
            Item::Node(_) => None,
        })
    }

    fn local_tokens_filter(self, filter: impl KindFilter) -> impl Iterator<Item = Self::Token> {
        self.local_tokens()
            .filter(move |token| filter.filter(token.kind()))
    }

    fn display(self) -> NodeDisplay<Self> {
        NodeDisplay(self)
    }

    fn non_trivia_span(&self) -> U32Span {
        let mut span = self.span();

        for child in self.children() {
            match child {
                Item::Token(_) => {
                    continue;
                }
                Item::Node(node)
                    if matches!(
                        node.kind(),
                        Kind::Whitespace | Kind::Comment | Kind::DocComment
                    ) =>
                {
                    continue;
                }
                Item::Node(node) => {
                    span.start = node.span_start();
                    return span;
                }
            }
        }

        span
    }
}

impl<T> NodeViewExt for T where T: NodeView {}

pub trait TokenView: Clone {
    fn kind(&self) -> Kind;

    fn span(&self) -> U32Span;

    fn slice(&self) -> &str;
}

pub trait TokenViewExt: TokenView {
    fn literal_text(&self) -> Option<UnescapeTextResult> {
        match self.kind() {
            kind @ (Kind::SingleQuoteText | Kind::DoubleQuoteText) => Some(unescape_text_literal(
                kind,
                self.slice(),
                self.span().into(),
            )),
            _ => None,
        }
    }
}

impl<T: TokenView> TokenViewExt for T {}

pub enum Item<N: NodeView> {
    Node(N),
    Token(N::Token),
}

pub struct NodeDisplay<V>(V);

impl<V: NodeView> Display for NodeDisplay<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for child in self.0.children() {
            match child {
                Item::Node(node) => {
                    write!(f, "{}", node.display())?;
                }
                Item::Token(token) => match token.kind() {
                    Kind::Comment | Kind::DocComment => {}
                    Kind::Whitespace => {
                        write!(f, " ")?;
                    }
                    _ => {
                        write!(f, "{}", token.slice())?;
                    }
                },
            }
        }

        Ok(())
    }
}
