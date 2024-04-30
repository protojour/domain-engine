use crate::{
    lexer::{
        kind::Kind,
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

    fn local_tokens_filter(self, kind: Kind) -> impl Iterator<Item = Self::Token> {
        self.local_tokens()
            .filter(move |token| token.kind() == kind)
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
