use std::ops::Range;

use crate::lexer::{
    kind::Kind,
    unescape::{unescape_text_literal, UnescapeTextResult},
};

use super::inspect::Node;

pub trait NodeView<'a>: Sized + Copy {
    type Token: TokenView<'a>;
    type Children: Iterator<Item = Item<'a, Self>>;

    fn kind(&self) -> Kind;

    fn children(&self) -> Self::Children;
}

pub trait NodeViewExt<'a>: NodeView<'a> {
    fn node(self) -> Node<Self> {
        Node::from_view(self).unwrap()
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

    fn local_doc_comments(self) -> impl Iterator<Item = &'a str> {
        self.local_tokens_filter(Kind::DocComment)
            .map(|token| token.slice().strip_prefix("///").unwrap())
    }
}

impl<'a, T> NodeViewExt<'a> for T where T: NodeView<'a> {}

pub trait TokenView<'a>: Copy {
    fn kind(&self) -> Kind;

    fn span(&self) -> Range<usize>;

    fn slice(&self) -> &'a str;
}

pub trait TokenViewExt<'a>: TokenView<'a> {
    fn literal_text(&self) -> Option<UnescapeTextResult> {
        match self.kind() {
            kind @ (Kind::SingleQuoteText | Kind::DoubleQuoteText) => {
                Some(unescape_text_literal(kind, self.slice(), self.span()))
            }
            _ => None,
        }
    }
}

impl<'a, T: TokenView<'a>> TokenViewExt<'a> for T {}

pub enum Item<'a, N: NodeView<'a>> {
    Node(N),
    Token(N::Token),
}
