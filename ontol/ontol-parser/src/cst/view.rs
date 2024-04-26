use std::ops::Range;

use crate::lexer::{
    kind::Kind,
    unescape::{unescape_text_literal, UnescapeTextResult},
};

use super::{
    inspect::Node,
    tree::{FlatSyntaxTree, SyntaxNode},
};

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

#[derive(Clone, Copy)]
pub struct FlatNodeView<'a> {
    pub(super) tree: &'a FlatSyntaxTree,
    pub(super) kind: Kind,
    pub(super) pos: usize,
    pub(super) src: &'a str,
}

#[derive(Clone, Copy)]
pub struct FlatTokenView<'a> {
    index: usize,
    tree: &'a FlatSyntaxTree,
    src: &'a str,
}

impl<'a> NodeView<'a> for FlatNodeView<'a> {
    type Token = FlatTokenView<'a>;
    type Children = FlatChildren<'a>;

    fn kind(&self) -> Kind {
        self.kind
    }

    fn children(&self) -> FlatChildren<'a> {
        FlatChildren {
            view: *self,
            tree_pos: self.pos + 1,
        }
    }
}

impl<'a> TokenView<'a> for FlatTokenView<'a> {
    fn kind(&self) -> Kind {
        self.tree.lex.tokens[self.index]
    }

    fn span(&self) -> Range<usize> {
        self.tree.lex.span(self.index).clone()
    }

    fn slice(&self) -> &'a str {
        &self.src[self.span()]
    }
}

pub struct FlatChildren<'a> {
    view: FlatNodeView<'a>,
    tree_pos: usize,
}

impl<'a> Iterator for FlatChildren<'a> {
    type Item = Item<'a, FlatNodeView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.view.tree.tree.get(self.tree_pos)? {
            SyntaxNode::StartPlaceholder => panic!(),
            SyntaxNode::Start { kind } => {
                let node_index = self.tree_pos;

                // advance read position, skip over all children.
                // now entering the subnode, but want to find the next sibling instead,
                // so start with depth = 1
                let mut depth = 1;
                self.tree_pos += 1;
                loop {
                    match self.view.tree.tree.get(self.tree_pos) {
                        None => {
                            break;
                        }
                        Some(SyntaxNode::Start { .. } | SyntaxNode::StartPlaceholder) => {
                            depth += 1;
                            self.tree_pos += 1;
                        }
                        Some(SyntaxNode::Token { .. }) => {
                            self.tree_pos += 1;
                        }
                        Some(SyntaxNode::End) => {
                            depth -= 1;
                            self.tree_pos += 1;
                            if depth == 0 {
                                break;
                            }
                        }
                    }
                }

                Some(Item::Node(FlatNodeView {
                    tree: self.view.tree,
                    kind: *kind,
                    pos: node_index,
                    src: self.view.src,
                }))
            }
            SyntaxNode::Token { index } => {
                self.tree_pos += 1;

                Some(Item::Token(FlatTokenView {
                    index: *index as usize,
                    tree: self.view.tree,
                    src: self.view.src,
                }))
            }
            SyntaxNode::End => None,
        }
    }
}
