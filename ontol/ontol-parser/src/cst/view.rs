use std::ops::Range;

use chumsky::primitive::Container;
use logos::Source;

use crate::lexer::kind::Kind;

use super::tree::{FlatSyntaxTree, SyntaxNode};

pub trait NodeView<'a>: Sized {
    type Token;
    type Children: Iterator<Item = Item<'a, Self>>;

    fn kind(&self) -> Kind;

    fn children(&self) -> Self::Children;
}

pub trait NodeViewExt<'a>: NodeView<'a> {
    fn sub_nodes(&self) -> impl Iterator<Item = Self> {
        self.children().filter_map(|item| match item {
            Item::Node(node) => Some(node),
            Item::Token(_) => None,
        })
    }

    fn local_tokens(&self) -> impl Iterator<Item = Self::Token> {
        self.children().filter_map(|item| match item {
            Item::Token(token) => Some(token),
            Item::Node(node) => None,
        })
    }
}

impl<'a, T> NodeViewExt<'a> for T where T: NodeView<'a> {}

pub trait TokenView {
    fn kind(&self) -> Kind;

    fn span(&self) -> Range<usize>;

    fn slice(&self) -> &str;
}

pub enum Item<'a, N: NodeView<'a>> {
    Node(N),
    Token(N::Token),
}

#[derive(Clone, Copy)]
pub struct FlatNodeView<'a> {
    pub(super) tree: &'a FlatSyntaxTree,
    pub(super) kind: Kind,
    pub(super) pos: usize,
    pub(super) input: &'a str,
}

#[derive(Clone)]
pub struct FlatTokenView<'a> {
    index: usize,
    tree: &'a FlatSyntaxTree,
    input: &'a str,
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
            pos: self.pos + 1,
        }
    }
}

impl<'a> TokenView for FlatTokenView<'a> {
    fn kind(&self) -> Kind {
        self.tree.tokens[self.index]
    }

    fn span(&self) -> Range<usize> {
        self.tree.spans[self.index].clone()
    }

    fn slice(&self) -> &str {
        &self.input[self.span()]
    }
}

pub struct FlatChildren<'a> {
    view: FlatNodeView<'a>,
    pos: usize,
}

impl<'a> Iterator for FlatChildren<'a> {
    type Item = Item<'a, FlatNodeView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.view.tree.tree.get(self.pos)? {
            SyntaxNode::StartPlaceholder => panic!(),
            SyntaxNode::Start { kind } => {
                let node_index = self.pos;

                // advance read position, skip over all children.
                // now entering the subnode, but want to find the next sibling instead,
                // so start with depth = 1
                let mut depth = 1;
                self.pos += 1;
                loop {
                    match self.view.tree.tree.get(self.pos) {
                        None => {
                            break;
                        }
                        Some(SyntaxNode::Start { .. } | SyntaxNode::StartPlaceholder) => {
                            depth += 1;
                            self.pos += 1;
                        }
                        Some(SyntaxNode::Token { .. }) => {
                            self.pos += 1;
                        }
                        Some(SyntaxNode::End) => {
                            depth -= 1;
                            self.pos += 1;
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
                    input: self.view.input,
                }))
            }
            SyntaxNode::Token { index } => {
                let kind = self.view.tree.tokens[*index as usize];
                let span = self.view.tree.spans[*index as usize].clone();

                self.pos += 1;

                Some(Item::Token(FlatTokenView {
                    index: *index as usize,
                    tree: self.view.tree,
                    input: self.view.input,
                }))
            }
            SyntaxNode::End => None,
        }
    }
}
