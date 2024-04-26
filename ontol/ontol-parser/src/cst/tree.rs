use std::fmt::Display;

use thin_vec::{thin_vec, ThinVec};

use crate::lexer::{kind::Kind, Lex};

use super::view::{Item, NodeView, TokenView};

/// A raw, flat tree that is the direct output of the CstParser.
///
/// It's very efficient for depth-first traversal.
pub struct FlatSyntaxTree {
    pub(super) markers: Vec<SyntaxMarker>,
    pub(super) lex: Lex,
}

/// A more condensed syntax tree which is more efficient for breadth-first traversal.
pub struct SyntaxTree {
    root: Syntax,
    lex: Lex,
}

/// Markers in the flat syntax tree
#[derive(Clone, PartialEq, Debug)]
pub enum SyntaxMarker {
    /// Placeholder node used for building the tree.
    /// The resulting tree should not contain this.
    StartPlaceholder,
    /// Marks the start of a node
    Start { kind: Kind },
    /// Marks the precense of a token
    Token { index: u32 },
    /// Marks the end of a node
    End,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Syntax {
    Node(SyntaxNode),
    Token { index: u32 },
}

#[derive(Clone, PartialEq, Debug)]
pub struct SyntaxNode {
    kind: Kind,
    start: usize,
    children: ThinVec<Syntax>,
}

impl FlatSyntaxTree {
    /// Create a proper tree out of the flat syntax tree
    pub fn unflatten(self) -> SyntaxTree {
        let mut parent_stack: Vec<SyntaxNode> = vec![];
        let mut cursor: usize = 0;
        let mut current_node = SyntaxNode {
            kind: Kind::Error,
            start: 0,
            children: thin_vec![],
        };

        for marker in self.markers.into_iter() {
            match marker {
                SyntaxMarker::StartPlaceholder => unreachable!(),
                SyntaxMarker::Start { kind } => {
                    parent_stack.push(current_node);
                    current_node = SyntaxNode {
                        kind,
                        start: cursor,
                        children: thin_vec![],
                    };
                }
                SyntaxMarker::Token { index } => {
                    current_node.children.push(Syntax::Token { index });
                    cursor = self.lex.span_end(index as usize);
                }
                SyntaxMarker::End => {
                    let mut parent = parent_stack.pop().unwrap();
                    parent.children.push(Syntax::Node(current_node));
                    current_node = parent;
                }
            }
        }

        SyntaxTree {
            root: current_node.children.into_iter().next().unwrap(),
            lex: self.lex,
        }
    }

    pub fn debug_tree<'a>(&'a self, src: &'a str) -> DebugTree<'a> {
        DebugTree {
            tree: self,
            src,
            verify: true,
            display_end: false,
        }
    }
}

impl SyntaxTree {
    pub fn view<'a>(&'a self, src: &'a str) -> TreeNodeView<'a> {
        TreeNodeView {
            node: match &self.root {
                Syntax::Node(node) => node,
                Syntax::Token { .. } => unreachable!(),
            },
            lex: &self.lex,
            src,
        }
    }
}

#[derive(Clone, Copy)]
pub struct TreeNodeView<'a> {
    node: &'a SyntaxNode,
    lex: &'a Lex,
    src: &'a str,
}

#[derive(Clone, Copy)]
pub struct TreeTokenView<'a> {
    index: u32,
    lex: &'a Lex,
    src: &'a str,
}

impl<'a> NodeView<'a> for TreeNodeView<'a> {
    type Token = TreeTokenView<'a>;
    type Children = TreeNodeChildren<'a>;

    fn kind(&self) -> Kind {
        self.node.kind
    }

    fn span_start(&self) -> usize {
        self.node.start
    }

    fn children(&self) -> Self::Children {
        TreeNodeChildren {
            iter: self.node.children.iter(),
            lex: self.lex,
            src: self.src,
        }
    }
}

impl<'a> TokenView<'a> for TreeTokenView<'a> {
    fn kind(&self) -> Kind {
        self.lex.tokens[self.index as usize]
    }

    fn slice(&self) -> &'a str {
        let span = self.span();
        &self.src[span]
    }

    fn span(&self) -> std::ops::Range<usize> {
        self.lex.span(self.index as usize)
    }
}

pub struct TreeNodeChildren<'a> {
    iter: std::slice::Iter<'a, Syntax>,
    lex: &'a Lex,
    src: &'a str,
}

impl<'a> Iterator for TreeNodeChildren<'a> {
    type Item = Item<'a, TreeNodeView<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(match self.iter.next()? {
            Syntax::Node(node) => Item::Node(TreeNodeView {
                node,
                lex: self.lex,
                src: self.src,
            }),
            Syntax::Token { index } => Item::Token(TreeTokenView {
                index: *index,
                lex: self.lex,
                src: self.src,
            }),
        })
    }
}

pub struct DebugTree<'a> {
    tree: &'a FlatSyntaxTree,
    src: &'a str,
    verify: bool,
    display_end: bool,
}

impl<'a> Display for DebugTree<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut indent: i64 = 0;
        for syntax in &self.tree.markers {
            match syntax {
                SyntaxMarker::StartPlaceholder => panic!(),
                SyntaxMarker::Start { kind } => {
                    for _ in 0..indent {
                        write!(f, "    ")?;
                    }
                    writeln!(f, "{kind:?}")?;
                    indent += 1;
                }
                SyntaxMarker::Token { index } => {
                    for _ in 0..indent {
                        write!(f, "    ")?;
                    }
                    let kind = self.tree.lex.tokens[*index as usize];
                    let span = self.tree.lex.span(*index as usize);

                    match kind {
                        Kind::Whitespace => {
                            writeln!(f, "{kind:?}")?;
                        }
                        _ => {
                            writeln!(f, "{kind:?} `{}`", &self.src[span])?;
                        }
                    }
                }
                SyntaxMarker::End => {
                    indent -= 1;
                    if self.display_end {
                        for _ in 0..indent {
                            write!(f, "    ")?;
                        }
                        writeln!(f, "End")?;
                    }
                }
            }
        }

        if self.verify {
            assert_eq!(indent, 0);
        }

        Ok(())
    }
}
