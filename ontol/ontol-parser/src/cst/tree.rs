use std::{fmt::Display, ops::Range};

use crate::lexer::{kind::Kind, LexedSource};

use super::view::FlatNodeView;

pub struct FlatSyntaxTree {
    pub(super) tree: Vec<SyntaxNode>,
    pub(super) lex: LexedSource,
}

#[derive(Clone, PartialEq, Debug)]
pub enum SyntaxNode {
    StartPlaceholder,
    Start { kind: Kind },
    Token { index: u32 },
    End,
}

impl FlatSyntaxTree {
    pub fn traverse_root<'a>(&'a self, input: &'a str) -> FlatNodeView<'a> {
        let root_kind = match self.tree[0] {
            SyntaxNode::Start { kind } => kind,
            _ => panic!(),
        };

        FlatNodeView {
            tree: self,
            pos: 0,
            kind: root_kind,
            input,
        }
    }
}

impl Display for FlatSyntaxTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            DebugTree {
                tree: self,
                verify: true,
                display_end: false,
            }
        )
    }
}

pub(super) struct DebugTree<'a> {
    pub tree: &'a FlatSyntaxTree,
    pub verify: bool,
    pub display_end: bool,
}

impl<'a> Display for DebugTree<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut indent: i64 = 0;
        for syntax in &self.tree.tree {
            match syntax {
                SyntaxNode::StartPlaceholder => panic!(),
                SyntaxNode::Start { kind } => {
                    for i in 0..indent {
                        write!(f, "    ")?;
                    }
                    writeln!(f, "{kind:?}");
                    indent += 1;
                }
                SyntaxNode::Token { index } => {
                    for i in 0..indent {
                        write!(f, "    ")?;
                    }
                    let kind = self.tree.lex.tokens[*index as usize];
                    writeln!(f, "{kind:?}")?;
                }
                SyntaxNode::End => {
                    indent -= 1;
                    if self.display_end {
                        for i in 0..indent {
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
