use std::ops::Range;

use logos::Logos;

use crate::lexer::kind::Kind;

use super::tree::{FlatSyntaxTree, SyntaxNode};

struct Cursor(usize);

#[derive(Clone, Copy, Debug)]
pub struct SyntaxCursor(usize);

impl SyntaxCursor {
    fn advance(&mut self) {
        self.0 += 1;
    }
}

/// A parser model that produces a SyntaxNode tree.
///
/// The syntax tree model resembles a DOM-like view of the source code, the logical
/// tree is composed based on interpreting the tokens found in the source file.
pub struct CstParser {
    /// Input tokens
    tokens: Vec<Kind>,
    /// Input spans
    spans: Vec<Range<usize>>,

    /// Cursor pointing to the current input token
    cursor: usize,

    /// The tree that the parser will produce
    tree: Vec<SyntaxNode>,
}

impl CstParser {
    pub fn from_input(input: &str) -> Self {
        let mut tokens = vec![];
        let mut spans = vec![];

        let mut lexer = Kind::lexer(input);

        while let Some(result) = lexer.next() {
            match result {
                Ok(kind) => {
                    tokens.push(result.unwrap());
                    spans.push(lexer.span());
                }
                Err(_) => todo!(),
            }
        }

        Self {
            tokens,
            spans,
            cursor: 0,
            tree: vec![],
        }
    }

    pub fn finish(self) -> FlatSyntaxTree {
        FlatSyntaxTree {
            tree: self.tree,
            tokens: self.tokens,
            spans: self.spans,
        }
    }

    pub fn at(&mut self) -> Kind {
        self.eat_trivia();
        self.tokens.get(self.cursor).copied().unwrap_or(Kind::Eof)
    }

    pub fn eat(&mut self, kind: Kind) -> Option<Kind> {
        let next = self.at();
        self.tree.push(SyntaxNode::Token {
            index: self.cursor as u32,
        });
        let result = if next == kind { Some(next) } else { None };
        self.cursor += 1;
        result
    }

    pub fn eat_trivia(&mut self) {
        loop {
            let kind = self.tokens.get(self.cursor).copied().unwrap_or(Kind::Eof);
            if matches!(kind, Kind::Whitespace | Kind::Comment | Kind::DocComment) {
                self.tree.push(SyntaxNode::Token {
                    index: self.cursor as u32,
                });
                self.cursor += 1;
            } else {
                return;
            }
        }
    }

    pub fn error(&mut self) {}

    pub fn syntax_cursor(&self) -> SyntaxCursor {
        SyntaxCursor(self.tree.len())
    }

    pub fn start_node(&mut self) -> SyntaxCursor {
        let index = self.tree.len();
        self.tree.push(SyntaxNode::StartPlaceholder);
        SyntaxCursor(index)
    }

    pub fn commit_node(&mut self, cursor: SyntaxCursor, kind: Kind) {
        self.tree[cursor.0] = SyntaxNode::Start { kind };
        self.tree.push(SyntaxNode::End);
    }

    pub fn insert_node(&mut self, cursor: SyntaxCursor, kind: Kind) {
        self.tree.insert(cursor.0, SyntaxNode::Start { kind });
        self.tree.push(SyntaxNode::End);
    }
}
