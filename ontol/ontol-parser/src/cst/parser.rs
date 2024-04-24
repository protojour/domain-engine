use std::ops::Range;

use chumsky::error::Simple;
use logos::Logos;

use crate::lexer::{kind::Kind, LexedSource};

use super::tree::{FlatSyntaxTree, SyntaxNode};

struct Cursor(usize);

#[derive(Clone, Copy, Debug)]
pub struct SyntaxCursor(usize);

impl SyntaxCursor {
    fn advance(&mut self) {
        self.0 += 1;
    }
}

#[derive(Clone, Copy, Debug)]
struct TokenCursor(usize);

impl TokenCursor {
    fn advance(&mut self) {
        self.0 += 1;
    }
}

pub struct StartNode {
    cursor: SyntaxCursor,
    kind: Kind,
}

impl StartNode {
    pub fn set_kind(&mut self, kind: Kind) {
        self.kind = kind;
    }
}

/// A parser model that produces a SyntaxNode tree.
///
/// The syntax tree model resembles a DOM-like view of the source code, the logical
/// tree is composed based on interpreting the tokens found in the source file.
pub struct CstParser<'a> {
    /// Cursor pointing to the current input token
    cursor: TokenCursor,

    /// Source of tokens
    lex: LexedSource,

    /// The tree that the parser will produce
    tree: Vec<SyntaxNode>,

    source: &'a str,
    errors: Vec<(TokenCursor, String)>,
}

impl<'a> CstParser<'a> {
    pub fn from_lexed_source(source: &'a str, lex: LexedSource) -> Self {
        let cap_estimate = lex.tokens.len();
        Self {
            source,
            lex,
            cursor: TokenCursor(0),
            tree: Vec::with_capacity(cap_estimate),
            errors: vec![],
        }
    }

    pub fn finish(self) -> (FlatSyntaxTree, Vec<(Range<usize>, String)>) {
        let tree = FlatSyntaxTree {
            tree: self.tree,
            lex: self.lex,
        };
        let errors = self
            .errors
            .into_iter()
            .map(|(cursor, msg)| {
                let span = tree
                    .lex
                    .opt_span(cursor.0)
                    .unwrap_or_else(|| self.source.len()..self.source.len());
                (span, msg)
            })
            .collect();

        (tree, errors)
    }

    pub fn peek_tokens(&self) -> &[Kind] {
        if self.cursor.0 > self.lex.tokens.len() {
            &[]
        } else {
            &self.lex.tokens[self.cursor.0..]
        }
    }

    pub fn not_peekforward(&self, f: impl Fn(Kind) -> bool) -> bool {
        for token in self.peek_tokens() {
            match token {
                token if f(*token) => {
                    return false;
                }
                Kind::Whitespace | Kind::DocComment | Kind::Comment => {}
                _ => return true,
            }
        }

        false
    }

    pub fn at(&mut self) -> Kind {
        self.eat_trivias();
        self.lex
            .tokens
            .get(self.cursor.0)
            .copied()
            .unwrap_or(Kind::Eof)
    }

    pub fn eat(&mut self, kind: Kind) -> Option<Kind> {
        let next = self.at();
        let result = if next == kind {
            self.append_token(self.cursor);
            Some(next)
        } else {
            self.eat_error("invalid token");
            None
        };
        self.cursor.advance();
        result
    }

    pub fn eat_sym_value(&mut self, sym: &str) -> bool {
        let next = self.at();
        self.append_token(self.cursor);

        let result = if next == Kind::Sym && self.current_text() == Some(sym) {
            true
        } else {
            self.report_error(format!("expected symbol `{sym}`"));
            false
        };

        self.cursor.advance();
        result
    }

    pub fn eat_text_literal(&mut self) -> bool {
        let next = self.at();
        self.append_token(self.cursor);

        let result = if matches!(next, Kind::SingleQuoteText | Kind::DoubleQuoteText) {
            true
        } else {
            self.report_error(format!("expected text literal"));
            false
        };

        self.cursor.advance();
        result
    }

    pub fn eat_trivias(&mut self) {
        self.eat_while(|kind| matches!(kind, Kind::Whitespace | Kind::Comment | Kind::DocComment));
    }

    pub fn eat_modifiers(&mut self) {
        while self.at() == Kind::Modifier {
            self.eat(Kind::Modifier);
        }
    }

    pub fn eat_ws(&mut self) {
        self.eat_while(|kind| matches!(kind, Kind::Whitespace));
    }

    pub fn eat_while(&mut self, mut f: impl FnMut(Kind) -> bool) {
        loop {
            let kind = self
                .lex
                .tokens
                .get(self.cursor.0)
                .copied()
                .unwrap_or(Kind::Eof);
            if f(kind) {
                self.append_token(self.cursor);
                self.cursor.advance();
                self.eat_trivias();
            } else {
                return;
            }
        }
    }

    pub fn eat_error(&mut self, msg: impl Into<String>) {
        let error = self.start(Kind::Error);

        self.report_error(msg);
        self.append_token(self.cursor);
        self.cursor.advance();

        self.end(error);
    }

    fn report_error(&mut self, msg: impl Into<String>) {
        self.errors.push((self.cursor, msg.into()));
    }

    pub fn syntax_cursor(&self) -> SyntaxCursor {
        SyntaxCursor(self.tree.len())
    }

    pub fn start(&mut self, kind: Kind) -> StartNode {
        let index = self.tree.len();
        self.tree.push(SyntaxNode::StartPlaceholder);
        StartNode {
            kind,
            cursor: SyntaxCursor(index),
        }
    }

    pub fn end(&mut self, start_node: StartNode) {
        self.tree[start_node.cursor.0] = SyntaxNode::Start {
            kind: start_node.kind,
        };
        self.tree.push(SyntaxNode::End);
    }

    pub fn insert_node(&mut self, cursor: SyntaxCursor, kind: Kind) {
        self.tree.insert(cursor.0, SyntaxNode::Start { kind });
        self.tree.push(SyntaxNode::End);
    }

    fn append_token(&mut self, cursor: TokenCursor) {
        if cursor.0 < self.lex.tokens.len() {
            self.tree.push(SyntaxNode::Token {
                index: cursor.0 as u32,
            });
        }
    }

    fn current_text(&self) -> Option<&str> {
        let span = self.lex.opt_span(self.cursor.0)?;
        Some(&self.source[span])
    }
}
