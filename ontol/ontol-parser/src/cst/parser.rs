use std::ops::Range;

use crate::{
    lexer::{kind::Kind, Lex},
    ToUsizeRange, U32Span,
};

use super::tree::{FlatSyntaxTree, SyntaxMarker};

#[derive(Clone, Copy, Debug)]
pub struct SyntaxCursor(usize);

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
    lex: Lex,

    /// The tree that the parser will produce
    markers: Vec<SyntaxMarker>,

    source: &'a str,
    errors: Vec<(TokenCursor, String)>,
    error_state: bool,
}

impl<'a> CstParser<'a> {
    pub fn from_lexed_source(source: &'a str, lex: Lex) -> Self {
        let cap_estimate = lex.tokens().len();
        Self {
            source,
            lex,
            cursor: TokenCursor(0),
            markers: Vec::with_capacity(cap_estimate),
            errors: vec![],
            error_state: false,
        }
    }

    pub fn finish(self) -> (FlatSyntaxTree, Vec<(U32Span, String)>) {
        let tree = FlatSyntaxTree {
            markers: self.markers,
            lex: self.lex,
        };
        let errors = self
            .errors
            .into_iter()
            .map(|(cursor, msg)| {
                let span = tree
                    .lex
                    .opt_span(cursor.0)
                    .unwrap_or_else(|| (self.source.len()..self.source.len()).into());
                (span, msg)
            })
            .collect();

        (tree, errors)
    }

    pub fn peek_tokens(&self) -> &[Kind] {
        if self.cursor.0 > self.lex.tokens().len() {
            &[]
        } else {
            &self.lex.tokens()[self.cursor.0..]
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

    /// Peek at the next token that's not trivia (whitespace/comment)
    pub fn at(&self) -> Kind {
        for kind in self.peek_tokens() {
            match kind {
                Kind::Whitespace | Kind::DocComment | Kind::Comment => {}
                _ => return *kind,
            }
        }

        Kind::Eof
    }

    pub fn eat(&mut self, expected: Kind) -> Option<Kind> {
        self.eat_trivia();

        let kind = self.at_exact();
        let result = if kind == expected {
            self.push_current_token(kind);
            Some(kind)
        } else {
            self.eat_error(|kind| format!("expected {expected}, found {kind}"));
            None
        };
        self.cursor.advance();
        result
    }

    pub fn eat_symbol_literal(&mut self, symbol: &str) -> bool {
        self.eat_trivia();
        let kind = self.at_exact();
        self.push_current_token(kind);

        let result = if kind == Kind::Symbol && self.current_text() == Some(symbol) {
            true
        } else {
            self.report_error(format!("expected symbol `{symbol}`"));
            false
        };

        self.cursor.advance();
        result
    }

    pub fn eat_text_literal(&mut self) -> bool {
        self.eat_trivia();
        let kind = self.at_exact();
        self.push_current_token(kind);

        let result = if matches!(kind, Kind::SingleQuoteText | Kind::DoubleQuoteText) {
            true
        } else {
            self.report_error("expected text literal");
            false
        };

        self.cursor.advance();
        result
    }

    /// Eat "trivia", which means Whitespace, Comment and DocComment.
    pub fn eat_trivia(&mut self) {
        self.eat_while(|kind| matches!(kind, Kind::Whitespace | Kind::Comment | Kind::DocComment));
    }

    /// Eat "space", which in this context means Whitespace and Comment.
    /// It does not eat DocComment.
    pub fn eat_space(&mut self) {
        loop {
            let kind = self.at_exact();
            if matches!(kind, Kind::Whitespace | Kind::Comment) {
                self.push_current_token(kind);
                self.cursor.advance();
            } else {
                break;
            }
        }
    }

    pub fn eat_modifiers(&mut self) {
        while self.at() == Kind::Modifier {
            self.eat(Kind::Modifier);
        }
    }

    pub fn eat_while(&mut self, mut f: impl FnMut(Kind) -> bool) {
        loop {
            let kind = self.at_exact();
            if f(kind) {
                self.push_current_token(kind);
                self.cursor.advance();
                self.eat_trivia();
            } else {
                return;
            }
        }
    }

    pub fn eat_error(&mut self, f: impl Fn(Kind) -> String) {
        let kind = self.at();

        let error = self.start(Kind::Error);

        if kind != Kind::Error {
            // If the Kind is Error, it has already been reported at the lexing stage
            self.report_error(f(kind));
        }

        self.push_current_token(self.at_exact());
        self.cursor.advance();

        self.end(error);
    }

    pub fn has_error_state(&self) -> bool {
        self.error_state
    }

    pub fn clear_error_state(&mut self) {
        self.error_state = false;
    }

    fn report_error(&mut self, msg: impl Into<String>) {
        if !self.error_state {
            self.errors.push((self.cursor, msg.into()));
            self.error_state = true;
        }
    }

    pub fn syntax_cursor(&self) -> SyntaxCursor {
        SyntaxCursor(self.markers.len())
    }

    pub fn start(&mut self, kind: Kind) -> StartNode {
        self.eat_space();
        self.start_exact(kind)
    }

    pub fn start_exact(&mut self, kind: Kind) -> StartNode {
        let index = self.markers.len();
        self.markers.push(SyntaxMarker::Start { kind: Kind::Error });
        StartNode {
            kind,
            cursor: SyntaxCursor(index),
        }
    }

    pub fn end(&mut self, start_node: StartNode) {
        self.markers[start_node.cursor.0] = SyntaxMarker::Start {
            kind: start_node.kind,
        };
        self.markers.push(SyntaxMarker::End);
    }

    pub fn insert_node(&mut self, cursor: SyntaxCursor, kind: Kind) -> StartNode {
        self.markers.insert(cursor.0, SyntaxMarker::Start { kind });
        StartNode { cursor, kind }
    }

    pub fn insert_node_closed(&mut self, cursor: SyntaxCursor, kind: Kind) {
        self.markers.insert(cursor.0, SyntaxMarker::Start { kind });
        self.markers.push(SyntaxMarker::End);
    }

    fn push_current_token(&mut self, kind: Kind) {
        let cursor = self.cursor;
        if cursor.0 < self.lex.tokens().len() {
            let index = cursor.0 as u32;
            let marker = match kind {
                Kind::Whitespace | Kind::Comment => SyntaxMarker::Ignorable { index },
                _ => SyntaxMarker::Token { index },
            };
            self.markers.push(marker);
        }
    }

    fn at_exact(&self) -> Kind {
        self.lex
            .tokens()
            .get(self.cursor.0)
            .copied()
            .unwrap_or(Kind::Eof)
    }

    fn current_text(&self) -> Option<&str> {
        let span: Range<usize> = self.lex.opt_span(self.cursor.0)?.to_usize_range();
        Some(&self.source[span])
    }
}
