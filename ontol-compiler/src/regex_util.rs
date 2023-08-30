use std::str::Chars;

use ontol_parser::Span;
use ontol_runtime::smart_format;
use regex_syntax::{
    hir::{
        Anchor, Class, ClassUnicode, ClassUnicodeRange, Hir, HirKind, Literal, Repetition,
        RepetitionKind, RepetitionRange,
    },
    Parser,
};
use smartstring::alias::String;

pub fn uuid() -> Hir {
    let hex = Hir::class(Class::Unicode(ClassUnicode::new([
        ClassUnicodeRange::new('0', '9'),
        ClassUnicodeRange::new('a', 'f'),
        ClassUnicodeRange::new('A', 'F'),
    ])));
    let dash = Hir::literal(Literal::Unicode('-'));

    fn repeat_exact(hir: Hir, n: u32) -> Hir {
        Hir::repetition(Repetition {
            kind: RepetitionKind::Range(RepetitionRange::Exactly(n)),
            greedy: true,
            hir: Box::new(hir),
        })
    }

    Hir::alternation(vec![
        repeat_exact(hex.clone(), 32),
        Hir::concat(vec![
            repeat_exact(hex.clone(), 8),
            dash.clone(),
            repeat_exact(hex.clone(), 4),
            dash.clone(),
            repeat_exact(hex.clone(), 4),
            dash.clone(),
            repeat_exact(hex.clone(), 4),
            dash,
            repeat_exact(hex, 12),
        ]),
    ])
}

pub fn datetime_rfc3339() -> Hir {
    Parser::new()
        .parse(r"((?:([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?))(Z|[\+-][0-9]{2}:[0-9]{2})?)")
        .unwrap()
}

pub fn empty_string() -> Hir {
    Hir::concat(vec![
        Hir::anchor(Anchor::StartText),
        Hir::anchor(Anchor::EndText),
    ])
}

pub fn set_of_all_strings() -> Hir {
    Parser::new().parse(r#".*"#).unwrap()
}

pub fn collect_hir_constant_parts(hir: &Hir, parts: &mut String) {
    match hir.kind() {
        HirKind::Literal(Literal::Unicode(char)) => parts.push(*char),
        HirKind::Concat(hirs) => {
            for child in hirs {
                collect_hir_constant_parts(child, parts);
            }
        }
        _ => {}
    }
}

pub fn constant_prefix(hir: &Hir) -> Option<String> {
    if let HirKind::Concat(hirs) = hir.kind() {
        let mut iterator = hirs.iter();
        let first = iterator.next()?;

        match first.kind() {
            HirKind::Anchor(Anchor::StartLine | Anchor::StartText) => {}
            _ => return None,
        }

        let mut prefix = String::new();

        for next in iterator {
            match next.kind() {
                HirKind::Literal(Literal::Unicode(char)) => {
                    prefix.push(*char);
                }
                _ => {
                    break;
                }
            }
        }

        if prefix.is_empty() {
            None
        } else {
            Some(prefix)
        }
    } else {
        None
    }
}

pub fn parse_literal_regex_to_hir(expr: &str, expr_span: &Span) -> Result<Hir, (String, Span)> {
    let mut parser = Parser::new();

    parser.parse(expr).map_err(|err| match err {
        regex_syntax::Error::Parse(err) => (
            smart_format!("{}", err.kind()),
            project_regex_span(expr, expr_span, err.span()),
        ),
        regex_syntax::Error::Translate(err) => (
            smart_format!("{}", err.kind()),
            project_regex_span(expr, expr_span, err.span()),
        ),
        // note: regex_syntax Error is a non-exhaustive enum
        _ => panic!("BUG: unhandled regex error"),
    })
}

pub fn project_regex_span(
    expr: &str,
    expr_span: &Span,
    regex_span: &regex_syntax::ast::Span,
) -> Span {
    // literal regexes start with '/' so that's part of the ontol span,
    // but regex-syntax never sees that, so add 1.

    struct Scanner {
        source_cursor: usize,
        regex_cursor: usize,
    }

    impl Scanner {
        fn advance_to_regex_pos(&mut self, chars: &mut Chars, regex_pos: usize) -> usize {
            while self.regex_cursor < regex_pos {
                let char = chars.next().unwrap();
                match char {
                    // slash is the only escaped character in literal regexes
                    '/' => self.source_cursor += 2,
                    _ => self.source_cursor += 1,
                }
                self.regex_cursor += 1;
            }
            self.source_cursor
        }
    }

    let mut scanner = Scanner {
        source_cursor: expr_span.start + 1,
        regex_cursor: 0,
    };

    let mut chars = expr.chars();
    let start = scanner.advance_to_regex_pos(&mut chars, regex_span.start.offset);
    let end = scanner.advance_to_regex_pos(&mut chars, regex_span.end.offset);

    Span { start, end }
}
