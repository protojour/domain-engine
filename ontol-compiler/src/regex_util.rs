use std::str::Chars;

use ontol_parser::Span;
use ontol_runtime::smart_format;
use regex_syntax::{
    hir::{Class, ClassUnicode, ClassUnicodeRange, Hir, HirKind, Literal, Look, Repetition},
    Parser,
};
use smartstring::alias::String;

use crate::def::RegexAst;

pub fn uuid() -> Hir {
    let hex = Hir::class(Class::Unicode(ClassUnicode::new([
        ClassUnicodeRange::new('0', '9'),
        ClassUnicodeRange::new('a', 'f'),
        ClassUnicodeRange::new('A', 'F'),
    ])));
    let dash = Hir::literal("-".as_bytes());

    fn repeat_exact(hir: Hir, n: u32) -> Hir {
        Hir::repetition(Repetition {
            min: n,
            max: Some(n),
            greedy: true,
            sub: Box::new(hir),
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
    Hir::concat(vec![Hir::look(Look::Start), Hir::look(Look::End)])
}

pub fn set_of_all_strings() -> Hir {
    Parser::new().parse(r#".*"#).unwrap()
}

pub fn collect_hir_constant_parts(hir: &Hir, parts: &mut String) {
    match hir.kind() {
        HirKind::Literal(Literal(bytes)) => parts.push_str(std::str::from_utf8(bytes).unwrap()),
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
            HirKind::Look(Look::Start | Look::StartLF | Look::StartCRLF) => {}
            _ => return None,
        }

        let mut prefix = String::new();

        for next in iterator {
            match next.kind() {
                HirKind::Literal(Literal(bytes)) => {
                    prefix.push_str(std::str::from_utf8(bytes).unwrap());
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

pub fn parse_literal_regex(pattern: &str, pattern_span: &Span) -> Result<RegexAst, (String, Span)> {
    let mut ast_parser = regex_syntax::ast::parse::Parser::new();
    let ast = match ast_parser.parse(pattern) {
        Ok(ast) => ast,
        Err(err) => {
            return Err((
                smart_format!("{}", err.kind()),
                project_regex_span(pattern, pattern_span, err.span()),
            ))
        }
    };

    let mut translator = regex_syntax::hir::translate::Translator::new();
    let hir = match translator.translate(pattern, &ast) {
        Ok(hir) => hir,
        Err(err) => {
            return Err((
                smart_format!("{}", err.kind()),
                project_regex_span(pattern, pattern_span, err.span()),
            ))
        }
    };

    Ok(RegexAst { ast, hir })
}

pub fn project_regex_span(
    pattern: &str,
    pattern_span: &Span,
    ast_span: &regex_syntax::ast::Span,
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
        source_cursor: pattern_span.start + 1,
        regex_cursor: 0,
    };

    let mut chars = pattern.chars();
    let start = scanner.advance_to_regex_pos(&mut chars, ast_span.start.offset);
    let end = scanner.advance_to_regex_pos(&mut chars, ast_span.end.offset);

    Span { start, end }
}
