use crate::parse::Span;
use ontol_runtime::smart_format;
use regex_syntax::{
    hir::{
        Class, ClassUnicode, ClassUnicodeRange, Hir, HirKind, Literal, Repetition, RepetitionKind,
        RepetitionRange,
    },
    Parser,
};
use smartstring::alias::String;
use tracing::debug;

pub fn uuid_regex() -> Hir {
    let hex = Hir::class(Class::Unicode(ClassUnicode::new([
        ClassUnicodeRange::new('0', '9'),
        ClassUnicodeRange::new('a', 'f'),
        ClassUnicodeRange::new('A', 'F'),
    ])));
    let opt_dash = Hir::repetition(Repetition {
        kind: RepetitionKind::ZeroOrOne,
        greedy: true,
        hir: Box::new(Hir::literal(Literal::Unicode('-'))),
    });

    fn repeat_exact(hir: Hir, n: u32) -> Hir {
        Hir::repetition(Repetition {
            kind: RepetitionKind::Range(RepetitionRange::Exactly(n)),
            greedy: true,
            hir: Box::new(hir),
        })
    }

    Hir::concat(vec![
        repeat_exact(hex.clone(), 8),
        opt_dash.clone(),
        repeat_exact(hex.clone(), 4),
        opt_dash.clone(),
        repeat_exact(hex.clone(), 4),
        opt_dash.clone(),
        repeat_exact(hex.clone(), 4),
        opt_dash,
        repeat_exact(hex, 12),
    ])
}

pub fn collect_hir_constant_parts(hir: &Hir, parts: &mut String) {
    match hir.kind() {
        HirKind::Literal(Literal::Unicode(char)) => parts.push(*char),
        HirKind::Concat(hirs) => {
            for child in hirs {
                collect_hir_constant_parts(child, parts);
            }
        }
        other => {
            debug!("constant parts from {other:?}");
        }
    }
}

pub fn parse_literal_regex_to_hir(re: &str, re_span: &Span) -> Result<Hir, (String, Span)> {
    let mut parser = Parser::new();

    fn project_span(ontol_span: &Span, regex_span: &regex_syntax::ast::Span) -> Span {
        // literal regexes start with '/' so that's part of the ontol span,
        // but regex-syntax never sees that, so add 1
        Span {
            start: ontol_span.start + 1 + regex_span.start.offset,
            end: ontol_span.start + 1 + regex_span.end.offset,
        }
    }

    parser.parse(re).map_err(|err| match err {
        regex_syntax::Error::Parse(err) => (
            smart_format!("{}", err.kind()),
            project_span(re_span, err.span()),
        ),
        regex_syntax::Error::Translate(err) => (
            smart_format!("{}", err.kind()),
            project_span(re_span, err.span()),
        ),
        // note: regex_syntax Error is a non-exhaustive enum
        _ => panic!("BUG: unhandled regex error"),
    })
}
