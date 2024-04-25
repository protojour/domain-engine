use std::{fs, path::PathBuf};

use assert_matches::assert_matches;
use chumsky::Stream;

use crate::{cst::grammar, lexer::lex, parser::statement_sequence};

use super::*;

#[rstest::rstest]
fn cst(#[files("test-cases/cst/*.test")] path: PathBuf) {
    let contents = fs::read_to_string(&path).unwrap();

    let mut ontol_src = String::new();
    let mut cst_expect = String::new();

    let mut lines = contents.lines();

    let mut parse_fn: Option<fn(&mut CstParser)> = None;

    while let Some(mut line) = lines.next() {
        if line.starts_with("//@") {
            line = line.strip_prefix("//@ grammar=").unwrap();
            match line {
                "ontol" => parse_fn = Some(grammar::ontol),
                "pattern" => parse_fn = Some(grammar::pattern_with_expr),
                "expr" => parse_fn = Some(grammar::expr_pattern::entry),
                other => {
                    panic!("unrecognized test function `{other}`");
                }
            }
            break;
        } else {
            ontol_src.push_str(line);
            ontol_src.push('\n');
        }
    }

    for line in lines {
        cst_expect.push_str(line);
        cst_expect.push('\n');
    }

    let parse_fn = parse_fn.expect("no test function");

    let (lexed, _errors) = LexedSource::lex(&ontol_src);
    let mut parser = CstParser::from_lexed_source(&ontol_src, lexed);
    parse_fn(&mut parser);

    let (tree, _errors) = parser.finish();

    pretty_assertions::assert_eq!(cst_expect, format!("{}", tree.debug_tree(&ontol_src)));
}

#[derive(Debug, PartialEq)]
enum Error {
    Lex(Vec<Simple<char>>),
    Parse(Vec<Simple<Token>>),
}

fn parse(input: &str) -> Result<Vec<Statement>, Error> {
    let (tokens, errors) = lex(input);
    if !errors.is_empty() {
        return Err(Error::Lex(errors));
    }
    let len = input.len();
    let stmts = statement_sequence()
        .parse(Stream::from_iter(len..len + 1, tokens.into_iter()))
        .map_err(Error::Parse)?;
    Ok(stmts.into_iter().map(|(stmt, _)| stmt).collect())
}

#[test]
fn parse_def() {
    let source = "
    /// doc comment
    def foo()
    /// doc comment
    def bar(
        rel a '': b
        rel .lol: c
        fmt a => . => .
    )
    ";

    let stmts = parse(source).unwrap();
    assert_matches!(stmts.as_slice(), [Statement::Def(_), Statement::Def(_)]);

    assert_eq!(1, stmts[0].docs().unwrap().lines().count());
}

#[test]
fn doc_comment_unindent() {
    let source = "
    /// line 1
    ///
    /// line 3
    ///   line 4
    def foo()
    ";

    let stmts = parse(source).unwrap();
    let Statement::Def(def) = &stmts[0] else {
        panic!();
    };

    assert_eq!(def.docs.as_ref().unwrap(), "line 1\n\nline 3\n  line 4");
}

#[test]
fn parse_fmt() {
    let source = "fmt '' => '' => ''";

    let stmts = parse(source).unwrap();
    assert_matches!(stmts.as_slice(), [Statement::Fmt(_)]);
}

#[test]
fn parse_map() {
    let source = "
    map(
        foo(x),
        bar(
            'foo': x
        )
    )

    // comment
    map(
        foo(x + 1),
        bar(
            // FIXME: Want to allow this without parentheses:
            'foo': ((x / 3) + 4),
        ),
    )
    ";

    let stmts = parse(source).unwrap();
    assert_matches!(stmts.as_slice(), [Statement::Map(_), Statement::Map(_)]);
}

#[test]
fn parse_map2() {
    let source = "map(x(), x('x': ((x / 3) + 4)))";

    let stmts = parse(source).unwrap();
    assert_matches!(stmts.as_slice(), [Statement::Map(_)]);
}

#[test]
fn parse_map_struct_attr_expr_without_parentheses() {
    let source = "map(x(), x('x': (x / 3) + 4))";

    let error = parse(source).unwrap_err();
    assert_eq!(
        error,
        Error::Parse(vec![Simple::expected_input_found(
            24..25,
            // expected:
            vec![Some(Token::Close(')')), Some(Token::Sigil(','))],
            // found:
            Some(Token::Sigil('+'))
        )
        .with_label("struct pattern")])
    );
}

#[test]
fn parse_regex_in_map() {
    let source = r"
    map(
        foo(x),
        bar(
            'foo': /Hello (?<name>\w+)!/
        )
    )
    ";

    let stmts = parse(source).unwrap();
    assert_matches!(stmts.as_slice(), [Statement::Map(_)]);
}

#[test]
fn parse_spread_in_map() {
    let source = r"
    map(
        foo(x),
        bar('a': b, ..rest)
    )
    ";

    let stmts = parse(source).unwrap();
    assert_matches!(stmts.as_slice(), [Statement::Map(_)]);
}
