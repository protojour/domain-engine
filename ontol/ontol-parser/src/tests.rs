use assert_matches::assert_matches;
use chumsky::Stream;

use crate::{lexer::lex, parser::statement_sequence};

use super::*;

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

    assert_eq!(1, stmts[0].docs().len());
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
