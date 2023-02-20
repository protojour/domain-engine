use std::fmt::Display;

use chumsky::prelude::*;
use smartstring::alias::String;

use super::Spanned;

/// A simple syntax tree consisting of S-expressions.
///
/// TODO: Wildcard (_)
#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum Tree {
    Paren(Vec<Spanned<Tree>>),
    Brace(Vec<Spanned<Tree>>),
    Bracket(Vec<Spanned<Tree>>),
    Dot,
    Colon,
    Questionmark,
    Underscore,
    /// Any unquoted string not starting with "_"
    Sym(String),
    Num(String),
    StringLiteral(String),
    Regex(String),
    Comment(String),
}

impl PartialOrd for Tree {
    fn partial_cmp(&self, _: &Self) -> Option<std::cmp::Ordering> {
        panic!("DELETE ME")
    }
}

impl Ord for Tree {
    fn cmp(&self, _: &Self) -> std::cmp::Ordering {
        panic!("DELETE ME")
    }
}

impl Display for Tree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paren(_) => write!(f, "parentheses"),
            Self::Brace(_) => write!(f, "curly braces"),
            Self::Bracket(_) => write!(f, "square brackets"),
            Self::Dot => write!(f, "`.`"),
            Self::Colon => write!(f, ":"),
            Self::Questionmark => write!(f, "?"),
            Self::Underscore => write!(f, "_"),
            Self::Sym(_) => write!(f, "symbol"),
            Self::Num(_) => write!(f, "number"),
            Self::StringLiteral(_) => write!(f, "string literal"),
            Self::Regex(_) => write!(f, "regex"),
            Self::Comment(_) => write!(f, "comment"),
        }
    }
}

pub fn tree_parser() -> impl Parser<char, Spanned<Tree>, Error = Simple<char>> {
    recursive(|tree| {
        let repetition = tree.repeated().padded();

        let paren = repetition
            .clone()
            .map(Tree::Paren)
            .delimited_by(just("("), just(")"));
        let brace = repetition
            .clone()
            .map(Tree::Brace)
            .delimited_by(just("{"), just("}"));
        let bracket = repetition
            .map(Tree::Bracket)
            .delimited_by(just("["), just("]"));

        let combined_tree = paren
            .or(brace)
            .or(bracket)
            .or(just(".").map(|_| Tree::Dot))
            .or(just(":").map(|_| Tree::Colon))
            .or(just("?").map(|_| Tree::Questionmark))
            .or(just("_").map(|_| Tree::Underscore))
            .or(num().map(Tree::Num))
            .or(double_quote_string_literal().map(Tree::StringLiteral))
            .or(single_quote_string_literal().map(Tree::StringLiteral))
            .or(regex().map(Tree::Regex))
            .or(just("/").map(|_| Tree::Sym("/".into())))
            .or(ident().map(Tree::Sym))
            .or(comment());

        combined_tree
            .map_with_span(|tree, span| (tree, span))
            .padded()
    })
    .padded()
}

/// Parse a sequence of trees.
///
/// Typically a source file.
pub fn trees_parser() -> impl Parser<char, Vec<Spanned<Tree>>, Error = Simple<char>> {
    tree_parser().repeated().then_ignore(end())
}

pub fn num() -> impl Parser<char, String, Error = Simple<char>> {
    filter(|c: &char| c.is_ascii_digit() && !special_char(*c))
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| c.is_ascii_digit() && !special_char(*c)).repeated(),
        )
        .map(|vec| String::from_iter(vec.into_iter()))
}

pub fn ident() -> impl Parser<char, String, Error = Simple<char>> {
    filter(|c: &char| !c.is_whitespace() && !special_char(*c) && !c.is_ascii_digit() && *c != '_')
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| !c.is_whitespace() && !special_char(*c)).repeated(),
        )
        .map(|vec| String::from_iter(vec.into_iter()))
}

pub fn double_quote_string_literal() -> impl Parser<char, String, Error = Simple<char>> {
    let escape = just('\\').ignore_then(choice((
        just('\\'),
        just('/'),
        just('"'),
        just('b').to('\x08'),
        just('f').to('\x0C'),
        just('n').to('\n'),
        just('r').to('\r'),
        just('t').to('\t'),
    )));

    just('"')
        .ignore_then(
            filter(|c: &char| *c != '\\' && *c != '"')
                .or(escape)
                .repeated(),
        )
        .then_ignore(just('"'))
        .map(|vec| String::from_iter(vec.into_iter()))
}

pub fn single_quote_string_literal() -> impl Parser<char, String, Error = Simple<char>> {
    let escape = just('\\').ignore_then(choice((
        just('\\'),
        just('/'),
        just('\''),
        just('b').to('\x08'),
        just('f').to('\x0C'),
        just('n').to('\n'),
        just('r').to('\r'),
        just('t').to('\t'),
    )));

    just('\'')
        .ignore_then(
            filter(|c: &char| *c != '\\' && *c != '\'')
                .or(escape)
                .repeated(),
        )
        .then_ignore(just('\''))
        .map(|vec| String::from_iter(vec.into_iter()))
}

pub fn regex() -> impl Parser<char, String, Error = Simple<char>> {
    let escape = just('\\').ignore_then(just('/'));

    just('/')
        .ignore_then(filter(|c: &char| *c != '/' && !c.is_whitespace()).or(escape))
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| *c != '\\' && *c != '/')
                .or(escape)
                .repeated(),
        )
        .then_ignore(just('/'))
        .map(|vec| String::from_iter(vec.into_iter()))
}

fn comment() -> impl Parser<char, Tree, Error = Simple<char>> {
    filter(|c: &char| c == &';')
        .map(Some)
        .chain::<char, Vec<_>, _>(filter(|c: &char| c != &'\n').repeated())
        .map(|vec| Tree::Comment(String::from_iter(vec.into_iter())))
}

pub fn special_char(c: char) -> bool {
    matches!(
        c,
        '(' | ')' | '[' | ']' | '{' | '}' | '.' | ';' | ':' | '?' | '/'
    )
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    fn expect_one(input: &str) -> Spanned<Tree> {
        trees_parser()
            .parse(input)
            .expect("Expected OK tree parse, got error")
            .into_iter()
            .next()
            .expect("Expected one tree, got none")
    }

    #[test]
    fn paren1() {
        assert_eq!((Tree::Paren(vec![]), 0..2), expect_one("()"));
    }

    #[test]
    fn paren2() {
        assert_eq!((Tree::Paren(vec![]), 1..4), expect_one(" ( ) "));
    }

    #[test]
    fn paren3() {
        assert_eq!(
            (Tree::Paren(vec![(Tree::Sym("a".into()), 1..2)]), 0..3),
            expect_one("(a)")
        );
    }

    #[test]
    fn paren4() {
        assert_eq!(
            (
                Tree::Paren(vec![
                    (Tree::Sym("a".into()), 3..4),
                    (Tree::Sym("b".into()), 5..6)
                ]),
                1..8
            ),
            expect_one(" ( a b ) ")
        );
    }

    #[test]
    fn bracket() {
        assert_eq!(
            (
                Tree::Bracket(vec![
                    (Tree::Sym("a".into()), 3..4),
                    (Tree::Sym("b".into()), 5..6)
                ]),
                1..8
            ),
            expect_one(" [ a b ] ")
        );
    }

    #[test]
    fn ident() {
        assert_eq!((Tree::Sym("abc".into()), 0..3), expect_one("abc"));
    }

    #[test]
    fn ident_utf8() {
        let (Tree::Sym(ident), span) = expect_one("bæ") else {
            panic!();
        };
        assert!(ident.is_inline());
        assert_eq!(0..2, span);
        assert_eq!("bæ", ident);
    }

    #[test]
    fn int() {
        assert_eq!((Tree::Num("123".into()), 0..3), expect_one("123"));
    }

    #[test]
    fn regex() {
        assert_eq!((Tree::Regex("abc/".into()), 0..7), expect_one(r#"/abc\//"#));
    }

    #[test]
    fn many() {
        let trees = trees_parser().parse(" a (1) ").unwrap();
        assert_eq!(2, trees.len());
    }

    #[test]
    fn comments() {
        let trees = trees_parser()
            .parse(
                "
                1 ; hallo
                2 ; foo bar
                ",
            )
            .unwrap();
        assert_matches!(
            trees.as_slice(),
            &[
                (Tree::Num(_), _),
                (Tree::Comment(_), _),
                (Tree::Num(_), _),
                (Tree::Comment(_), _)
            ]
        );
    }

    #[test]
    fn comments2() {
        let trees = trees_parser()
            .parse(
                "
                (yo
                    number ;; comment
                )
                ",
            )
            .unwrap();
        assert_matches!(trees.as_slice(), &[(Tree::Paren(ref elems), _),] if elems.len() == 3);
    }

    #[test]
    fn string_literal() {
        let trees = trees_parser()
            .parse(
                r#"
                "literal"
                "#,
            )
            .unwrap();
        assert_matches!(trees.as_slice(), &[(Tree::StringLiteral(ref lit), _),] if lit == "literal");
    }

    #[test]
    fn double_quote_string_literal_escapes() {
        assert_eq!(
            (Tree::StringLiteral("'a'\"b\"c\n".into()), 1..14),
            expect_one(r#" "'a'\"b\"c\n" "#)
        );
    }

    #[test]
    fn single_quote_string_literal_escapes() {
        assert_eq!(
            (Tree::StringLiteral("'a'\"b\"c\n".into()), 1..14),
            expect_one(r#" '\'a\'"b"c\n' "#)
        );
    }

    #[test]
    fn parse_incomplete_should_error() {
        let _ = trees_parser().parse("(").expect_err("");
    }
}
