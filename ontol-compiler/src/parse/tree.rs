use std::fmt::Display;

use chumsky::prelude::*;
use smartstring::alias::String;

use super::Spanned;

/// A simple syntax tree consisting of S-expressions.
#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum Tree {
    Paren(Vec<Spanned<Tree>>),
    Bracket(Vec<Spanned<Tree>>),
    Dot,
    /// Any unquoted string
    Sym(String),
    /// String starting with ":"
    Variable(String),
    Num(String),
    StringLiteral(String),
    Comment(String),
}

impl Display for Tree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paren(_) => write!(f, "parentheses"),
            Self::Bracket(_) => write!(f, "square brackets"),
            Self::Dot => write!(f, "`.`"),
            Self::Sym(_) => write!(f, "symbol"),
            Self::Variable(_) => write!(f, "variable"),
            Self::Num(_) => write!(f, "number"),
            Self::StringLiteral(_) => write!(f, "string literal"),
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
        let bracket = repetition
            .map(Tree::Bracket)
            .delimited_by(just("["), just("]"));

        let combined_tree = paren
            .or(bracket)
            .or(just(".").map(|_| Tree::Dot))
            .or(num())
            .or(just(":").ignore_then(ident().map(Tree::Variable)))
            .or(string_literal().map(Tree::StringLiteral))
            .or(ident().map(Tree::Sym))
            .or(comment());

        combined_tree
            .map_with_span(|tree, span| (tree, span))
            .padded()
    })
    .padded()
}

pub fn trees_parser() -> impl Parser<char, Vec<Spanned<Tree>>, Error = Simple<char>> {
    tree_parser().repeated()
}

fn num() -> impl Parser<char, Tree, Error = Simple<char>> {
    filter(|c: &char| c.is_ascii_digit() && *c != '0' && !special_char(*c))
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| c.is_ascii_digit() && !special_char(*c)).repeated(),
        )
        .map(|vec| Tree::Num(String::from_iter(vec.into_iter())))
}

fn ident() -> impl Parser<char, String, Error = Simple<char>> {
    filter(|c: &char| !c.is_whitespace() && !special_char(*c))
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| !c.is_whitespace() && !special_char(*c)).repeated(),
        )
        .map(|vec| String::from_iter(vec.into_iter()))
}

fn string_literal() -> impl Parser<char, String, Error = Simple<char>> {
    // FIXME: other escape codes?
    // see https://github.com/zesterer/chumsky/blob/940d531a7b4bc63062284bec6888fc5dae10a3d2/benches/json.rs
    just('"')
        .ignore_then(filter(|c: &char| *c != '\\' && *c != '"').repeated())
        .then_ignore(just('"'))
        .map(|vec| String::from_iter(vec.into_iter()))
}

fn comment() -> impl Parser<char, Tree, Error = Simple<char>> {
    filter(|c: &char| c == &';')
        .map(Some)
        .chain::<char, Vec<_>, _>(filter(|c: &char| c != &'\n').repeated())
        .map(|vec| Tree::Comment(String::from_iter(vec.into_iter())))
}

fn special_char(c: char) -> bool {
    matches!(c, '(' | ')' | '[' | ']' | '{' | '}' | '.' | ';' | ':')
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
    #[ignore = "bug in chumsky::repeated. It believes that the error is a recovery."]
    fn chumsky_repeated_bug() {
        let _ = tree_parser()
            .parse("(")
            .expect("Expected OK tree parse, got error");
    }

    #[test]
    #[ignore = "this bug is reported here: https://github.com/zesterer/chumsky/issues/268"]
    fn bug_test() {
        pub fn simplistic_parser<'s>() -> impl Parser<char, &'s str, Error = Simple<char>> {
            just("foobar")
        }

        simplistic_parser()
            .repeated()
            .parse("something invalid")
            .expect_err("should fail");
    }
}
