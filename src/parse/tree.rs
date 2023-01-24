use chumsky::prelude::*;
use smartstring::alias::String;

use super::Spanned;

/// A simple syntax tree consisting of S-expressions.
#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum Tree {
    Paren(Vec<Spanned<Tree>>),
    Bracket(Vec<Spanned<Tree>>),
    Dot,
    Sym(String),
    Num(String),
    Comment(String),
}

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum Keyword {
    True,
    False,
    Struct,
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
            .or(sym())
            .or(comment());

        combined_tree
            .map_with_span(|tok, span| (tok, span))
            .padded()
    })
    .padded()
}

pub fn trees_parser() -> impl Parser<char, Vec<Spanned<Tree>>, Error = Simple<char>> {
    tree_parser().repeated()
}

fn num() -> impl Parser<char, Tree, Error = Simple<char>> {
    filter(|c: &char| c.is_digit(10) && c != &'0' && !special_char(*c))
        .map(Some)
        .chain::<char, Vec<_>, _>(filter(|c: &char| c.is_digit(10) && !special_char(*c)).repeated())
        .map(|vec| Tree::Num(String::from_iter(vec.into_iter())))
}

fn sym() -> impl Parser<char, Tree, Error = Simple<char>> {
    filter(|c: &char| !c.is_whitespace() && !special_char(*c))
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| !c.is_whitespace() && !special_char(*c)).repeated(),
        )
        .map(|vec| Tree::Sym(String::from_iter(vec.into_iter())))
}

fn comment() -> impl Parser<char, Tree, Error = Simple<char>> {
    filter(|c: &char| c == &';')
        .map(Some)
        .chain::<char, Vec<_>, _>(filter(|c: &char| c != &'\n').repeated())
        .map(|vec| Tree::Comment(String::from_iter(vec.into_iter())))
}

fn special_char(c: char) -> bool {
    match c {
        '(' | ')' | '[' | ']' | '{' | '}' | '.' | ';' => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    fn test_one(input: &str) -> Spanned<Tree> {
        let tokens = trees_parser().parse(input).unwrap();
        tokens.into_iter().next().unwrap()
    }

    #[test]
    fn paren1() {
        assert_eq!((Tree::Paren(vec![]), 0..2), test_one("()"));
    }

    #[test]
    fn paren2() {
        assert_eq!((Tree::Paren(vec![]), 1..4), test_one(" ( ) "));
    }

    #[test]
    fn paren3() {
        assert_eq!(
            (Tree::Paren(vec![(Tree::Sym("a".into()), 1..2)]), 0..3),
            test_one("(a)")
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
            test_one(" ( a b ) ")
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
            test_one(" [ a b ] ")
        );
    }

    #[test]
    fn ident() {
        assert_eq!((Tree::Sym("abc".into()), 0..3), test_one("abc"));
    }

    #[test]
    fn ident_utf8() {
        let (Tree::Sym(ident), span) = test_one("bæ") else {
            panic!();
        };
        assert!(ident.is_inline());
        assert_eq!(0..2, span);
        assert_eq!("bæ", ident);
    }

    #[test]
    fn int() {
        assert_eq!((Tree::Num("123".into()), 0..3), test_one("123"));
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
}
