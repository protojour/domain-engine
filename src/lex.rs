use std::{convert::identity, ops::Range};

use chumsky::prelude::*;
use smartstring::{LazyCompact, SmartString};

type SS = SmartString<LazyCompact>;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

#[derive(Eq, PartialEq, Debug)]
pub enum Token {
    LParen,
    RParen,
    LBracket,
    RBracket,
    True,
    False,
    Ident(SS),
    Num(SS),
}

pub fn lexer() -> impl Parser<char, Vec<Spanned<Token>>, Error = Simple<char>> {
    let token = identity(just("(").map(|_| Token::LParen))
        .or(just(")").map(|_| Token::RParen))
        .or(just("[").map(|_| Token::LBracket))
        .or(just("]").map(|_| Token::RBracket))
        .or(just("true").map(|_| Token::True))
        .or(just("false").map(|_| Token::False))
        .or(num())
        .or(identifier());

    token
        .map_with_span(|tok, span| (tok, span))
        .padded()
        .repeated()
}

fn num() -> impl Parser<char, Token, Error = Simple<char>> {
    filter(|c: &char| c.is_digit(10) && c != &'0')
        .map(Some)
        .chain::<char, Vec<_>, _>(filter(|c: &char| c.is_digit(10)).repeated())
        .map(|vec| Token::Num(SS::from_iter(vec.into_iter())))
}

fn identifier() -> impl Parser<char, Token, Error = Simple<char>> {
    filter(|c: &char| !c.is_whitespace())
        .map(Some)
        .chain::<char, Vec<_>, _>(filter(|c: &char| !c.is_whitespace()).repeated())
        .map(|vec| Token::Ident(SS::from_iter(vec.into_iter())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_one(input: &str) -> Spanned<Token> {
        let tokens = lexer().parse(input).unwrap();
        tokens.into_iter().next().unwrap()
    }

    #[test]
    fn lex_ctrl() {
        assert_eq!((Token::LParen, 0..1), lex_one("("));
        assert_eq!((Token::LParen, 1..2), lex_one(" ("));
    }

    #[test]
    fn lex_ident() {
        assert_eq!((Token::Ident("abc".into()), 0..3), lex_one("abc"));
    }

    #[test]
    fn lex_ident_utf8() {
        assert_eq!((Token::Ident("bæ".into()), 0..2), lex_one("bæ"));
    }

    #[test]
    fn lex_int() {
        assert_eq!((Token::Num("123".into()), 0..3), lex_one("123"));
    }

    #[test]
    fn lex_many() {
        let tokens = lexer().parse(" a (1) ").unwrap();
        assert_eq!(4, tokens.len());
    }
}
