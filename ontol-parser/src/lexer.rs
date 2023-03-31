use std::fmt::Display;

use chumsky::prelude::*;
use smartstring::alias::String;

use super::Spanned;

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Token {
    Open(char),
    Close(char),
    Sigil(char),
    Use,
    Type,
    Rel,
    Fmt,
    Map,
    Pub,
    FatArrow,
    Number(String),
    StringLiteral(String),
    Regex(String),
    Sym(String),
    DocComment(String),
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(c) | Self::Close(c) | Self::Sigil(c) => write!(f, "`{c}`"),
            Self::Use => write!(f, "`use`"),
            Self::Type => write!(f, "`type`"),
            Self::Rel => write!(f, "`rel`"),
            Self::Fmt => write!(f, "`fmt`"),
            Self::Map => write!(f, "`map`"),
            Self::Pub => write!(f, "`pub`"),
            Self::FatArrow => write!(f, "`=>`"),
            Self::Number(_) => write!(f, "`number`"),
            Self::StringLiteral(_) => write!(f, "`string`"),
            Self::Regex(_) => write!(f, "`regex`"),
            Self::Sym(sym) => write!(f, "`{sym}`"),
            Self::DocComment(_) => write!(f, "`doc_comment`"),
        }
    }
}

#[allow(dead_code)]
pub fn lexer() -> impl Parser<char, Vec<Spanned<Token>>, Error = Simple<char>> {
    let ident = ident().map(|ident| match ident.as_str() {
        "use" => Token::Use,
        "type" => Token::Type,
        "rel" => Token::Rel,
        "fmt" => Token::Fmt,
        "map" => Token::Map,
        "pub" => Token::Pub,
        _ => Token::Sym(ident),
    });

    let comment = just("//")
        .ignore_then(filter(|c: &char| *c != '/'))
        .ignore_then(take_until(just('\n').to(()).or(end())))
        .padded();

    doc_comment()
        .or(just("=>").map(|_| Token::FatArrow))
        .or(one_of(".,:?_+-*|=<>").map(Token::Sigil))
        .or(one_of("({[").map(Token::Open))
        .or(one_of(")}]").map(Token::Close))
        .or(num().map(Token::Number))
        .or(double_quote_string_literal().map(Token::StringLiteral))
        .or(single_quote_string_literal().map(Token::StringLiteral))
        .or(regex().map(Token::Regex))
        .or(just('/').then_ignore(none_of("/")).map(Token::Sigil))
        .or(ident)
        .map_with_span(|token, span| (token, span))
        .padded_by(comment.repeated())
        .recover_with(skip_then_retry_until([]))
        .padded()
        .repeated()
        .padded()
        .then_ignore(end())
}

fn num() -> impl Parser<char, String, Error = Simple<char>> {
    filter(|c: &char| c.is_ascii_digit() && !special_char(*c))
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| c.is_ascii_digit() && !special_char(*c)).repeated(),
        )
        .map(|vec| String::from_iter(vec.into_iter()))
}

fn ident() -> impl Parser<char, String, Error = Simple<char>> {
    filter(|c: &char| !c.is_whitespace() && !special_char(*c) && !c.is_ascii_digit() && *c != '_')
        .map(Some)
        .chain::<char, Vec<_>, _>(
            filter(|c: &char| !c.is_whitespace() && !special_char(*c)).repeated(),
        )
        .map(|vec| String::from_iter(vec.into_iter()))
}

fn double_quote_string_literal() -> impl Parser<char, String, Error = Simple<char>> {
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

fn single_quote_string_literal() -> impl Parser<char, String, Error = Simple<char>> {
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

fn regex() -> impl Parser<char, String, Error = Simple<char>> {
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

fn doc_comment() -> impl Parser<char, Token, Error = Simple<char>> {
    just("///")
        .ignore_then(take_until(just('\n')))
        .map(|(vec, _)| Token::DocComment(String::from_iter(vec.into_iter())))
}

fn special_char(c: char) -> bool {
    matches!(
        c,
        '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>' | '.' | ';' | ':' | '?' | '/' | ','
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Result<Vec<Spanned<Token>>, Vec<Simple<char>>> {
        let (tokens, errors) = lexer().parse_recovery(input);
        if !errors.is_empty() {
            Err(errors)
        } else if let Some(tokens) = tokens {
            Ok(tokens)
        } else {
            Err(vec![])
        }
    }

    #[test]
    fn comment_at_eof() {
        lex("foobar // comment ").unwrap();
    }
}
