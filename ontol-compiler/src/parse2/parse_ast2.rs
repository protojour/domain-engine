use chumsky::prelude::*;
use smartstring::alias::String;

use super::{
    ast2::{ChainedSubjectConnection, RelConnection, RelStmt, Stmt, Type, TypeStmt},
    lexer::Token,
    Span, Spanned,
};

#[allow(dead_code)]
pub fn stmt_seq() -> impl Parser<Token, Vec<Spanned<Stmt>>, Error = Simple<Token>> {
    stmt().repeated().then_ignore(end())
}

fn stmt() -> impl Parser<Token, Spanned<Stmt>, Error = Simple<Token>> {
    let type_stmt = spanned(type_stmt()).map(span_map(Stmt::Type));
    let rel_stmt = spanned(rel_stmt()).map(span_map(Stmt::Rel));

    type_stmt.or(rel_stmt)
}

fn type_stmt() -> impl Parser<Token, TypeStmt, Error = Simple<Token>> {
    keyword(Token::Type)
        .then(spanned(ident()))
        .then(spanned(
            rel_stmt()
                .repeated()
                .delimited_by(just(Token::Open('{')), just(Token::Close('}')))
                .or_not(),
        ))
        .map(|((kw, ident), rel_block)| TypeStmt {
            kw,
            ident,
            rel_block,
        })
}

fn rel_stmt() -> impl Parser<Token, RelStmt, Error = Simple<Token>> {
    keyword(Token::Rel)
        // subject
        .then(spanned(ty()).or_not())
        // connection
        .then(rel_connection())
        // chain
        .then(
            spanned(ty())
                .or_not()
                .then(rel_connection())
                .map(|(subject, connection)| ChainedSubjectConnection {
                    subject,
                    connection,
                })
                .repeated(),
        )
        // object
        .then(spanned(ty()).or_not())
        .map(|((((kw, subject), connection), chain), object)| RelStmt {
            kw,
            subject,
            connection,
            chain,
            object,
        })
}

fn rel_connection() -> impl Parser<Token, RelConnection, Error = Simple<Token>> {
    // type
    spanned(ty())
        // many
        .then(just(Token::Sigil('*')).or_not())
        // optional
        .then(just(Token::Sigil('?')).or_not())
        // object prop ident
        .then(spanned(just(Token::Sigil('|')).ignore_then(string_literal())).or_not())
        // rel params
        .then(spanned(just(Token::Sigil(':')).ignore_then(string_literal())).or_not())
        // within {}
        .delimited_by(just(Token::Open('{')), just(Token::Close('}')))
        .map(|((((ty, _), _), object_prop_ident), _)| RelConnection {
            ty,
            object_prop_ident,
            rel_params: None,
        })
}

fn ty() -> impl Parser<Token, Type, Error = Simple<Token>> {
    let unit = just(Token::Sigil('.')).map(|_| Type::Unit);
    let path = ident().map(Type::Path);
    let number_literal = number_literal().map(Type::NumberLiteral);
    let string_literal = string_literal().map(Type::StringLiteral);
    let regex = select! { Token::Regex(string) => string.clone() }.map(Type::Regex);

    unit.or(path)
        .or(number_literal)
        .or(string_literal)
        .or(regex)
}

fn keyword(token: Token) -> impl Parser<Token, Span, Error = Simple<Token>> {
    just(token).map_with_span(|_, span| span)
}

fn ident() -> impl Parser<Token, String, Error = Simple<Token>> {
    select! { Token::Sym(ident) => ident.clone() }
}

fn number_literal() -> impl Parser<Token, String, Error = Simple<Token>> {
    select! { Token::Number(string) => string.clone() }
}

fn string_literal() -> impl Parser<Token, String, Error = Simple<Token>> {
    select! { Token::StringLiteral(string) => string.clone() }
}

fn spanned<P, O>(parser: P) -> impl Parser<Token, Spanned<O>, Error = Simple<Token>>
where
    P: Parser<Token, O, Error = Simple<Token>>,
{
    parser.map_with_span(|t, span| (t, span))
}

fn span_map<T, U, F: Fn(T) -> U>(f: F) -> impl Fn(Spanned<T>) -> Spanned<U> {
    move |(data, span)| (f(data), span)
}

#[cfg(test)]
mod tests {
    use chumsky::Stream;

    use crate::parse2::lexer::lexer;

    use super::*;

    #[derive(Debug)]
    enum Error {
        Lex(Vec<Simple<char>>),
        Parse(Vec<Simple<Token>>),
    }

    fn p(input: &str) -> Result<Vec<Stmt>, Error> {
        let tokens = lexer().parse(input).map_err(Error::Lex)?;
        let len = input.len();
        let stmts = stmt_seq()
            .parse(Stream::from_iter(len..len + 1, tokens.into_iter()))
            .map_err(Error::Parse)?;
        Ok(stmts.into_iter().map(|(stmt, _)| stmt).collect())
    }

    #[test]
    fn parse_type() {
        let source = "
        type foo
        type bar {
            rel a { '' } b
            rel { lol } c
            rel { '' } { '' }
            rel { '' } b { '' }
        }
        ";

        assert_eq!(2, p(source).unwrap().len());
    }
}
