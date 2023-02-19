use chumsky::prelude::*;
use smartstring::alias::String;

use super::{
    ast2::{RelConnection, RelStmt, Stmt, Type, TypeStmt},
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
    let rel_block = rel_stmt()
        .repeated()
        .delimited_by(just(Token::Bracket('{')), just(Token::Bracket('}')));

    keyword(Token::Type)
        .then(spanned(ident()))
        .then(spanned(rel_block.or_not()))
        .map(|((kw, ident), rel_block)| TypeStmt {
            kw,
            ident,
            rel_block,
        })
}

fn rel_stmt() -> impl Parser<Token, RelStmt, Error = Simple<Token>> {
    let connection = just(Token::Bracket('{'))
        .ignore_then(spanned(ty()))
        .then_ignore(just(Token::Bracket('}')))
        .map(|ty| RelConnection {
            ty,
            rel_params: None,
            object_prop_ident: None,
        });

    keyword(Token::Rel)
        .then(spanned(ty()).or_not())
        .then(connection)
        .then(spanned(ty()).or_not())
        .map(|(((kw, subject), connection), object)| RelStmt {
            kw,
            subject,
            connection,
            object,
        })
}

fn ty() -> impl Parser<Token, Type, Error = Simple<Token>> {
    let sym = ident().map(Type::Sym);
    let string_literal =
        select! { Token::StringLiteral(string) => string.clone() }.map(Type::StringLiteral);

    sym.or(string_literal)
}

fn keyword(token: Token) -> impl Parser<Token, Span, Error = Simple<Token>> {
    just(token).map_with_span(|_, span| span)
}

fn ident() -> impl Parser<Token, String, Error = Simple<Token>> {
    select! { Token::Sym(ident) => ident.clone() }
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
    fn parse_something() {
        let source = "
        type foo {
            rel a { '' } b
            rel b { lol } c
        }
        ";

        p(source).unwrap();
    }
}
