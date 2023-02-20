use chumsky::prelude::*;
use smartstring::alias::String;

use super::{
    ast2::{
        ChainedSubjectConnection, EqAttribute, EqAttributeRel, EqStmt, EqType, Expr, RelConnection,
        RelStmt, Stmt, Type, TypeStmt,
    },
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
    let eq_stmt = spanned(eq_stmt()).map(span_map(Stmt::Eq));

    type_stmt.or(rel_stmt).or(eq_stmt)
}

fn type_stmt() -> impl Parser<Token, TypeStmt, Error = Simple<Token>> + Clone {
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

fn rel_stmt() -> impl Parser<Token, RelStmt, Error = Simple<Token>> + Clone {
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

fn rel_connection() -> impl Parser<Token, RelConnection, Error = Simple<Token>> + Clone {
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

fn eq_stmt() -> impl Parser<Token, EqStmt, Error = Simple<Token>> + Clone {
    keyword(Token::Eq)
        .then(
            spanned(variable())
                .repeated()
                .delimited_by(just(Token::Open('(')), just(Token::Close(')'))),
        )
        .then(
            spanned(eq_type())
                .then(spanned(eq_type()))
                .delimited_by(just(Token::Open('{')), just(Token::Close('}'))),
        )
        .map(|((kw, variables), (first, second))| EqStmt {
            kw,
            variables,
            first,
            second,
        })
}

fn eq_type() -> impl Parser<Token, EqType, Error = Simple<Token>> + Clone {
    spanned(path())
        .then(
            eq_attribute()
                .repeated()
                .delimited_by(just(Token::Open('{')), just(Token::Close('}'))),
        )
        .map(|(path, attributes)| EqType { path, attributes })
}

fn eq_attribute() -> impl Parser<Token, EqAttribute, Error = Simple<Token>> + Clone {
    let variable = expr().map(EqAttribute::Expr);
    let rel = keyword(Token::Rel)
        .then(spanned(expr()).or_not())
        .then(spanned(ty()).delimited_by(just(Token::Open('{')), just(Token::Close('}'))))
        .then(spanned(expr()).or_not())
        .map(|(((kw, subject), connection), object)| {
            EqAttribute::Rel(EqAttributeRel {
                kw,
                subject,
                connection,
                object,
            })
        });

    variable.or(rel)
}

fn expr() -> impl Parser<Token, Expr, Error = Simple<Token>> + Clone {
    recursive(|expr| {
        let path = path().map(Expr::Path);
        let variable = variable().map(Expr::Variable);
        let number_literal = number_literal().map(Expr::NumberLiteral);
        let string_literal = string_literal().map(Expr::StringLiteral);
        let group = expr.delimited_by(just(Token::Open('(')), just(Token::Close(')')));

        let atom = path
            .or(variable)
            .or(number_literal)
            .or(string_literal)
            .or(group);

        // Consume operators by precedende
        // TODO: Migrate to Pratt parse when Chumsky supports it.
        // these are large types and thus pretty slow to compile :(

        // infix precedence for multiplication and division:
        let op = just(Token::Sigil('*')).or(just(Token::Sigil('/')));
        let product = atom
            .clone()
            .then(op.then(atom).repeated())
            .foldl(|a, (op, b)| Expr::Binary(Box::new(a), op, Box::new(b)));

        // infix precedence for addition and subtraction:
        let op = just(Token::Sigil('+')).or(just(Token::Sigil('-')));
        product
            .clone()
            .then(op.then(product).repeated())
            .foldl(|a, (op, b)| Expr::Binary(Box::new(a), op, Box::new(b)))
    })
}

fn ty() -> impl Parser<Token, Type, Error = Simple<Token>> + Clone {
    let unit = just(Token::Sigil('.')).map(|_| Type::Unit);
    let path = ident().map(Type::Path);
    let number_literal = number_literal().map(Type::NumberLiteral);
    let string_literal = string_literal().map(Type::StringLiteral);
    let regex = select! { Token::Regex(string) => string }.map(Type::Regex);

    unit.or(path)
        .or(number_literal)
        .or(string_literal)
        .or(regex)
}

fn keyword(token: Token) -> impl Parser<Token, Span, Error = Simple<Token>> + Clone {
    just(token).map_with_span(|_, span| span)
}

fn path() -> impl Parser<Token, String, Error = Simple<Token>> + Clone {
    select! { Token::Sym(ident) => ident }
}

fn ident() -> impl Parser<Token, String, Error = Simple<Token>> + Clone {
    select! { Token::Sym(ident) => ident }
}

fn variable() -> impl Parser<Token, String, Error = Simple<Token>> + Clone {
    just(Token::Sigil(':')).ignore_then(ident())
}

fn number_literal() -> impl Parser<Token, String, Error = Simple<Token>> + Clone {
    select! { Token::Number(string) => string }
}

fn string_literal() -> impl Parser<Token, String, Error = Simple<Token>> + Clone {
    select! { Token::StringLiteral(string) => string }
}

fn spanned<P, O>(parser: P) -> impl Parser<Token, Spanned<O>, Error = Simple<Token>> + Clone
where
    P: Parser<Token, O, Error = Simple<Token>> + Clone,
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

    fn parse(input: &str) -> Result<Vec<Stmt>, Error> {
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

        assert_eq!(2, parse(source).unwrap().len());
    }

    #[test]
    fn parse_eq() {
        let source = "
        eq (:x :y) {
            foo { :x }
            bar {
                rel { 'foo' } :x
            }
        }

        eq (:x :y) {
            foo { :x + 1 }
            bar {
                rel { 'foo' } (:x + 3) * 4
            }
        }
        ";

        assert_eq!(2, parse(source).unwrap().len());
    }
}
