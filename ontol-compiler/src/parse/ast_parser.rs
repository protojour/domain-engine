use chumsky::prelude::*;
use smartstring::alias::String;

use super::{
    ast::{
        ChainedSubjectConnection, EqAttribute, EqAttributeRel, EqStmt, EqType, Expr, RelConnection,
        RelStmt, Stmt, Type, TypeStmt,
    },
    lexer::Token,
    Span, Spanned,
};

/// AstParser parses Tokens into AST nodes.
///
/// This is an extension trait that simplifies the use of chumsky's Parser trait in this module.
pub trait AstParser<O>: Parser<Token, O, Error = Simple<Token>> + Clone {}

impl<T, O> AstParser<O> for T where T: Parser<Token, O, Error = Simple<Token>> + Clone {}

/// A parser for a sequence of statements (i.e. a source file)
#[allow(dead_code)]
pub fn stmt_seq() -> impl AstParser<Vec<Spanned<Stmt>>> {
    stmt().repeated().then_ignore(end())
}

fn stmt() -> impl AstParser<Spanned<Stmt>> {
    let type_stmt = spanned(type_stmt()).map(span_map(Stmt::Type));
    let rel_stmt = spanned(rel_stmt()).map(span_map(Stmt::Rel));
    let eq_stmt = spanned(eq_stmt()).map(span_map(Stmt::Eq));

    type_stmt.or(rel_stmt).or(eq_stmt)
}

fn type_stmt() -> impl AstParser<TypeStmt> {
    doc_comments()
        .then(keyword(Token::Type))
        .then(spanned(ident()))
        .then(spanned(
            rel_stmt()
                .repeated()
                .delimited_by(just(Token::Open('{')), just(Token::Close('}')))
                .or_not(),
        ))
        .map(|(((docs, kw), ident), rel_block)| TypeStmt {
            docs,
            kw,
            ident,
            rel_block,
        })
}

fn rel_stmt() -> impl AstParser<RelStmt> {
    doc_comments()
        .then(keyword(Token::Rel))
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
        .map(
            |(((((docs, kw), subject), connection), chain), object)| RelStmt {
                docs,
                kw,
                subject,
                connection,
                chain,
                object,
            },
        )
}

fn rel_connection() -> impl AstParser<RelConnection> {
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

fn eq_stmt() -> impl AstParser<EqStmt> {
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

fn eq_type() -> impl AstParser<EqType> {
    spanned(path())
        .then(
            eq_attribute()
                .repeated()
                .delimited_by(just(Token::Open('{')), just(Token::Close('}'))),
        )
        .map(|(path, attributes)| EqType { path, attributes })
}

fn eq_attribute() -> impl AstParser<EqAttribute> {
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

/// Expression parser
fn expr() -> impl AstParser<Expr> {
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

/// Type parser
fn ty() -> impl AstParser<Type> {
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

fn doc_comments() -> impl AstParser<Vec<String>> {
    select! { Token::DocComment(string) => string }.repeated()
}

fn keyword(token: Token) -> impl AstParser<Span> {
    just(token).map_with_span(|_, span| span)
}

fn path() -> impl AstParser<String> {
    select! { Token::Sym(ident) => ident }
}

fn ident() -> impl AstParser<String> {
    select! { Token::Sym(ident) => ident }
}

fn variable() -> impl AstParser<String> {
    just(Token::Sigil(':')).ignore_then(ident())
}

fn number_literal() -> impl AstParser<String> {
    select! { Token::Number(string) => string }
}

fn string_literal() -> impl AstParser<String> {
    select! { Token::StringLiteral(string) => string }
}

/// Parser combinator that transforms an output type O into Spanned<O>.
fn spanned<P: AstParser<O>, O>(parser: P) -> impl AstParser<Spanned<O>> {
    parser.map_with_span(|t, span| (t, span))
}

/// Map the inner type T in Spanned<T> into U.
fn span_map<T, U, F>(f: F) -> impl (Fn(Spanned<T>) -> Spanned<U>) + Clone
where
    F: (Fn(T) -> U) + Clone,
{
    move |(data, span)| (f(data), span)
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use chumsky::Stream;

    use crate::parse::lexer::lexer;

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
        /// doc comment
        type foo
        /// doc comment
        type bar {
            rel a { '' } b
            rel { lol } c
            rel { '' } { '' }
            rel { '' } b { '' }
        }
        ";

        let stmts = parse(source).unwrap();
        assert_matches!(stmts.as_slice(), [Stmt::Type(_), Stmt::Type(_)]);

        assert_eq!(1, stmts[0].docs().len());
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

        // comment
        eq (:x :y) {
            foo { :x + 1 }
            bar {
                rel { 'foo' } (:x / 3) + 4
            }
        }
        ";

        let stmts = parse(source).unwrap();
        assert_matches!(stmts.as_slice(), [Stmt::Eq(_), Stmt::Eq(_)]);
    }
}
