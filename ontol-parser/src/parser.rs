use std::ops::Range;

use chumsky::prelude::*;
use smartstring::alias::String;

use crate::ast::{Path, UseStatement};

use super::{
    ast::{
        BinaryOp, Cardinality, ChainedSubjectConnection, EqAttribute, EqAttributeRel, EqStatement,
        EqType, Expression, RelConnection, RelStatement, RelType, Statement, Type, TypeStatement,
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
pub fn statement_sequence() -> impl AstParser<Vec<Spanned<Statement>>> {
    header_statement()
        .repeated()
        .chain(domain_statement().repeated())
        .then_ignore(end())
}

/// Statement allowed only at the top/header section of a file
fn header_statement() -> impl AstParser<Spanned<Statement>> {
    spanned(use_statement()).map(span_map(Statement::Use))
}

fn domain_statement() -> impl AstParser<Spanned<Statement>> {
    let type_stmt = spanned(type_statement()).map(span_map(Statement::Type));
    let rel_stmt = spanned(rel_statement()).map(span_map(Statement::Rel));
    let eq_stmt = spanned(eq_statement()).map(span_map(Statement::Eq));

    type_stmt.or(rel_stmt).or(eq_stmt)
}

fn use_statement() -> impl AstParser<UseStatement> {
    keyword(Token::Use)
        .then(spanned(string_literal()))
        .then_ignore(sym("as", "`as`"))
        .then(spanned(ident()))
        .map(|((kw, source), as_ident)| UseStatement {
            kw,
            reference: source,
            as_ident,
        })
}

fn type_statement() -> impl AstParser<TypeStatement> {
    doc_comments()
        .then(keyword(Token::Type))
        .then(spanned(ident()))
        .then(spanned(
            spanned(rel_statement())
                .repeated()
                .delimited_by(open('{'), close('}'))
                .or_not(),
        ))
        .map(|(((docs, kw), ident), rel_block)| TypeStatement {
            docs,
            kw,
            ident,
            rel_block,
        })
}

fn rel_statement() -> impl AstParser<RelStatement> {
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
            |(((((docs, kw), subject), connection), chain), object)| RelStatement {
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
    let rel_ty_int_range = spanned(u16_range()).map(RelType::IntRange);
    let rel_ty_type = spanned(ty()).map(RelType::Type);

    let rel_ty = rel_ty_int_range.or(rel_ty_type);

    // type
    rel_ty
        .then(cardinality())
        // object prop and cardinality
        .then(
            sigil('|')
                .ignore_then(spanned(string_literal()))
                .then(cardinality())
                .or_not(),
        )
        // rel params
        .then(spanned(sigil(':').ignore_then(ty())).or_not())
        // within {}
        .delimited_by(open('{'), close('}'))
        .map(|(((ty, subject_cardinality), object), rel_params)| {
            let (object_prop_ident, object_cardinality) = match object {
                Some((prop, cardinality)) => (Some(prop), cardinality),
                None => (None, None),
            };

            RelConnection {
                ty,
                subject_cardinality,
                object_prop_ident,
                object_cardinality,
                rel_params,
            }
        })
}

fn cardinality() -> impl AstParser<Option<Cardinality>> {
    opt_sigil('*').then(opt_sigil('?')).map(|(many, option)| {
        match (many.is_some(), option.is_some()) {
            (true, true) => Some(Cardinality::OptionalMany),
            (true, false) => Some(Cardinality::Many),
            (false, true) => Some(Cardinality::Optional),
            (false, false) => None,
        }
    })
}

fn eq_statement() -> impl AstParser<EqStatement> {
    keyword(Token::Eq)
        .then(
            spanned(variable())
                .repeated()
                .delimited_by(open('('), close(')')),
        )
        .then(
            spanned(eq_type())
                .then(spanned(eq_type()))
                .delimited_by(open('{'), close('}')),
        )
        .map(|((kw, variables), (first, second))| EqStatement {
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
                .delimited_by(open('{'), close('}')),
        )
        .map(|(path, attributes)| EqType { path, attributes })
}

fn eq_attribute() -> impl AstParser<EqAttribute> {
    let variable = expression().map(EqAttribute::Expr);
    let rel = keyword(Token::Rel)
        .then(expression().or_not())
        .then(spanned(ty()).delimited_by(open('{'), close('}')))
        .then(expression().or_not())
        .map_with_span(|(((kw, subject), connection), object), span| {
            EqAttribute::Rel((
                EqAttributeRel {
                    kw,
                    subject,
                    connection,
                    object,
                },
                span,
            ))
        });

    variable.or(rel)
}

/// Expression parser
fn expression() -> impl AstParser<Spanned<Expression>> {
    recursive(|expr| {
        let path = path().map(Expression::Path);
        let variable = variable().map(Expression::Variable);
        let number_literal = number_literal().map(Expression::NumberLiteral);
        let string_literal = string_literal().map(Expression::StringLiteral);
        let group = expr.delimited_by(open('('), close(')'));

        let atom = spanned(path)
            .or(spanned(variable))
            .or(spanned(number_literal))
            .or(spanned(string_literal))
            .or(group);

        // Consume operators by precedende
        // TODO: Migrate to Pratt parse when Chumsky supports it.
        // these are large types and thus pretty slow to compile :(

        let add = sigil('+').to(BinaryOp::Add);
        let sub = sigil('-').to(BinaryOp::Sub);
        let mul = sigil('*').to(BinaryOp::Mul);
        let div = sigil('/').to(BinaryOp::Div);

        // infix precedence for multiplication and division:
        let product =
            atom.clone()
                .then(mul.or(div).then(atom).repeated())
                .foldl(|left, (op, right)| {
                    let span = left.1.start..right.1.end;
                    (
                        Expression::Binary(Box::new(left), op, Box::new(right)),
                        span,
                    )
                });

        // infix precedence for addition and subtraction:
        product
            .clone()
            .then(add.or(sub).then(product).repeated())
            .foldl(|left, (op, right)| {
                let span = left.1.start..right.1.end;
                (
                    Expression::Binary(Box::new(left), op, Box::new(right)),
                    span,
                )
            })
    })
}

/// Type parser
fn ty() -> impl AstParser<Type> {
    let unit = sigil('.').to(Type::Unit);
    let path = path().map(Type::Path);
    let number_literal = number_literal().map(Type::NumberLiteral);
    let string_literal = string_literal().map(Type::StringLiteral);
    let regex = select! { Token::Regex(string) => string }.map(Type::Regex);

    unit.or(path)
        .or(number_literal)
        .or(string_literal)
        .or(regex)
        .labelled("type")
}

fn doc_comments() -> impl AstParser<Vec<String>> {
    select! { Token::DocComment(string) => string }.repeated()
}

fn open(char: char) -> impl AstParser<Token> {
    just(Token::Open(char))
}

fn close(char: char) -> impl AstParser<Token> {
    just(Token::Close(char))
}

fn sigil(char: char) -> impl AstParser<Token> {
    just(Token::Sigil(char))
}

fn opt_sigil(char: char) -> impl AstParser<Option<Token>> {
    sigil(char).or_not()
}

fn keyword(token: Token) -> impl AstParser<Span> {
    just(token)
        .map_with_span(|_, span| span)
        .labelled("keyword")
}

fn path() -> impl AstParser<Path> {
    enum SpannedPath {
        Ident(Spanned<String>),
        Path(Vec<Spanned<String>>),
    }

    spanned(any_sym())
        .map(SpannedPath::Ident)
        .then(dot().ignore_then(spanned(any_sym())).repeated())
        .foldl(|prev, next| match prev {
            SpannedPath::Ident(ident) => SpannedPath::Path(vec![ident, next]),
            SpannedPath::Path(mut path) => {
                path.push(next);
                SpannedPath::Path(path)
            }
        })
        .map(|tmp_path| match tmp_path {
            // Remove the span from the ident case,
            // this span will always be the same as the Path itself:
            SpannedPath::Ident(spanned_sym) => Path::Ident(spanned_sym.0),
            SpannedPath::Path(path) => Path::Path(path),
        })
        .labelled("path")
}

fn any_sym() -> impl AstParser<String> {
    select! { Token::Sym(ident) => ident }
}

fn sym(str: &'static str, label: &'static str) -> impl AstParser<String> {
    select! { Token::Sym(string) => string }
        .labelled(label)
        .try_map(move |string, span| {
            if string == str {
                Ok(string)
            } else {
                Err(Simple::custom(span, format!("expected `{str}`")))
            }
        })
}

fn ident() -> impl AstParser<String> {
    select! { Token::Sym(ident) => ident }.labelled("identifier")
}

fn variable() -> impl AstParser<String> {
    just(Token::Sigil(':'))
        .ignore_then(ident())
        .labelled("variable")
}

fn number_literal() -> impl AstParser<String> {
    select! { Token::Number(string) => string }.labelled("number")
}

fn u16() -> impl AstParser<u16> {
    number_literal().try_map(|lit: String, span| {
        let u: u16 = lit
            .parse()
            .map_err(|_| Simple::custom(span, "unable to parse number"))?;
        Ok(u)
    })
}

fn u16_range() -> impl AstParser<Range<Option<u16>>> {
    let simple = u16()
        .then(dot_dot().ignore_then(u16().or_not()).or_not())
        .map(move |(start, end)| Range {
            start: Some(start),
            end: match end {
                Some(end) => end,
                None => Some(start + 1),
            },
        });
    let dot_dot_following = dot_dot().ignore_then(u16()).map(|end| Range {
        start: None,
        end: Some(end),
    });

    simple.or(dot_dot_following)
}

fn dot() -> impl AstParser<()> {
    just(Token::Sigil('.')).ignored()
}

fn dot_dot() -> impl AstParser<()> {
    dot().then_ignore(dot())
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

    use crate::lexer::lexer;

    use super::*;

    #[derive(Debug)]
    enum Error {
        Lex(Vec<Simple<char>>),
        Parse(Vec<Simple<Token>>),
    }

    fn parse(input: &str) -> Result<Vec<Statement>, Error> {
        let tokens = lexer().parse(input).map_err(Error::Lex)?;
        let len = input.len();
        let stmts = statement_sequence()
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
        assert_matches!(stmts.as_slice(), [Statement::Type(_), Statement::Type(_)]);

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
        assert_matches!(stmts.as_slice(), [Statement::Eq(_), Statement::Eq(_)]);
    }
}
