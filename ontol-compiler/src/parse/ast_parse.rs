use std::ops::Range;

use chumsky::prelude::Simple;

use super::{
    ast::*,
    tree::Tree,
    tree_stream::{Brace, Colon, Dot, Num, Paren, Questionmark, StringLiteral, Sym, TreeStream},
    Span, Spanned,
};

pub fn parse((tree, span): Spanned<Tree>) -> ParseResult<Ast> {
    match tree {
        Tree::Paren(list) => parse_ast(TreeStream::new(list, span)),
        Tree::Comment(comment) => Ok((Ast::Comment(comment), span)),
        _ => Err(error("expected list", span)),
    }
}

fn parse_ast(mut input: TreeStream) -> ParseResult<Ast> {
    let (keyword, span) = input.next::<Sym>("expected keyword")?;

    match keyword.as_str() {
        "import" => parse_import(input),
        "type!" => {
            let span = input.span();
            let ident = input.next::<Sym>("expected ident")?;
            input.end()?;
            Ok((Ast::Type(ident), span))
        }
        "entity!" => {
            let span = input.span();
            let ident = input.next::<Sym>("expected ident")?;
            input.end()?;
            Ok((Ast::Entity(ident), span))
        }
        "rel!" => parse_relationship(input),
        "eq!" => parse_eq(input),
        _ => Err(error("unknown keyword", span)),
    }
}

fn parse_import(mut input: TreeStream) -> ParseResult<Ast> {
    let (path, span) = parse_path(&mut input)?;
    input.end()?;
    Ok((Ast::Import(path), span))
}

fn parse_type(stream: &mut TreeStream) -> ParseResult<Type> {
    let (next, type_span) = stream.next_any("expected type")?;
    match next {
        Tree::Sym(sym) => {
            let ty = Type::Sym(sym);
            Ok((ty, type_span))
        }
        Tree::StringLiteral(lit) => Ok((Type::Literal(Literal::String(lit)), type_span)),
        Tree::Underscore => Ok((Type::Unit, type_span)),
        Tree::Bracket(tree) => {
            if !tree.is_empty() {
                return Err(error("sequence must be empty", type_span));
            }
            Ok((Type::EmptySequence, type_span))
        }
        _ => Err(error("invalid type", type_span)),
    }
}

fn parse_relationship(mut stream: TreeStream) -> ParseResult<Ast> {
    let span = stream.span();
    let subject = parse_type(&mut stream)?;
    let connection = parse_connection(&mut stream)?;

    let mut chain = vec![];
    let mut object = None;

    while let Some(next_span) = stream.peek_span() {
        if stream.peek::<Brace>() {
            chain.push(RelationshipChain {
                subject: object,
                connection: parse_connection(&mut stream)?,
            });
            object = None;
        } else if object.is_none() {
            object = Some(parse_type(&mut stream)?);
        } else {
            return Err(error("expected brace", next_span));
        }
    }

    stream.end()?;

    let object = object.ok_or_else(|| error("expected object type", stream.remain_span()))?;

    Ok((
        Ast::Relationship(Box::new(Relationship {
            subject,
            connection,
            chain,
            object,
        })),
        span,
    ))
}

fn parse_connection(stream: &mut TreeStream) -> Result<Connection, Simple<Tree>> {
    let (brace, brace_span) = stream.next::<Brace>("expected brace")?;
    let mut brace = TreeStream::new(brace, brace_span);

    let ty = ConnectionType::parse(&mut brace, "expected string literal or range")?;
    let subject_cardinality = parse_optional_cardinality(&mut brace)?;

    let (object_prop_ident, object_cardinality) = if brace.peek::<StringLiteral>() {
        let lit = brace.next::<StringLiteral>("").unwrap();
        let object_cardinality = parse_optional_cardinality(&mut brace)?;

        (Some(lit), object_cardinality)
    } else {
        (None, None)
    };

    let rel_params = if brace.peek::<Colon>() {
        let _ = brace.next::<Colon>("").unwrap();
        Some(parse_type(&mut brace)?)
    } else {
        None
    };

    brace.end()?;

    Ok(Connection {
        ty,
        subject_cardinality,
        rel_params,
        object_cardinality,
        object_prop_ident,
    })
}

fn parse_optional_cardinality(input: &mut TreeStream) -> Result<Option<Cardinality>, Simple<Tree>> {
    if input.peek::<Sym>() {
        let (sym, sym_span) = input.next::<Sym>("")?;

        match sym.as_str() {
            "*" => {}
            _ => {
                return Err(error("expected `*` or `?`", sym_span));
            }
        };

        if input.peek::<Questionmark>() {
            let _ = input.next::<Questionmark>("").unwrap();
            Ok(Some(Cardinality::OptionalMany))
        } else {
            Ok(Some(Cardinality::Many))
        }
    } else if input.peek::<Questionmark>() {
        let _ = input.next::<Questionmark>("").unwrap();
        Ok(Some(Cardinality::Optional))
    } else {
        Ok(None)
    }
}

fn parse_path(input: &mut TreeStream) -> ParseResult<Path> {
    let (initial, mut span) = input.next::<Sym>("expected identifier")?;
    let mut symbols = vec![(initial, span.clone())];

    while input.peek::<Dot>() {
        let _ = input.next::<Dot>("expected dot")?;
        let (next, next_span) = input.next::<Sym>("expected identifier")?;

        symbols.push((next, next_span.clone()));

        span.end = next_span.end;
    }
    input.end()?;

    Ok((Path(symbols), span))
}

fn parse_eq(mut input: TreeStream) -> ParseResult<Ast> {
    let span = input.span();

    let mut variables = vec![];
    let mut input_params: TreeStream = input.next::<Paren>("expected param list")?.into();
    while input_params.peek_any() {
        let _ = input_params.next::<Colon>("expected colon")?;
        let (var, span) = input_params.next::<Sym>("expected variable name")?;
        variables.push((var, span));
    }

    let first = parse_next_expr(&mut input)?;
    let second = parse_next_expr(&mut input)?;
    input.end()?;

    Ok((
        Ast::Eq(Eq {
            variables,
            first,
            second,
        }),
        span,
    ))
}

fn parse_next_expr(input: &mut TreeStream) -> ParseResult<Expr> {
    match input.next_any("expected expression")? {
        (Tree::Sym(string), span) => Ok((Expr::Sym(string), span)),
        // (Tree::Variable(string), span) => Ok((Expr::Variable(string), span)),
        (Tree::Colon, _) => {
            let (variable, span) = input.next::<Sym>("expected variable name")?;
            Ok((Expr::Variable(variable), span))
        }
        (Tree::Num(string), span) => Ok((Expr::Literal(Literal::Int(string)), span)),
        (Tree::Paren(list), span) => parse_list_expr(TreeStream::new(list, span)),
        (_, span) => Err(error("invalid expression", span)),
    }
}

fn parse_list_expr(mut input: TreeStream) -> ParseResult<Expr> {
    let span = input.span();
    let sym = input.next::<Sym>("expected identifier")?;

    match sym.0.as_str() {
        "obj!" => {
            let typename = input.next::<Sym>("expected type name")?;

            if input.peek::<Brace>() {
                let mut attributes = vec![];

                while input.peek_any() {
                    let mut brace: TreeStream = input.next::<Brace>("expected attribute")?.into();
                    let span = brace.span();
                    let ty = parse_type(&mut brace)?;
                    let value = parse_next_expr(&mut brace)?;
                    brace.end()?;

                    attributes.push((Attribute { ty, value }, span));
                }

                Ok((Expr::Obj(typename, attributes), span))
            } else if input.peek_any() {
                let (value, value_span) = parse_next_expr(&mut input)?;
                Ok((
                    Expr::Obj(
                        typename,
                        vec![(
                            Attribute {
                                ty: (Type::Unit, value_span.clone()),
                                value: (value, value_span.clone()),
                            },
                            value_span,
                        )],
                    ),
                    span,
                ))
            } else {
                Ok((Expr::Obj(typename, vec![]), span))
            }
        }
        _ => {
            let mut args = vec![];

            while input.peek_any() {
                args.push(parse_next_expr(&mut input)?);
            }

            Ok((Expr::Call(sym, args), span))
        }
    }
}

impl ConnectionType {
    fn parse(input: &mut TreeStream, _: impl ToString) -> ParseResult<Self> {
        if input.peek::<Num>() {
            let (num, span) = input.next::<Num>("").unwrap();
            let start: u16 = num
                .parse()
                .map_err(|_| error("must be an integer", span.clone()))?;

            let end = if input.peek::<Dot>() {
                parse_dot_dot_opt_u16(input)?.0
            } else {
                // if no dot, it's just an index, so the range is n..n+1
                Some(start + 1)
            };

            Ok((
                Self::IntRange(Range {
                    start: Some(start),
                    end,
                }),
                span,
            ))
        } else if input.peek::<Dot>() {
            let (end, span) = parse_dot_dot_opt_u16(input)?;
            let end = end.ok_or_else(|| error("expected number", span.clone()))?;

            Ok((
                Self::IntRange(Range {
                    start: None,
                    end: Some(end),
                }),
                span,
            ))
        } else {
            let (ty, span) = parse_type(input)?;
            Ok((Self::Type((ty, span.clone())), span))
        }
    }
}

fn parse_dot_dot_opt_u16(input: &mut TreeStream) -> ParseResult<Option<u16>> {
    let (_, dot_span) = input.next::<Dot>("expected dot")?;
    input.next::<Dot>("expected dot")?;
    if input.peek::<Num>() {
        let (num, span) = parse_u16(input)?;
        Ok((Some(num), span))
    } else {
        Ok((None, dot_span))
    }
}

fn parse_u16(input: &mut TreeStream) -> ParseResult<u16> {
    let (num, span) = input.next::<Num>("expected number")?;
    Ok((
        num.parse()
            .map_err(|_| error("unable to parse number", span.clone()))?,
        span,
    ))
}

pub fn error(msg: impl ToString, span: Span) -> Simple<Tree> {
    Simple::custom(span, msg)
}

#[cfg(test)]
mod tests {
    use chumsky::Parser;

    use crate::parse::tree::tree_parser;

    use super::*;

    fn test_parse(input: &str) -> Ast {
        let tree = tree_parser().parse(input).unwrap();
        parse(tree).unwrap().0
    }

    #[test]
    fn parse_import() {
        let Ast::Import(Path(symbols)) = test_parse("(import a.b)") else {
            panic!();
        };

        assert_eq!(2, symbols.len());
    }

    #[test]
    fn parse_eq() {
        let src = "(eq! () (a b) 42)";
        let Ast::Eq(Eq { first, second, .. }) = test_parse(src) else {
            panic!();
        };

        let Expr::Call(_, _) = first.0 else {
            panic!("not a call");
        };
        let Expr::Literal(Literal::Int(_)) = second.0 else {
            panic!("not a number");
        };
    }
}
