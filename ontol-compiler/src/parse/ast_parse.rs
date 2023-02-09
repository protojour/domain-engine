use chumsky::prelude::Simple;
use smartstring::alias::String;

use super::{
    ast::*,
    tree::Tree,
    tree_stream::{
        At, Bracket, Dot, Num, Paren, Questionmark, Sym, TreeStream, Underscore, Variable,
    },
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
        "rel!" => parse_rel(input),
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
        Tree::Paren(trees) => {
            let mut ty_stream = TreeStream::new(trees, type_span);
            let type_span = ty_stream.span();
            let (kind, _) = ty_stream.next::<Sym>("")?;

            let ty = match kind.as_str() {
                "tuple!" => {
                    let mut elems = vec![];
                    while ty_stream.peek_any() {
                        elems.push(parse_type(&mut ty_stream)?);
                    }

                    Type::Tuple(elems)
                }
                _ => Type::Sym(kind),
            };

            ty_stream.end()?;
            Ok((ty, type_span))
        }
        Tree::StringLiteral(lit) => Ok((Type::Literal(Literal::String(lit)), type_span)),
        _ => Err(error("invalid type", type_span)),
    }
}

fn parse_rel(mut stream: TreeStream) -> ParseResult<Ast> {
    let span = stream.span();
    let subject = parse_type(&mut stream)?;
    let (ident, ident_span) = parse_sym_or_wildcard(&mut stream, "expected relation identifier")?;
    let subject_cardinality = parse_optional_cardinality(&mut stream)?;

    let edge_params = if stream.peek::<At>() {
        let _ = stream.next::<At>("").unwrap();
        Some(parse_type(&mut stream)?)
    } else {
        None
    };

    let (object_prop_ident, object_cardinality) = if stream.peek::<Sym>() {
        let sym = stream.next::<Sym>("").unwrap();
        let object_cardinality = parse_optional_cardinality(&mut stream)?;

        (Some(sym), object_cardinality)
    } else {
        (None, None)
    };

    let object = parse_type(&mut stream)?;
    stream.end()?;

    Ok((
        Ast::Rel(Rel {
            subject,
            ident: (ident, ident_span),
            subject_cardinality,
            edge_params,
            object_cardinality,
            object_prop_ident,
            object,
        }),
        span,
    ))
}

fn parse_optional_cardinality(input: &mut TreeStream) -> Result<Option<Cardinality>, Simple<Tree>> {
    if input.peek::<Bracket>() {
        let mut stream: TreeStream = input.next::<Bracket>("expected bracket")?.into();

        let mut has_range = false;
        let mut start: Option<u16> = None;
        let mut end: Option<u16> = None;

        if stream.peek::<Num>() {
            let (s, _) = parse_u16(&mut stream)?;
            start = Some(s);
        }

        if stream.peek::<Dot>() {
            stream.next::<Dot>("expected dot")?;
            stream.next::<Dot>("expected dot")?;
            has_range = true;
        }

        if stream.peek::<Num>() {
            let (e, _) = parse_u16(&mut stream)?;
            end = Some(e);
        }

        if has_range && start.is_none() && end.is_none() {
            return Err(error("expected array range constraint", stream.span()));
        }

        stream.end()?;

        if input.peek::<Questionmark>() {
            let _ = input.next::<Questionmark>("").unwrap();
            Ok(Some(Cardinality::OptionalMany(std::ops::Range {
                start,
                end,
            })))
        } else {
            Ok(Some(Cardinality::Many(std::ops::Range { start, end })))
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
        let (var, span) = input_params.next::<Variable>("expected variable")?;
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
    parse_expr(input.next_any("expected expression")?)
}

fn parse_expr((tree, span): Spanned<Tree>) -> ParseResult<Expr> {
    match tree {
        Tree::Sym(string) => Ok((Expr::Sym(string), span)),
        Tree::Variable(string) => Ok((Expr::Variable(string), span)),
        Tree::Num(string) => Ok((Expr::Literal(Literal::Int(string)), span)),
        Tree::Paren(list) => parse_list_expr(TreeStream::new(list, span)),
        _ => Err(error("invalid expression", span)),
    }
}

fn parse_list_expr(mut input: TreeStream) -> ParseResult<Expr> {
    let span = input.span();
    let sym = input.next::<Sym>("expected identifier")?;

    match sym.0.as_str() {
        "obj!" => {
            let typename = input.next::<Sym>("expected type name")?;

            let mut attributes = vec![];

            while input.peek_any() {
                let mut list: TreeStream = input.next::<Paren>("expected attribute")?.into();
                let span = list.span();
                let (property, prop_span) = parse_sym_or_wildcard(&mut list, "expected property")?;
                let value = parse_next_expr(&mut list)?;
                list.end()?;

                attributes.push((
                    Attribute {
                        property: (
                            property.map(Property::Named).unwrap_or(Property::Wildcard),
                            prop_span,
                        ),
                        value,
                    },
                    span,
                ));
            }

            Ok((Expr::Obj(typename, attributes), span))
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

fn parse_sym_or_wildcard(
    input: &mut TreeStream,
    msg: impl ToString,
) -> ParseResult<Option<String>> {
    if input.peek::<Underscore>() {
        let (_, span) = input.next::<Underscore>("").unwrap();
        Ok((None, span))
    } else {
        let (ident, span) = input.next::<Sym>(msg)?;
        Ok((Some(ident), span))
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
