use chumsky::prelude::Simple;

use super::{ast::*, tree::Tree, tree_stream::TreeStream, Span, Spanned};

pub fn parse((tree, span): Spanned<Tree>) -> ParseResult<Ast> {
    match tree {
        Tree::Paren(list) => parse_ast(TreeStream::new(list, span)),
        Tree::Comment(comment) => Ok((Ast::Comment(comment), span)),
        _ => Err(error("expected list", span)),
    }
}

fn parse_ast(mut input: TreeStream) -> ParseResult<Ast> {
    let (keyword, span) = input.next_sym_msg("expected keyword")?;

    match keyword.as_str() {
        "import" => parse_import(input),
        "type!" => {
            let span = input.span();
            let ident = input.next_sym_msg("expected ident")?;
            input.end()?;
            Ok((Ast::Type(ident), span))
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
    let (next, type_span) = stream.next_msg(Some, "expected type")?;
    match next {
        Tree::Paren(trees) => {
            let mut ty_stream = TreeStream::new(trees, type_span);
            let type_span = ty_stream.span();
            let (kind, _) = ty_stream.next_sym_msg("")?;

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
    let (rel_ident, ident_span) = stream.next_sym_msg("expected relation identifier")?;
    let ident = match rel_ident.as_str() {
        "_" => None,
        _ => Some(rel_ident),
    };

    let mut cardinality = Cardinality::One;

    if let Some(_) = stream.peek(|tree| match tree {
        Tree::Bracket(v) if v.len() == 0 => Some(&()),
        _ => None,
    }) {
        stream.next().unwrap();
        cardinality = Cardinality::Many;
    }

    let object = parse_type(&mut stream)?;
    stream.end()?;

    Ok((
        Ast::Rel(Rel {
            subject,
            ident: (ident, ident_span),
            cardinality,
            object,
        }),
        span,
    ))
}

fn parse_path(input: &mut TreeStream) -> ParseResult<Path> {
    let (initial, mut span) = input.next_sym()?;
    let mut symbols = vec![(initial, span.clone())];

    while input.peek_dot() {
        let _ = input.next_dot()?;
        let (next, next_span) = input.next_sym()?;

        symbols.push((next, next_span.clone()));

        span.end = next_span.end;
    }
    input.end()?;

    Ok((Path(symbols), span))
}

fn parse_eq(mut input: TreeStream) -> ParseResult<Ast> {
    let span = input.span();

    let mut params = vec![];
    let mut input_params = input.next_list_msg("expected param list")?;
    while input_params.peek_any() {
        let (var, span) = input_params.next_var()?;
        params.push((var, span));
    }

    let first = parse_next_expr(&mut input)?;
    let second = parse_next_expr(&mut input)?;
    input.end()?;

    Ok((
        Ast::Eq(Eq {
            variables: params.into(),
            first,
            second,
        }),
        span,
    ))
}

fn parse_next_expr(input: &mut TreeStream) -> ParseResult<Expr> {
    parse_expr(input.next_msg(Some, "expected expression")?)
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
    let sym = input.next_sym()?;

    match sym.0.as_str() {
        "obj!" => {
            let typename = input.next_sym_msg("expected type name")?;

            let mut attributes = vec![];

            while input.peek_any() {
                let mut list = input.next_list_msg("expected attribute")?;
                let span = list.span();
                let (property, prop_span) = list.next_sym_msg("expected property")?;
                let value = parse_next_expr(&mut list)?;
                list.end()?;

                attributes.push((
                    Attribute {
                        property: (
                            match property.as_str() {
                                "_" => Property::Wildcard,
                                _ => Property::Named(property),
                            },
                            prop_span,
                        ),
                        value,
                    },
                    span,
                ));
            }

            Ok((Expr::Obj(typename, attributes.into()), span))
        }
        _ => {
            let mut args = vec![];

            while input.peek_any() {
                args.push(parse_next_expr(&mut input)?);
            }

            Ok((Expr::Call(sym, args.into()), span))
        }
    }
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
