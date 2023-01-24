use chumsky::prelude::Simple;

use super::{ast::*, tree::Tree, tree_stream::TreeStream, Span, Spanned};

pub fn parse((tree, span): Spanned<Tree>) -> ParseResult<Ast> {
    match tree {
        Tree::Paren(list) => parse_ast(TreeStream::new(list, span)),
        _ => Err(error("expected list", span)),
    }
}

fn parse_ast(mut input: TreeStream) -> ParseResult<Ast> {
    let (keyword, span) = input.next_sym_msg("expected keyword")?;

    match keyword.as_str() {
        "import" => parse_import(input),
        "data" => parse_data(input),
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

fn parse_data(mut input: TreeStream) -> ParseResult<Ast> {
    let span = input.span();
    let ident = input.next_sym_msg("expected identifier")?;
    let ty = parse_type(&mut input)?;
    input.end()?;

    Ok((Ast::Data(Data { ident, ty }), span))
}

fn parse_type(stream: &mut TreeStream) -> ParseResult<Type> {
    let type_span = stream.span();
    let mut ty_stream = stream.next_list_msg("expected type")?;
    let (kind, span) = ty_stream.next_sym_msg("")?;

    let ty = match kind.as_str() {
        "record" => {
            let (fields, _) = parse_record_fields(&mut ty_stream)?;
            Type::Record(Record { fields })
        }
        /*
        "number" => Type::Sym(kind),
        _ => Err(error("Unrecognized type", span))?,
        */
        _ => Type::Sym(kind),
    };

    ty_stream.end()?;
    Ok((ty, type_span))
}

fn parse_record_fields(record: &mut TreeStream) -> ParseResult<Vec<Spanned<RecordField>>> {
    let span = record.span();
    let mut fields = vec![];

    while record.peek_any() {
        let mut field = record.next_list_msg("expected field s-expression")?;
        let (keyword, kw_span) = field.next_sym_msg("expected field")?;
        if keyword != "field" {
            return Err(error("expected field keyword", kw_span));
        }
        let ident = field.next_sym_msg("expected field identifier")?;
        let ty = parse_type(&mut field)?;
        field.end()?;

        fields.push((RecordField { ident, ty }, field.span()));
    }

    Ok((fields, span))
}

fn parse_rel(mut stream: TreeStream) -> ParseResult<Ast> {
    let span = stream.span();
    let subject = parse_type(&mut stream)?;
    let (rel_ident, ident_span) = stream.next_sym_msg("expected relation identifier")?;
    let ident = match rel_ident.as_str() {
        "_" => RelIdent::Unnamed,
        _ => RelIdent::Named(rel_ident),
    };
    let object = parse_type(&mut stream)?;
    stream.end()?;

    Ok((
        Ast::Rel(Rel {
            subject,
            ident: (ident, ident_span),
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
    input.next_list_msg("expected param list")?.end()?;

    let first = parse_next_expr(&mut input)?;
    let second = parse_next_expr(&mut input)?;
    input.end()?;

    Ok((
        Ast::Eq(Eq {
            params: (),
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
        Tree::Num(string) => Ok((Expr::Literal(Literal::Number(string)), span)),
        Tree::Paren(list) => parse_expr_call(TreeStream::new(list, span)),
        _ => Err(error("invalid expression", span)),
    }
}

fn parse_expr_call(mut input: TreeStream) -> ParseResult<Expr> {
    let span = input.span();
    let sym = input.next_sym()?;
    let mut args = vec![];

    while input.peek_any() {
        args.push(parse_next_expr(&mut input)?);
    }

    Ok((Expr::Call(sym, args), span))
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
    fn parse_data_record() {
        let src = "
            (data foobar (record (field name (number))))
        ";
        let Ast::Data(Data { ident, ty: (Type::Record(record), _), }) = test_parse(src) else {
            panic!();
        };

        assert_eq!("foobar", ident.0);
        assert_eq!(1, record.fields.len());

        let (RecordField { ty: (Type::Sym(field_type), _), .. }, _) = &record.fields[0] else {
            panic!();
        };

        assert_eq!(field_type, "number");
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
        let Expr::Literal(Literal::Number(_)) = second.0 else {
            panic!("not a number");
        };
    }
}
