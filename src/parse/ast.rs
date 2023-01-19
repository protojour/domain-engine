use chumsky::prelude::*;

use crate::parse::{tree::Tree, tree_stream::TreeStream, SString, Spanned};

use super::Span;

pub type ParseResult<T> = Result<Spanned<T>, Simple<Tree>>;

pub enum Ast {
    Import(Path),
    List(Vec<Spanned<Ast>>),
    Data(Data),
}

pub struct Data {
    pub ident: Spanned<SString>,
    pub ty: Spanned<Type>,
}

pub enum Type {
    Sym(SString),
    Record(Record),
}

pub struct Record {
    pub fields: Vec<Spanned<RecordField>>,
}

pub struct RecordField {
    pub ident: Spanned<SString>,
    pub ty: Spanned<Type>,
}

pub struct Path(Vec<Spanned<SString>>);

pub fn parse((tree, span): Spanned<Tree>) -> ParseResult<Ast> {
    match tree {
        Tree::Paren(list) => parse_ast(TreeStream::new(span, list)),
        _ => Err(error(span, "expected list")),
    }
}

fn parse_ast(mut input: TreeStream) -> ParseResult<Ast> {
    let (keyword, span) = input.next_sym_msg("expected keyword")?;

    match keyword.as_str() {
        "import" => parse_import(input),
        "data" => parse_data(input),
        _ => Err(error(span, "unknown keyword")),
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
        "number" => Type::Sym(kind),
        "record" => {
            let (fields, _) = parse_record_fields(&mut ty_stream)?;
            Type::Record(Record { fields })
        }
        _ => Err(error(span, "Unrecognized type"))?,
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
            return Err(error(kw_span, "expected field keyword"));
        }
        let ident = field.next_sym_msg("expected field identifier")?;
        let ty = parse_type(&mut field)?;
        field.end()?;

        fields.push((RecordField { ident, ty }, field.span()));
    }

    Ok((fields, span))
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

    Ok((Path(symbols), span))
}

pub fn error(span: Span, msg: impl ToString) -> Simple<Tree> {
    Simple::custom(span, msg)
}

#[cfg(test)]
mod tests {
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
}
