use chumsky::prelude::*;

use crate::{tree::Tree, tree_stream::TreeStream, SString, Span, Spanned};

pub type ParseResult<T> = Result<Spanned<T>, Simple<Tree>>;

pub enum Ast {
    Nothing,
    List(Vec<Spanned<Ast>>),
    Struct(Struct),
    Import(Path),
}

pub struct Struct {
    pub ident: Spanned<SString>,
    pub fields: Vec<Spanned<StructField>>,
}

pub struct StructField {}

pub struct Path(Vec<Spanned<SString>>);

pub fn parse((tree, span): Spanned<Tree>) -> ParseResult<Ast> {
    match tree {
        Tree::Paren(list) => parse_ast(TreeStream::new(span, list)),
        _ => Err(error(span, "expected list")),
    }
}

fn parse_ast(mut input: TreeStream) -> ParseResult<Ast> {
    let (keyword, span) = input.next_sym_msg("Expected keyword")?;

    match keyword.as_str() {
        "import" => parse_import(input),
        "struct" => parse_struct(input),
        _ => Err(error(span, "Unknown keyword")),
    }
}

fn parse_import(mut input: TreeStream) -> ParseResult<Ast> {
    let (path, span) = parse_path(&mut input)?;
    input.end()?;
    Ok((Ast::Import(path), span))
}

fn parse_struct(mut input: TreeStream) -> ParseResult<Ast> {
    let span = input.span();
    let ident = input.next_sym_msg("expected identifier")?;
    let mut fields = vec![];

    while input.peek_any() {
        let field = input.next_list("expected field")?;

        fields.push((StructField {}, field.span()));
    }

    Ok((Ast::Struct(Struct { ident, fields }), span))
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
    use crate::tree::tree_parser;

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
    fn parse_struct() {
        let src = "

            (struct foobar
                (field)
                (else)
            )
        ";
        let Ast::Struct(s) = test_parse(src) else {
            panic!();
        };

        assert_eq!("foobar", s.ident.0);
        assert_eq!(2, s.fields.len());
    }
}
