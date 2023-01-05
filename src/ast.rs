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
    ident: Spanned<SString>,
}

pub struct Path(Vec<Spanned<SString>>);

pub fn parse_tree((tree, span): Spanned<Tree>) -> ParseResult<Ast> {
    match tree {
        Tree::Paren(list) => parse_list(TreeStream::new(span, list)),
        _ => Err(error(span, "expected list")),
    }
}

fn parse_list(mut input: TreeStream) -> ParseResult<Ast> {
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

    while input.peek_any() {
        let _member = input.next_list("expected member")?;
    }

    Ok((Ast::Struct(Struct { ident }), span))
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
        parse_tree(tree).unwrap().0
    }

    #[test]
    fn parse_import() {
        let Ast::Import(Path(symbols)) = test_parse("(import a.b)") else {
            panic!();
        };

        assert_eq!(2, symbols.len());
    }
}
