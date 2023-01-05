use std::ops::Range;

use chumsky::prelude::*;
use smartstring::{LazyCompact, SmartString};

use crate::tree::Tree;

type SmString = SmartString<LazyCompact>;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

#[derive(Clone, Eq, PartialEq)]
pub enum Ast {
    Nothing,
    List(Vec<Spanned<Ast>>),
    Struct(SmString),
    Import(),
}

pub fn parser() -> impl Parser<Tree, Spanned<Ast>, Error = Simple<Tree>> + Clone {
    recursive(|_tree| {
        // let list = token.delimited_by(just(Token::LParen), just(Token::RParen));
        //.map(Ast::List);
        //.map(Ast::List);

        // just(Ast::Nothing).foldl(|a, b| a)
        // .labelled("null")

        let import = just(Tree::Sym("import".into())).map(|_| Ast::Import());

        import.map_with_span(|ast, span| (ast, span))
    })
}

#[cfg(test)]
mod tests {
    use chumsky::Stream;

    use super::*;

    fn test_parse(trees: Vec<Tree>) {
        let len = trees.len();
        parser()
            .parse(Stream::from_iter(
                len..len + 1,
                trees
                    .into_iter()
                    .enumerate()
                    .map(|(index, token)| (token, index..index + 1)),
            ))
            .unwrap();
    }

    #[test]
    fn huh() {
        //let tokens = test_parse(vec![Tree::LParen, Token::Ident("a".into()), Token::RParen]);
    }
}
