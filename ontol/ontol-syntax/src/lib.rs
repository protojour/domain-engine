use std::ops::Range;

use ontol_parser::{
    cst::{parser::CstParser, tree::SyntaxMarker},
    lexer::kind::Kind,
};

pub mod view;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct OntolLang;

impl rowan::Language for OntolLang {
    type Kind = Kind;

    fn kind_from_raw(raw: rowan::SyntaxKind) -> Self::Kind {
        assert!(raw.0 <= Kind::Ontol as u16);
        unsafe { std::mem::transmute::<u16, Kind>(raw.0) }
    }

    fn kind_to_raw(kind: Self::Kind) -> rowan::SyntaxKind {
        rowan::SyntaxKind(kind as u16)
    }
}

pub fn parse_syntax(ontol_src: &str, grammar: fn(&mut CstParser)) -> rowan::SyntaxNode<OntolLang> {
    let green_root = parse_green(ontol_src, grammar);
    rowan::SyntaxNode::<OntolLang>::new_root(green_root)
}

pub fn parse_green(ontol_src: &str, grammar: fn(&mut CstParser)) -> rowan::GreenNode {
    let (flat_tree, _errors) = ontol_parser::cst_parse_grammar(ontol_src, grammar);

    let mut builder = rowan::GreenNodeBuilder::new();
    let (markers, lex) = flat_tree.into_markers_and_lex();

    for marker in markers {
        match marker {
            SyntaxMarker::Start { kind } => {
                builder.start_node(rowan::SyntaxKind(kind as u16));
            }
            SyntaxMarker::Token { index } | SyntaxMarker::Ignorable { index } => {
                let kind = lex.kind(index as usize);
                let span: Range<usize> = lex.span(index as usize).into();

                builder.token(rowan::SyntaxKind(kind as u16), &ontol_src[span]);
            }
            SyntaxMarker::End => {
                builder.finish_node();
            }
        }
    }

    builder.finish()
}

#[cfg(test)]
mod tests {
    use ontol_parser::cst::{
        grammar,
        inspect::{Node, Statement},
        view::NodeViewExt,
    };
    use pretty_assertions::assert_eq;

    use crate::{parse_green, parse_syntax, view::View};

    #[test]
    fn syntax_view() {
        let src = "def foo()";
        let root = parse_syntax(src, grammar::ontol);

        assert_eq!(root.children().count(), 1);
        assert_eq!(src, root.green().to_string());

        let Node::Ontol(ontol) = root.view().node() else {
            panic!();
        };

        assert_eq!(1, ontol.statements().count());
    }

    #[test]
    fn syntax_edit() {
        let src = "def foo()";
        let root = parse_syntax(src, grammar::ontol);

        let Node::Ontol(ontol) = root.view().node() else {
            panic!()
        };
        let Statement::DefStatement(def) = ontol.statements().next().unwrap() else {
            panic!();
        };
        let ident_path = def.ident_path().unwrap();

        let new = ident_path
            .view()
            .0
            .replace_with(parse_green("bar", grammar::ident_path));

        assert_eq!("def bar()", new.to_string());
    }
}
