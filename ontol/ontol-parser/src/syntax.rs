//! Abstracted source of ONTOL syntax

use std::{ops::Range, sync::Arc};

use crate::{
    ast::Statement,
    cst::tree::{SyntaxTree, TreeNodeView},
    Error,
};

pub struct Syntax {
    pub kind: SyntaxKind,
    pub errors: Vec<Error>,
}

/// A representation of ONTOL syntax
///
/// NOTE: This is probably a temporary design
pub enum SyntaxKind {
    Ast(Vec<(Statement, Range<usize>)>),
    CstTree(SyntaxTree, Arc<String>),
}

pub enum SyntaxView<'a> {
    Ast(&'a [(Statement, Range<usize>)]),
    CstTree(TreeNodeView<'a>),
}

/// Can be expanded to include LSP lossless rowan tree
pub enum SyntaxSource<'a> {
    TextAst(&'a str),
    TextCst(Arc<String>),
}

impl<'a> SyntaxSource<'a> {
    pub fn parse(&self) -> Syntax {
        match self {
            Self::TextAst(source) => {
                let (statements, errors) = crate::parse_statements(source);
                Syntax {
                    kind: SyntaxKind::Ast(statements),
                    errors,
                }
            }
            Self::TextCst(source) => {
                let (flat_tree, errors) = crate::cst_parse(source);
                // println!("{}", flat_tree.debug_tree(&source));
                Syntax {
                    kind: SyntaxKind::CstTree(flat_tree.unflatten(), source.clone()),
                    errors,
                }
            }
        }
    }
}

impl SyntaxKind {
    pub fn view(&self) -> SyntaxView {
        match self {
            Self::Ast(statements) => SyntaxView::Ast(statements),
            Self::CstTree(tree, src) => SyntaxView::CstTree(tree.view(src)),
        }
    }
}
