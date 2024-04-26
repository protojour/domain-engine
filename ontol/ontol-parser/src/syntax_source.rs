//! Abstracted source of ONTOL syntax

use std::ops::Range;

use crate::{
    ast::Statement,
    cst::tree::{SyntaxTree, TreeNodeView},
    Error, TEST_CST,
};

/// Can be expanded to include LSP lossless rowan tree
pub enum SyntaxSource {
    TextAst(String),
    TextCst(String),
}

pub enum SyntaxImpl {
    Ast(Vec<(Statement, Range<usize>)>, Vec<Error>),
    CstTree(SyntaxTree, Vec<Error>),
}

pub enum SyntaxView<'a> {
    Ast(&'a [(Statement, Range<usize>)]),
    CstTree(TreeNodeView<'a>),
}

impl SyntaxSource {
    pub fn parse(&self) -> SyntaxImpl {
        match self {
            Self::TextAst(source) => {
                let (statements, errors) = crate::parse_statements(source);
                SyntaxImpl::Ast(statements, errors)
            }
            Self::TextCst(source) => {
                let (flat_tree, errors) = crate::cst_parse(source);
                SyntaxImpl::CstTree(flat_tree.unflatten(), errors)
            }
        }
    }
}

impl From<String> for SyntaxSource {
    fn from(value: String) -> Self {
        if TEST_CST {
            Self::TextCst(value)
        } else {
            Self::TextAst(value)
        }
    }
}

impl SyntaxImpl {
    pub fn view<'a>(&'a self, source: &'a SyntaxSource) -> (SyntaxView<'a>, &'a [Error]) {
        match (self, source) {
            (Self::Ast(statements, errors), _) => (SyntaxView::Ast(statements), errors),
            (
                Self::CstTree(tree, errors),
                SyntaxSource::TextAst(src) | SyntaxSource::TextCst(src),
            ) => (SyntaxView::CstTree(tree.view(src)), errors),
        }
    }
}
