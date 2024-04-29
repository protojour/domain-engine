//! Abstracted source of ONTOL syntax

use std::{rc::Rc, sync::Arc};

use crate::{
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
    CstTreeRc(SyntaxTree, Rc<String>),
    CstTreeArc(SyntaxTree, Arc<String>),
}

pub enum SyntaxView<'a> {
    CstTree(TreeNodeView<'a>),
}

/// Can be expanded to include LSP lossless rowan tree
pub enum SyntaxSource {
    TextCstRc(Rc<String>),
    TextCstArc(Arc<String>),
}

impl SyntaxSource {
    pub fn parse(&self) -> Syntax {
        match self {
            Self::TextCstRc(source) => {
                let (flat_tree, errors) = crate::cst_parse(source);
                // println!("{}", flat_tree.debug_tree(&source));
                Syntax {
                    kind: SyntaxKind::CstTreeRc(flat_tree.unflatten(), source.clone()),
                    errors,
                }
            }
            Self::TextCstArc(source) => {
                let (flat_tree, errors) = crate::cst_parse(source);
                // println!("{}", flat_tree.debug_tree(&source));
                Syntax {
                    kind: SyntaxKind::CstTreeArc(flat_tree.unflatten(), source.clone()),
                    errors,
                }
            }
        }
    }
}

impl SyntaxKind {
    pub fn view(&self) -> SyntaxView {
        match self {
            Self::CstTreeRc(tree, src) => SyntaxView::CstTree(tree.view(src)),
            Self::CstTreeArc(tree, src) => SyntaxView::CstTree(tree.view(src)),
        }
    }
}
