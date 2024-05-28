use std::{borrow::Borrow, panic::UnwindSafe};

use ontol_parser::{cst::tree::SyntaxTree, U32Span};
use ontol_runtime::DefId;

use crate::{
    lower_ontol_syntax,
    package::{extract_ontol_dependentices, PackageReference},
    Session, Src,
};

/// A generalization of an ONTOL source file.
///
/// It's whatever that can produce syntax nodes to fulfill
/// the methods of the trait.
pub trait OntolSyntax: UnwindSafe {
    fn dependencies(&self) -> Vec<(PackageReference, U32Span)>;
    fn lower(&self, pkg_def_id: DefId, src: Src, session: Session) -> Vec<DefId>;
}

/// An ontol_parser native syntax tree.
pub struct OntolTreeSyntax<S> {
    pub tree: SyntaxTree,
    pub source_text: S,
}

impl<S: Borrow<String> + UnwindSafe> OntolSyntax for OntolTreeSyntax<S> {
    fn dependencies(&self) -> Vec<(PackageReference, U32Span)> {
        extract_ontol_dependentices(self.tree.view(self.source_text.borrow()))
    }

    fn lower(&self, pkg_def_id: DefId, src: Src, session: Session) -> Vec<DefId> {
        lower_ontol_syntax(
            self.tree.view(self.source_text.borrow()),
            pkg_def_id,
            src,
            session,
        )
    }
}
