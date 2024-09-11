use std::{borrow::Borrow, panic::UnwindSafe, rc::Rc};

use ontol_parser::{cst::tree::SyntaxTree, U32Span};
use ontol_runtime::DefId;

use crate::{
    lower_ontol_syntax,
    lowering::context::LoweringOutcome,
    topology::{extract_ontol_dependencies, DomainReference, DomainUrl},
    Session, Src,
};

/// A generalization of an ONTOL source file.
///
/// It's whatever that can produce syntax nodes to fulfill
/// the methods of the trait.
pub trait OntolSyntax: UnwindSafe {
    fn dependencies(
        &self,
        errors: &mut Vec<ontol_parser::Error>,
    ) -> Vec<(DomainReference, U32Span)>;
    fn lower(
        &self,
        url: Rc<DomainUrl>,
        pkg_def_id: DefId,
        src: Src,
        session: Session,
    ) -> LoweringOutcome;
}

/// An ontol_parser native syntax tree.
pub struct OntolTreeSyntax<S> {
    pub tree: SyntaxTree,
    pub source_text: S,
}

impl<S: Borrow<String> + UnwindSafe> OntolSyntax for OntolTreeSyntax<S> {
    fn dependencies(
        &self,
        errors: &mut Vec<ontol_parser::Error>,
    ) -> Vec<(DomainReference, U32Span)> {
        extract_ontol_dependencies(self.tree.view(self.source_text.borrow()), errors)
    }

    fn lower(
        &self,
        url: Rc<DomainUrl>,
        domain_def_id: DefId,
        src: Src,
        session: Session,
    ) -> LoweringOutcome {
        lower_ontol_syntax(
            self.tree.view(self.source_text.borrow()),
            url,
            domain_def_id,
            src,
            session,
        )
    }
}
