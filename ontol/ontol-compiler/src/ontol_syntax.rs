use std::panic::UnwindSafe;

use ontol_core::{tag::DomainIndex, url::DomainUrl};
use ontol_parser::{basic_syntax::OntolTreeSyntax, source::SourceId, topology::ExtractHeaderData};
use ontol_runtime::DefId;

use crate::{Session, lower_ontol_syntax, lowering::context::LoweringOutcome};

/// A generalization of an ONTOL source file.
///
/// It's whatever that can produce syntax nodes to fulfill
/// the methods of the trait.
pub trait OntolSyntax: ExtractHeaderData {
    fn lower(
        &self,
        url: DomainUrl,
        pkg_def_id: DefId,
        source_id: SourceId,
        domain_index: DomainIndex,
        session: Session,
    ) -> LoweringOutcome;
}

impl<S: AsRef<str> + UnwindSafe> OntolSyntax for OntolTreeSyntax<S> {
    fn lower(
        &self,
        url: DomainUrl,
        domain_def_id: DefId,
        source_id: SourceId,
        domain_index: DomainIndex,
        session: Session,
    ) -> LoweringOutcome {
        lower_ontol_syntax(
            self.tree.view(self.source_text.as_ref()),
            url,
            domain_def_id,
            source_id,
            domain_index,
            session,
        )
    }
}
