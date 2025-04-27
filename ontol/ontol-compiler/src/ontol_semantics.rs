use std::panic::UnwindSafe;

use ontol_core::{tag::DomainIndex, url::DomainUrl};
use ontol_log::compile_entrypoint::SemModelCompileEntrypoint;
use ontol_parser::{basic_syntax::OntolTreeSyntax, source::SourceId, topology::ExtractHeaderData};
use ontol_runtime::DefId;

use crate::{
    Session, log_lowering::LogLowering, lower_ontol_syntax, lowering::context::LoweringOutcome,
};

/// A generalization of an ONTOL source file.
///
/// It's whatever that can produce syntax nodes to fulfill
/// the methods of the trait.
pub trait OntolSemantics: ExtractHeaderData {
    fn lower(
        &self,
        url: DomainUrl,
        pkg_def_id: DefId,
        source_id: SourceId,
        domain_index: DomainIndex,
        session: Session,
    ) -> LoweringOutcome;
}

impl<S: AsRef<str> + UnwindSafe> OntolSemantics for OntolTreeSyntax<S> {
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

impl OntolSemantics for SemModelCompileEntrypoint {
    fn lower(
        &self,
        url: DomainUrl,
        domain_def_id: DefId,
        source_id: SourceId,
        domain_index: DomainIndex,
        session: Session,
    ) -> LoweringOutcome {
        let model = self.model.lock().unwrap();

        let mut lowering = LogLowering::new(
            url,
            domain_def_id,
            source_id,
            domain_index,
            self.local_log,
            self.subdomain,
            &model,
            session.0,
        );
        lowering.lower();
        lowering.into_outcome()
    }
}
