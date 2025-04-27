use std::sync::{Arc, Mutex};

use ontol_core::LogRef;
use ontol_parser::topology::{ExtractHeaderData, OntolHeaderData};

use crate::{sem_model::GlobalSemModel, tag::Tag};

/// This uses a mutex because the semantic model
/// generally needs to change after an entrypoint has been constructed.
pub struct SemModelCompileEntrypoint {
    pub model: Arc<Mutex<GlobalSemModel>>,
    pub local_log: LogRef,
    pub subdomain: Tag,
}

impl ExtractHeaderData for SemModelCompileEntrypoint {
    fn header_data(
        &self,
        with_docs: ontol_parser::topology::WithDocs,
        errors: &mut Vec<impl ontol_parser::topology::MakeParseError>,
    ) -> OntolHeaderData {
        self.model.lock().unwrap().subdomain_header_data(
            self.local_log,
            self.subdomain,
            with_docs,
            errors,
        )
    }
}
