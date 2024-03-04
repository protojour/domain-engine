use serde::{Deserialize, Serialize};

use crate::debug::OntolDebug;

/// An index into the [crate::ontology::Ontology] table of text constants.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct TextConstant(pub u32);

impl OntolDebug for TextConstant {
    fn fmt(
        &self,
        ontol_fmt: &dyn crate::debug::OntolFormatter,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        ontol_fmt.fmt_text_constant(*self, f)
    }
}
