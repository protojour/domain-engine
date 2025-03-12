use ontol_core::debug::{OntolDebug, OntolFormatter};
use serde::{Deserialize, Serialize};

/// An index into the [crate::ontology::Ontology] table of text constants.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
pub struct TextConstant(pub u32);

impl OntolDebug for TextConstant {
    fn fmt(
        &self,
        ontol_fmt: &dyn OntolFormatter,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        ontol_fmt.fmt_text_constant(self.0, f)
    }
}
