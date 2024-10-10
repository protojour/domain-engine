use ::serde::{Deserialize, Serialize};

use super::aspects::{
    ConfigAspect, DefsAspect, DocumentationAspect, ExecutionAspect, InterfaceAspect, SerdeAspect,
};

/// All of the information that makes an Ontology.
#[derive(Serialize, Deserialize)]
pub struct Data {
    pub(crate) defs: DefsAspect,
    pub(crate) serde: SerdeAspect,
    pub(crate) documentation: DocumentationAspect,
    pub(crate) interface: InterfaceAspect,
    pub(crate) execution: ExecutionAspect,
    pub(crate) config: ConfigAspect,
}
