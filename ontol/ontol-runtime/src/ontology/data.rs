use ::serde::{Deserialize, Serialize};

use super::aspects::{
    ConfigAspect, DocumentationAspect, DomainAspect, InterfaceAspect, MappingAspect, SerdeAspect,
};

/// All of the information that makes an Ontology.
#[derive(Serialize, Deserialize)]
pub struct Data {
    pub(crate) domain: DomainAspect,
    pub(crate) serde: SerdeAspect,
    pub(crate) documentation: DocumentationAspect,
    pub(crate) interface: InterfaceAspect,
    pub(crate) mapping: MappingAspect,
    pub(crate) config: ConfigAspect,
}
