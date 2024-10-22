use ontol_runtime::{attr::Attr, ontology::aspects::DefsAspect, value::Value};

use crate::DomainResult;

pub struct MakeInterfacable<'e> {
    pub(crate) defs: &'e DefsAspect,
}

impl<'e> MakeInterfacable<'e> {
    pub fn new(ontology: &'e impl AsRef<DefsAspect>) -> Self {
        Self {
            defs: ontology.as_ref(),
        }
    }

    pub fn make_interfacable(&self, value: &mut Value) -> DomainResult<()> {
        match value {
            value @ Value::CrdtStruct(..) => {
                let Value::CrdtStruct(crdt_struct, tag) = std::mem::replace(value, Value::unit())
                else {
                    unreachable!();
                };
                *value = self.automerge_to_ontol(&crdt_struct.0, tag.into())?;
            }
            Value::Struct(attrs, _) => {
                for attr in attrs.values_mut() {
                    match attr {
                        Attr::Unit(value) => self.make_interfacable(value)?,
                        Attr::Tuple(tuple) => {
                            for value in &mut tuple.elements {
                                self.make_interfacable(value)?;
                            }
                        }
                        Attr::Matrix(matrix) => {
                            for column in &mut matrix.columns {
                                for value in column.elements_mut() {
                                    self.make_interfacable(value)?;
                                }
                            }
                        }
                    }
                }
            }
            Value::Sequence(seq, _) => {
                for value in seq.elements_mut() {
                    self.make_interfacable(value)?;
                }
            }
            Value::Dict(dict, _) => {
                for value in dict.values_mut() {
                    self.make_interfacable(value)?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}
