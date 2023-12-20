use serde::de::Visitor;

use crate::{
    value::{Attribute, Value},
    DefId,
};

use super::{
    operator::SerdeOperatorAddr,
    processor::{RecursionLimitError, SerdeProcessor, SubProcessorContext},
};

pub struct GraphqlPatchVisitor<'on, 'p> {
    pub entity_sequence_processor: SerdeProcessor<'on, 'p>,
    pub inner_addr: SerdeOperatorAddr,
    pub type_def_id: DefId,
    pub ctx: SubProcessorContext,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Create,
    Update,
    // Delete,
}

impl<'on, 'p, 'de> Visitor<'de> for GraphqlPatchVisitor<'on, 'p> {
    type Value = Attribute;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "patch structure")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut patches = vec![];

        while let Some(operation) = map.next_key::<Operation>()? {
            let sequence_attr = map.next_value_seed(
                self.entity_sequence_processor.with_level(
                    self.entity_sequence_processor
                        .level
                        .local_child()
                        .map_err(RecursionLimitError::to_de_error)?,
                ),
            )?;
            let Value::Sequence(mut seq, _def_id) = sequence_attr.value else {
                return Err(<A::Error as serde::de::Error>::custom("Not a sequence"));
            };

            match operation {
                Operation::Create => {
                    patches.extend(seq.attrs);
                }
                Operation::Update => {
                    for attr in seq.attrs.iter_mut() {
                        match &mut attr.value {
                            Value::Struct(attrs, def_id) => {
                                let attrs = std::mem::take(attrs);
                                attr.value = Value::StructUpdate(attrs, *def_id);
                            }
                            _ => {}
                        }
                    }
                    patches.extend(seq.attrs);
                }
            }
        }

        Ok(Attribute {
            value: Value::Patch(patches, self.type_def_id),
            rel_params: Value::unit(),
        })
    }
}
