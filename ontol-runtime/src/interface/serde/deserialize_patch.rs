use serde::de::Visitor;

use crate::{
    value::{Attribute, Value},
    DefId,
};

use super::{
    operator::SerdeOperatorAddr,
    processor::{ProcessorMode, RecursionLimitError, SerdeProcessor, SubProcessorContext},
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
    Add,
    Update,
    Remove,
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
            match operation {
                Operation::Add | Operation::Update => {
                    let sub_processor = self.entity_sequence_processor.with_level(
                        self.entity_sequence_processor
                            .level
                            // prevent recursing into GraphqlPatchVisitor again:
                            .local_child()
                            .map_err(RecursionLimitError::to_de_error)?,
                    );

                    let Value::Sequence(mut seq, _def_id) =
                        map.next_value_seed(sub_processor)?.value
                    else {
                        return Err(<A::Error as serde::de::Error>::custom("Not a sequence"));
                    };

                    if matches!(operation, Operation::Update) {
                        // Transform into StructUpdate values:
                        for attr in seq.attrs.iter_mut() {
                            if let Value::Struct(attrs, def_id) = &mut attr.value {
                                let attrs = std::mem::take(attrs);
                                attr.value = Value::StructUpdate(attrs, *def_id);
                            }
                        }
                    }

                    patches.extend(seq.attrs);
                }
                Operation::Remove => {
                    let mut sub_processor = self.entity_sequence_processor.with_level(
                        self.entity_sequence_processor
                            .level
                            // prevent recursing into GraphqlPatchVisitor again:
                            .local_child()
                            .map_err(RecursionLimitError::to_de_error)?,
                    );
                    // Go to id-only mode
                    sub_processor.mode = ProcessorMode::Delete;

                    let Value::Sequence(mut seq, _def_id) =
                        map.next_value_seed(sub_processor)?.value
                    else {
                        return Err(<A::Error as serde::de::Error>::custom("Not a sequence"));
                    };

                    for attr in seq.attrs.iter_mut() {
                        attr.rel_params = Value::DeleteRelationship(DefId::unit());
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
