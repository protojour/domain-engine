use serde::de::Visitor;
use smallvec::smallvec;

use crate::{
    tuple::{CardinalIdx, EndoTuple},
    value::{Attr, Value},
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
    type Value = Attr;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "patch structure")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut patches: Vec<Attr> = vec![];

        while let Some(operation) = map.next_key::<Operation>()? {
            match operation {
                Operation::Add | Operation::Update => {
                    let mut sub_processor = self.entity_sequence_processor.with_level(
                        self.entity_sequence_processor
                            .level
                            // prevent recursing into GraphqlPatchVisitor again:
                            .local_child()
                            .map_err(RecursionLimitError::to_de_error)?,
                    );

                    if matches!(operation, Operation::Update) {
                        sub_processor.ctx.is_update = true;
                    }

                    let Attr::Unit(Value::Sequence(seq, _def_id)) =
                        map.next_value_seed(sub_processor)?
                    else {
                        return Err(<A::Error as serde::de::Error>::custom("Not a sequence"));
                    };

                    patches.extend(seq.elements.into_iter().map(Attr::Unit));
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

                    let Attr::Unit(Value::Sequence(seq, _def_id)) =
                        map.next_value_seed(sub_processor)?
                    else {
                        return Err(<A::Error as serde::de::Error>::custom("Not a sequence"));
                    };

                    patches.extend(seq.elements.into_iter().map(|value| {
                        Attr::Tuple(Box::new(EndoTuple {
                            origin: CardinalIdx(0),
                            elements: smallvec![value, Value::DeleteRelationship(DefId::unit())],
                        }))
                    }));
                }
            }
        }

        Ok(Attr::Unit(Value::Patch(patches, self.type_def_id)))
    }
}
