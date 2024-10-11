use std::sync::Arc;

use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, PrimitiveBuilder,
        StringBuilder,
    },
    types::TimestampMicrosecondType,
    ArrayRef, RecordBatch, RecordBatchOptions,
};
use arrow_schema::SchemaRef;
use domain_engine_core::transact::RespMessage;
use ontol_runtime::{
    attr::Attr, format_utils::format_value, ontology::Ontology, value::Value, PropId,
};
use tracing::error;

use crate::schema::FieldType;

pub struct RecordBatchBuilder {
    column_selection: Vec<(PropId, FieldType)>,
    max_batch_size: usize,
    /// row count for the current batch produced
    current_row_count: usize,
    builders: Vec<DynBuilder>,
    arrow_schema: SchemaRef,
    ontology: Arc<Ontology>,
}

enum DynBuilder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Text(StringBuilder),
    Binary(BinaryBuilder),
    Timestamp(PrimitiveBuilder<TimestampMicrosecondType>),
}

impl RecordBatchBuilder {
    pub fn new(
        column_selection: Vec<(PropId, FieldType)>,
        arrow_schema: SchemaRef,
        ontology: Arc<Ontology>,
        max_batch_size: usize,
    ) -> Self {
        let builders = column_selection
            .iter()
            .map(|(_, field_type)| match field_type {
                FieldType::Boolean => DynBuilder::Bool(Default::default()),
                FieldType::I64 => DynBuilder::I64(Default::default()),
                FieldType::F64 => DynBuilder::F64(Default::default()),
                FieldType::Text => DynBuilder::Text(Default::default()),
                FieldType::Binary => DynBuilder::Binary(Default::default()),
                FieldType::Timestamp => DynBuilder::Timestamp(Default::default()),
            })
            .collect();

        Self {
            column_selection,
            max_batch_size,
            current_row_count: 0,
            builders,
            arrow_schema,
            ontology,
        }
    }

    pub fn handle_msg(&mut self, msg: RespMessage) -> Option<RecordBatch> {
        let RespMessage::Element(value, _operation) = msg else {
            return None;
        };

        let Value::Struct(attrs, _) = value else {
            return None;
        };
        let mut attrs = *attrs;

        for ((prop_id, _data_type), builder) in self.column_selection.iter().zip(&mut self.builders)
        {
            let attr = attrs.remove(prop_id);

            match builder {
                DynBuilder::Text(b) => match attr {
                    Some(Attr::Unit(value)) => {
                        b.append_value(format_value(&value, self.ontology.as_ref()))
                    }
                    _ => {
                        b.append_null();
                    }
                },
                DynBuilder::Bool(b) => {
                    if let Some(Attr::Unit(Value::I64(i, _))) = attr {
                        b.append_value(i != 0);
                    } else {
                        b.append_null();
                    }
                }
                DynBuilder::I64(b) => {
                    if let Some(Attr::Unit(Value::I64(i, _))) = attr {
                        b.append_value(i);
                    } else {
                        b.append_null();
                    }
                }
                DynBuilder::F64(b) => {
                    if let Some(Attr::Unit(Value::F64(f, _))) = attr {
                        b.append_value(f);
                    } else {
                        b.append_null();
                    }
                }
                DynBuilder::Binary(b) => {
                    if let Some(Attr::Unit(Value::OctetSequence(o, _))) = attr {
                        b.append_value(&o.0);
                    } else {
                        b.append_null();
                    }
                }
                DynBuilder::Timestamp(b) => {
                    if let Some(Attr::Unit(Value::ChronoDateTime(dt, _))) = attr {
                        b.append_value(dt.timestamp_micros());
                    } else {
                        b.append_null();
                    }
                }
            }
        }
        self.current_row_count += 1;

        if self.current_row_count == self.max_batch_size {
            self.yield_batch()
        } else {
            None
        }
    }

    pub fn yield_batch(&mut self) -> Option<RecordBatch> {
        let row_count = self.current_row_count;
        self.current_row_count = 0;

        if row_count == 0 {
            return None;
        }

        let columns: Vec<_> = self
            .builders
            .iter_mut()
            .map(|dyn_builder| -> ArrayRef {
                match dyn_builder {
                    DynBuilder::Bool(b) => Arc::new(b.finish()),
                    DynBuilder::I64(b) => Arc::new(b.finish()),
                    DynBuilder::F64(b) => Arc::new(b.finish()),
                    DynBuilder::Text(b) => Arc::new(b.finish()),
                    DynBuilder::Binary(b) => Arc::new(b.finish()),
                    DynBuilder::Timestamp(b) => Arc::new(b.finish()),
                }
            })
            .collect();

        match RecordBatch::try_new_with_options(
            self.arrow_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        ) {
            Ok(batch) => Some(batch),
            Err(err) => {
                error!("error constructing arrow batch: {err:?}");
                None
            }
        }
    }
}
