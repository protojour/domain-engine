use std::sync::Arc;

use arrow_array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder,
        PrimitiveBuilder, StringBuilder, StructBuilder,
    },
    types::TimestampMicrosecondType,
    RecordBatch, RecordBatchOptions,
};
use arrow_schema::SchemaRef;
use domain_engine_core::transact::RespMessage;
use ontol_runtime::{
    attr::Attr,
    format_utils::format_value,
    ontology::{aspects::DefsAspect, Ontology},
    value::Value,
    PropId,
};
use tracing::error;

use crate::schema::FieldType;

const INITIAL_CAPACITY: usize = 128;

pub struct RecordBatchBuilder {
    column_selection: Vec<(PropId, FieldType)>,
    max_batch_size: usize,
    /// row count for the current batch produced
    current_row_count: usize,
    builders: Vec<Box<dyn ArrayBuilder>>,
    arrow_schema: SchemaRef,
    ontology: Arc<Ontology>,
}

impl RecordBatchBuilder {
    pub fn new(
        column_selection: Vec<(PropId, FieldType)>,
        arrow_schema: SchemaRef,
        ontology: Arc<Ontology>,
        max_batch_size: usize,
    ) -> Self {
        let builders: Vec<_> = column_selection
            .iter()
            .map(|(_, field_type)| mk_column_builder(field_type, ontology.as_ref().as_ref()))
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

        for ((prop_id, field_type), builder) in self.column_selection.iter().zip(&mut self.builders)
        {
            let attr = attrs.remove(prop_id);

            match push_attr(attr, builder, field_type, &self.ontology) {
                Some(()) => {}
                None => error!("failed to encode arrow value"),
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
            .map(|dyn_builder| dyn_builder.finish())
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

fn mk_column_builder(field_type: &FieldType, ontology_defs: &DefsAspect) -> Box<dyn ArrayBuilder> {
    match field_type {
        FieldType::Boolean => Box::new(BooleanBuilder::default()),
        FieldType::I64 => Box::new(Int64Builder::default()),
        FieldType::F64 => Box::new(Float64Builder::default()),
        FieldType::Text => Box::new(StringBuilder::default()),
        FieldType::Binary => Box::new(BinaryBuilder::default()),
        FieldType::Timestamp => Box::new(PrimitiveBuilder::<TimestampMicrosecondType>::default()),
        FieldType::Struct(ft) => Box::new(StructBuilder::from_fields(
            ft.arrow_fields(ontology_defs),
            INITIAL_CAPACITY,
        )),
    }
}

fn push_attr(
    attr: Option<Attr>,
    builder: &mut dyn ArrayBuilder,
    field_type: &FieldType,
    ontology: &Ontology,
) -> Option<()> {
    let builder = builder.as_any_mut();

    match field_type {
        FieldType::Boolean => {
            let b = builder.downcast_mut::<BooleanBuilder>()?;
            if let Some(Attr::Unit(Value::I64(i, _))) = attr {
                b.append_value(i != 0);
            } else {
                b.append_null();
            }
        }
        FieldType::I64 => {
            let b = builder.downcast_mut::<Int64Builder>()?;
            if let Some(Attr::Unit(Value::I64(i, _))) = attr {
                b.append_value(i);
            } else {
                b.append_null();
            }
        }
        FieldType::F64 => {
            let b = builder.downcast_mut::<Float64Builder>()?;
            if let Some(Attr::Unit(Value::F64(f, _))) = attr {
                b.append_value(f);
            } else {
                b.append_null();
            }
        }
        FieldType::Text => {
            let b = builder.downcast_mut::<StringBuilder>()?;
            match attr {
                Some(Attr::Unit(value)) => b.append_value(format_value(&value, ontology)),
                _ => {
                    b.append_null();
                }
            }
        }
        FieldType::Binary => {
            let b = builder.downcast_mut::<BinaryBuilder>()?;
            if let Some(Attr::Unit(Value::OctetSequence(o, _))) = attr {
                b.append_value(&o.0);
            } else {
                b.append_null();
            }
        }
        FieldType::Timestamp => {
            let b = builder.downcast_mut::<PrimitiveBuilder<TimestampMicrosecondType>>()?;
            if let Some(Attr::Unit(Value::ChronoDateTime(dt, _))) = attr {
                b.append_value(dt.timestamp_micros());
            } else {
                b.append_null();
            }
        }
        FieldType::Struct(_) => {
            // let b = builder.downcast_mut::<StructBuilder>()?;
            todo!()
        }
    }

    Some(())
}
