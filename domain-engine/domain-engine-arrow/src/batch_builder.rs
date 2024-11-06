use std::sync::Arc;

use arrow_array::{
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder,
        PrimitiveBuilder, StringBuilder, StructBuilder,
    },
    types::TimestampMicrosecondType,
    RecordBatch, RecordBatchOptions,
};
use arrow_schema::{Fields, SchemaRef};
use domain_engine_core::transact::RespMessage;
use ontol_runtime::{
    attr::Attr,
    format_utils::format_value,
    ontology::{aspects::DefsAspect, Ontology},
    value::Value,
    PropId,
};
use tracing::error;

use crate::schema::{iter_arrow_fields, ArrowSchemaBuilder, FieldType, StructFieldType};

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

            match push_attr(attr, IdentityDowncast(builder), field_type, &self.ontology) {
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
            ArrowSchemaBuilder::new(ontology_defs).mk_field_list(ontology_defs.def(ft.0)),
            INITIAL_CAPACITY,
        )),
        FieldType::EmptyStruct => Box::new(StructBuilder::from_fields(
            Fields::empty(),
            INITIAL_CAPACITY,
        )),
        FieldType::List(item_type) => {
            let item_builder = mk_column_builder(item_type, ontology_defs);
            Box::new(ListBuilder::new(item_builder))
        }
    }
}

trait DowncastArrayBuilder {
    fn downcast_builder<T: ArrayBuilder>(&mut self) -> Option<&mut T>;
}

struct IdentityDowncast<'a>(&'a mut dyn ArrayBuilder);

impl<'a> DowncastArrayBuilder for IdentityDowncast<'a> {
    fn downcast_builder<T: ArrayBuilder>(&mut self) -> Option<&mut T> {
        self.0.as_any_mut().downcast_mut::<T>()
    }
}

struct StructFieldDowncast<'a> {
    struct_builder: &'a mut StructBuilder,
    index: usize,
}

impl<'a> DowncastArrayBuilder for StructFieldDowncast<'a> {
    fn downcast_builder<T: ArrayBuilder>(&mut self) -> Option<&mut T> {
        self.struct_builder.field_builder::<T>(self.index)
    }
}

fn push_attr(
    attr: Option<Attr>,
    mut builder: impl DowncastArrayBuilder,
    field_type: &FieldType,
    ontology: &Ontology,
) -> Option<()> {
    match field_type {
        FieldType::Boolean => {
            let b = builder.downcast_builder::<BooleanBuilder>()?;
            if let Some(Attr::Unit(Value::I64(i, _))) = attr {
                b.append_value(i != 0);
            } else {
                b.append_null();
            }
        }
        FieldType::I64 => {
            let b = builder.downcast_builder::<Int64Builder>()?;
            if let Some(Attr::Unit(Value::I64(i, _))) = attr {
                b.append_value(i);
            } else {
                b.append_null();
            }
        }
        FieldType::F64 => {
            let b = builder.downcast_builder::<Float64Builder>()?;
            if let Some(Attr::Unit(Value::F64(f, _))) = attr {
                b.append_value(f);
            } else {
                b.append_null();
            }
        }
        FieldType::Text => {
            let b = builder.downcast_builder::<StringBuilder>()?;
            match attr {
                Some(Attr::Unit(value)) => match format_value(&value, ontology).as_str() {
                    Some(str) => b.append_value(str),
                    None => b.append_null(),
                },
                _ => {
                    b.append_null();
                }
            }
        }
        FieldType::Binary => {
            let b = builder.downcast_builder::<BinaryBuilder>()?;
            if let Some(Attr::Unit(Value::OctetSequence(o, _))) = attr {
                b.append_value(&o.0);
            } else {
                b.append_null();
            }
        }
        FieldType::Timestamp => {
            let b = builder.downcast_builder::<PrimitiveBuilder<TimestampMicrosecondType>>()?;
            if let Some(Attr::Unit(Value::ChronoDateTime(dt, _))) = attr {
                b.append_value(dt.timestamp_micros());
            } else {
                b.append_null();
            }
        }
        FieldType::Struct(StructFieldType(def_id)) => {
            let b = builder.downcast_builder::<StructBuilder>()?;
            let def = ontology.def(*def_id);

            if let Some(Attr::Unit(Value::Struct(mut st, _))) = attr {
                let mut valid = true;

                for (index, field_info) in
                    iter_arrow_fields(def, ontology.as_ref(), vec![]).enumerate()
                {
                    let attr = st.remove(&field_info.prop_id);

                    if push_attr(
                        attr,
                        StructFieldDowncast {
                            struct_builder: b,
                            index,
                        },
                        &field_info.field_type,
                        ontology,
                    )
                    .is_none()
                    {
                        valid = false;
                    }
                }

                b.append(valid);
            } else {
                b.append(false);
            }
        }
        FieldType::EmptyStruct => {
            let b = builder.downcast_builder::<StructBuilder>()?;
            b.append(true);
        }
        FieldType::List(item_type) => {
            let b = builder.downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>()?;

            if let Some(Attr::Matrix(matrix)) = attr {
                if let Some(first_column) = matrix.columns.into_iter().next() {
                    let mut valid = true;

                    for value in first_column.into_elements() {
                        if push_attr(
                            Some(Attr::Unit(value)),
                            IdentityDowncast(b.values()),
                            item_type,
                            ontology,
                        )
                        .is_none()
                        {
                            valid = false;
                        }
                    }

                    b.append(valid);
                } else {
                    b.append(false);
                }
            } else {
                b.append(false);
            }
        }
    }

    Some(())
}
