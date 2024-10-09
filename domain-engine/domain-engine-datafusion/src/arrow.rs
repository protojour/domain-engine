//! ONTOL Arrow encoding

use std::sync::Arc;

use datafusion::arrow::{
    array::{
        ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, PrimitiveBuilder,
        RecordBatch, RecordBatchOptions, StringBuilder,
    },
    datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType},
};
use domain_engine_core::transact::RespMessage;
use ontol_runtime::{
    attr::Attr,
    interface::serde::operator::SerdeOperator,
    ontology::{
        domain::{DataRelationshipTarget, Def, DefRepr},
        ontol::text_pattern::FormatPattern,
        Ontology,
    },
    property::PropertyCardinality,
    value::{FormatValueAsText, Value},
    PropId,
};
use tracing::error;

pub fn mk_arrow_schema(def: &Def, ontology: &Ontology) -> Schema {
    let fields = iter_arrow_fields(def, ontology)
        .map(|field_info| {
            Field::new(
                field_info.name.to_string(),
                field_info.field_type.as_arrow(),
                field_info.nullable,
            )
        })
        .collect::<Vec<_>>();

    Schema {
        fields: Fields::from_iter(fields),
        metadata: Default::default(),
    }
}

#[derive(Debug)]
pub struct ArrowFieldInfo<'o> {
    pub prop_id: PropId,
    pub name: &'o str,
    pub field_type: FieldType,
    pub nullable: bool,
}

/// field type that's the intersection of what ONTOL and Arrow supports
#[derive(Clone, Copy, Debug)]
pub enum FieldType {
    Boolean,
    I64,
    F64,
    Text,
    Binary,
    Timestamp,
}

impl FieldType {
    fn as_arrow(&self) -> DataType {
        match self {
            FieldType::Boolean => DataType::Boolean,
            FieldType::I64 => DataType::Int64,
            FieldType::F64 => DataType::Float64,
            FieldType::Text => DataType::Utf8,
            FieldType::Binary => DataType::Binary,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }
}

pub fn iter_arrow_fields<'o>(
    def: &'o Def,
    ontology: &'o Ontology,
) -> impl Iterator<Item = ArrowFieldInfo<'o>> {
    def.data_relationships
        .iter()
        .filter_map(|(prop_id, rel_info)| match &rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                let repr = ontology.def(*def_id).repr()?;
                let field_type = match repr {
                    DefRepr::Unit => return None,
                    DefRepr::I64 => FieldType::I64,
                    DefRepr::F64 => FieldType::F64,
                    DefRepr::Serial => FieldType::I64,
                    DefRepr::Boolean => FieldType::Boolean,
                    DefRepr::Text => FieldType::Text,
                    DefRepr::TextConstant(_) => FieldType::Text,
                    DefRepr::Octets => FieldType::Binary,
                    DefRepr::DateTime => FieldType::Timestamp,
                    DefRepr::Seq => return None,
                    DefRepr::Struct => return None,
                    DefRepr::Intersection(_) => return None,
                    DefRepr::Union(_, _) => return None,
                    DefRepr::FmtStruct(_) => FieldType::Text,
                    DefRepr::Macro => return None,
                    DefRepr::Vertex => return None,
                    DefRepr::Unknown => return None,
                };

                Some(ArrowFieldInfo {
                    prop_id: *prop_id,
                    name: &ontology[rel_info.name],
                    field_type,
                    nullable: matches!(rel_info.cardinality.0, PropertyCardinality::Optional),
                })
            }
            DataRelationshipTarget::Union(_def_id) => None,
        })
}

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
                    Some(Attr::Unit(Value::Text(t, _))) => {
                        b.append_value(t);
                    }
                    Some(Attr::Unit(v)) => {
                        let text = match self
                            .ontology
                            .def(v.type_def_id())
                            .operator_addr
                            .map(|addr| &self.ontology[addr])
                        {
                            Some(SerdeOperator::CapturingTextPattern(pattern_def_id)) => {
                                let pattern =
                                    &self.ontology.get_text_pattern(*pattern_def_id).unwrap();
                                format!(
                                    "{}",
                                    FormatPattern {
                                        pattern,
                                        value: &v,
                                        ontology: &self.ontology
                                    }
                                )
                            }
                            _ => {
                                format!(
                                    "{}",
                                    FormatValueAsText {
                                        value: &v,
                                        type_def_id: v.type_def_id(),
                                        ontology: &self.ontology,
                                    }
                                )
                            }
                        };

                        b.append_value(text);
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

        let columns = self
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
