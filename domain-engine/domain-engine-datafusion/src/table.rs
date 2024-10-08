use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, Int64Builder, RecordBatch, RecordBatchOptions, StringBuilder,
        },
        datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit},
    },
    prelude::Expr,
};
use domain_engine_core::{domain_select, transact::RespMessage};
use ontol_runtime::{
    attr::Attr,
    interface::serde::operator::SerdeOperator,
    ontology::{
        domain::{DataRelationshipTarget, Def},
        ontol::text_pattern::FormatPattern,
        Ontology,
    },
    property::PropertyCardinality,
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    value::{FormatValueAsText, Value},
    DefId, PropId,
};

pub fn mk_datafusion_schema(def: &Def, ontology: &Ontology) -> Schema {
    let fields = iter_fields(def, ontology)
        .map(|field_info| {
            Field::new(
                field_info.name.to_string(),
                field_info.data_type,
                field_info.nullable,
            )
        })
        .collect::<Vec<_>>();

    Schema {
        fields: Fields::from_iter(fields),
        metadata: Default::default(),
    }
}

struct FieldInfo<'o> {
    prop_id: PropId,
    name: &'o str,
    data_type: DataType,
    nullable: bool,
}

fn iter_fields<'o>(def: &'o Def, ontology: &'o Ontology) -> impl Iterator<Item = FieldInfo<'o>> {
    def.data_relationships
        .iter()
        .filter_map(|(prop_id, rel_info)| match &rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                let repr = ontology.def(*def_id).repr()?;
                let data_type = match repr {
                    ontol_runtime::ontology::domain::DefRepr::Unit => return None,
                    ontol_runtime::ontology::domain::DefRepr::I64 => DataType::Int64,
                    ontol_runtime::ontology::domain::DefRepr::F64 => DataType::Float64,
                    ontol_runtime::ontology::domain::DefRepr::Serial => DataType::Int64,
                    ontol_runtime::ontology::domain::DefRepr::Boolean => DataType::Boolean,
                    ontol_runtime::ontology::domain::DefRepr::Text => DataType::Utf8,
                    ontol_runtime::ontology::domain::DefRepr::TextConstant(_) => DataType::Utf8,
                    ontol_runtime::ontology::domain::DefRepr::Octets => DataType::Binary,
                    ontol_runtime::ontology::domain::DefRepr::DateTime => {
                        DataType::Timestamp(TimeUnit::Microsecond, None)
                    }
                    ontol_runtime::ontology::domain::DefRepr::Seq => return None,
                    ontol_runtime::ontology::domain::DefRepr::Struct => return None,
                    ontol_runtime::ontology::domain::DefRepr::Intersection(_) => return None,
                    ontol_runtime::ontology::domain::DefRepr::Union(_, _) => return None,
                    ontol_runtime::ontology::domain::DefRepr::FmtStruct(_) => DataType::Utf8,
                    ontol_runtime::ontology::domain::DefRepr::Macro => return None,
                    ontol_runtime::ontology::domain::DefRepr::Vertex => return None,
                    ontol_runtime::ontology::domain::DefRepr::Unknown => return None,
                };

                Some(FieldInfo {
                    prop_id: *prop_id,
                    name: &ontology[rel_info.name],
                    data_type,
                    nullable: matches!(rel_info.cardinality.0, PropertyCardinality::Optional),
                })
            }
            DataRelationshipTarget::Union(_def_id) => None,
        })
}

#[derive(Clone)]
pub struct DatafusionFilter {
    entity_select: EntitySelect,
    column_selection: Vec<(PropId, DataType)>,
}

impl DatafusionFilter {
    pub fn compile(
        def_id: DefId,
        (projection, _filters, limit): (Option<&Vec<usize>>, &[Expr], Option<usize>),
        _df_schema: &Schema,
        ontology: &Ontology,
    ) -> Self {
        let def = ontology.def(def_id);
        let mut select_properties: BTreeMap<PropId, Select> = Default::default();
        let mut columns = vec![];

        let fields: Vec<_> = iter_fields(def, ontology).collect();

        if let Some(projection) = projection {
            for field_idx in projection {
                let Some(field_info) = fields.get(*field_idx) else {
                    continue;
                };

                select_properties.insert(field_info.prop_id, Select::Unit);
                columns.push((field_info.prop_id, field_info.data_type.clone()));
            }
        } else {
            if let Select::Struct(struct_select) =
                domain_select::domain_select_no_edges(def_id, ontology)
            {
                select_properties = struct_select.properties;
            }

            columns = fields
                .iter()
                .map(|field_info| (field_info.prop_id, field_info.data_type.clone()))
                .collect();
        }

        DatafusionFilter {
            entity_select: EntitySelect {
                source: StructOrUnionSelect::Struct(StructSelect {
                    def_id,
                    properties: select_properties,
                }),
                filter: Filter::default_for_domain(),
                limit,
                after_cursor: None,
                include_total_len: false,
            },
            column_selection: columns,
        }
    }

    pub fn entity_select(&self) -> EntitySelect {
        self.entity_select.clone()
    }

    pub fn column_selection(&self) -> Vec<(PropId, DataType)> {
        self.column_selection.clone()
    }
}

pub struct RecordBatchBuilder {
    df_schema: SchemaRef,
    column_selection: Vec<(PropId, DataType)>,
    batch_size: usize,
    row_count: usize,
    builders: Vec<DynBuilder>,
    ontology: Arc<Ontology>,
}

enum DynBuilder {
    Bool(BooleanBuilder),
    I64(Int64Builder),
    String(StringBuilder),
}

impl RecordBatchBuilder {
    pub fn new(
        df_schema: SchemaRef,
        column_selection: Vec<(PropId, DataType)>,
        ontology: Arc<Ontology>,
    ) -> Self {
        let mut builders = Default::default();
        init_builders(&mut builders, &column_selection);

        Self {
            df_schema,
            column_selection,
            batch_size: 128,
            row_count: 0,
            builders,
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
                DynBuilder::String(b) => match attr {
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
            }
        }
        self.row_count += 1;

        if self.row_count == self.batch_size {
            self.produce_batch()
        } else {
            None
        }
    }

    pub fn produce_batch(&mut self) -> Option<RecordBatch> {
        let row_count = self.row_count;
        self.row_count = 0;
        let builders = std::mem::take(&mut self.builders);
        init_builders(&mut self.builders, &self.column_selection);

        let mut columns: Vec<ArrayRef> = vec![];

        for builder in builders {
            let array_ref: ArrayRef = match builder {
                DynBuilder::Bool(mut b) => Arc::new(b.finish()),
                DynBuilder::I64(mut b) => Arc::new(b.finish()),
                DynBuilder::String(mut b) => Arc::new(b.finish()),
            };
            columns.push(array_ref);
        }

        match RecordBatch::try_new_with_options(
            self.df_schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        ) {
            Ok(batch) => Some(batch),
            Err(_) => None,
        }
    }
}

fn init_builders(builders: &mut Vec<DynBuilder>, selection: &[(PropId, DataType)]) {
    for (_prop_id, data_type) in selection {
        let builder = match data_type {
            DataType::Boolean => DynBuilder::Bool(Default::default()),
            DataType::Int64 => DynBuilder::I64(Default::default()),
            DataType::Utf8 => DynBuilder::String(Default::default()),
            dt => panic!("unhandled data type: {dt:?}"),
        };

        builders.push(builder);
    }
}
