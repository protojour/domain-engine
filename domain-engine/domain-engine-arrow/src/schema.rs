use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use ontol_runtime::{
    DefId, PropId,
    ontology::{
        aspects::DefsAspect,
        domain::{DataRelationshipTarget, Def, DefRepr},
    },
    property::{PropertyCardinality, ValueCardinality},
};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct ArrowFieldInfo<'o> {
    pub prop_id: PropId,
    pub name: &'o str,
    pub field_def_id: DefId,
    pub field_type: FieldType,
    pub nullable: bool,
}

/// field type that's the intersection of what ONTOL and Arrow supports
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum FieldType {
    Boolean,
    I64,
    F64,
    Text,
    Binary,
    Timestamp,
    Struct(StructFieldType),
    /// Empty struct is needed for self-recursive ontol structures
    EmptyStruct,
    List(Box<FieldType>),
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct StructFieldType(pub DefId);

pub struct ArrowSchemaBuilder<'o> {
    ontology_defs: &'o DefsAspect,
    def_stack: Vec<DefId>,
}

impl<'o> ArrowSchemaBuilder<'o> {
    pub fn new(ontology_defs: &'o DefsAspect) -> Self {
        Self {
            ontology_defs,
            def_stack: vec![],
        }
    }

    pub fn mk_schema(&mut self, def: &Def) -> Schema {
        Schema {
            fields: Fields::from_iter(self.mk_field_list(def)),
            metadata: Default::default(),
        }
    }

    pub fn mk_field_list(&mut self, def: &Def) -> Vec<Field> {
        let def_stack = self.def_stack.clone();

        iter_arrow_fields(def, self.ontology_defs, def_stack)
            .map(|field_info| {
                self.def_stack.push(def.id);
                let field = self.mk_field(&field_info);
                self.def_stack.pop();
                field
            })
            .collect::<Vec<_>>()
    }

    fn mk_field(&mut self, field_info: &ArrowFieldInfo) -> Field {
        Field::new(
            field_info.name.to_string(),
            self.mk_data_type(&field_info.field_type),
            field_info.nullable,
        )
    }

    fn mk_data_type(&mut self, field_type: &FieldType) -> DataType {
        match field_type {
            FieldType::Boolean => DataType::Boolean,
            FieldType::I64 => DataType::Int64,
            FieldType::F64 => DataType::Float64,
            FieldType::Text => DataType::Utf8,
            FieldType::Binary => DataType::Binary,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            FieldType::Struct(ft) => {
                let def_id = ft.0;
                let def = self.ontology_defs.def(def_id);
                let fields = self.mk_field_list(def);

                DataType::Struct(Fields::from_iter(fields))
            }
            FieldType::EmptyStruct => DataType::Struct(Fields::empty()),
            FieldType::List(item_type) => {
                let item_data_type = self.mk_data_type(item_type);

                // setting nullable to true regardless, if not the builder will complain,
                // maybe due to bugs
                let nullable = true;

                DataType::List(Arc::new(Field::new("item", item_data_type, nullable)))
            }
        }
    }
}

pub fn iter_arrow_fields<'o>(
    def: &'o Def,
    ontology_defs: &'o DefsAspect,
    def_stack: Vec<DefId>,
) -> impl Iterator<Item = ArrowFieldInfo<'o>> {
    def.data_relationships
        .iter()
        .filter_map(move |(prop_id, rel_info)| match &rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                let repr = ontology_defs.def(*def_id).repr()?;
                let mut field_type = match repr {
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
                    DefRepr::Struct => {
                        if def_stack.contains(def_id) {
                            FieldType::EmptyStruct
                        } else {
                            FieldType::Struct(StructFieldType(*def_id))
                        }
                    }
                    DefRepr::Intersection(_) => return None,
                    DefRepr::Union(_, _) => return None,
                    DefRepr::FmtStruct(_) => FieldType::Text,
                    DefRepr::Macro => return None,
                    DefRepr::Vertex => return None,
                    DefRepr::Unknown => return None,
                };

                if matches!(
                    rel_info.cardinality.1,
                    ValueCardinality::IndexSet | ValueCardinality::List
                ) {
                    field_type = FieldType::List(Box::new(field_type));
                }

                Some(ArrowFieldInfo {
                    prop_id: *prop_id,
                    name: &ontology_defs[rel_info.name],
                    field_def_id: *def_id,
                    field_type,
                    nullable: matches!(rel_info.cardinality.0, PropertyCardinality::Optional),
                })
            }
            DataRelationshipTarget::Union(_def_id) => None,
        })
}
