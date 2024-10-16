use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use ontol_runtime::{
    ontology::{
        aspects::DefsAspect,
        domain::{DataRelationshipTarget, Def, DefRepr},
    },
    property::PropertyCardinality,
    DefId, PropId,
};
use serde::{Deserialize, Serialize};

pub fn mk_arrow_schema(def: &Def, ontology_defs: &DefsAspect) -> Schema {
    let fields = iter_arrow_fields(def, ontology_defs)
        .map(|info| info.to_arrow_field(ontology_defs))
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

impl<'o> ArrowFieldInfo<'o> {
    fn to_arrow_field(&self, ontology_defs: &DefsAspect) -> Field {
        Field::new(
            self.name.to_string(),
            self.field_type.as_arrow(ontology_defs),
            self.nullable,
        )
    }
}

/// field type that's the intersection of what ONTOL and Arrow supports
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub enum FieldType {
    Boolean,
    I64,
    F64,
    Text,
    Binary,
    Timestamp,
    Struct(StructFieldType),
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct StructFieldType(DefId);

impl FieldType {
    fn as_arrow(&self, ontology_defs: &DefsAspect) -> DataType {
        match self {
            FieldType::Boolean => DataType::Boolean,
            FieldType::I64 => DataType::Int64,
            FieldType::F64 => DataType::Float64,
            FieldType::Text => DataType::Utf8,
            FieldType::Binary => DataType::Binary,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            FieldType::Struct(ft) => DataType::Struct(ft.arrow_fields(ontology_defs)),
        }
    }
}

impl StructFieldType {
    pub fn arrow_fields(&self, ontology_defs: &DefsAspect) -> Fields {
        let def = ontology_defs.def(self.0);
        let fields = iter_arrow_fields(def, ontology_defs)
            .map(|info| Arc::new(info.to_arrow_field(ontology_defs)))
            .collect::<Vec<_>>();

        Fields::from_iter(fields)
    }
}

pub fn iter_arrow_fields<'o>(
    def: &'o Def,
    ontology_defs: &'o DefsAspect,
) -> impl Iterator<Item = ArrowFieldInfo<'o>> {
    def.data_relationships
        .iter()
        .filter_map(|(prop_id, rel_info)| match &rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                let repr = ontology_defs.def(*def_id).repr()?;
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
                    DefRepr::Struct => {
                        // FieldType::Struct(StructFieldType(*def_id))
                        return None;
                    }
                    DefRepr::Intersection(_) => return None,
                    DefRepr::Union(_, _) => return None,
                    DefRepr::FmtStruct(_) => FieldType::Text,
                    DefRepr::Macro => return None,
                    DefRepr::Vertex => return None,
                    DefRepr::Unknown => return None,
                };

                Some(ArrowFieldInfo {
                    prop_id: *prop_id,
                    name: &ontology_defs[rel_info.name],
                    field_type,
                    nullable: matches!(rel_info.cardinality.0, PropertyCardinality::Optional),
                })
            }
            DataRelationshipTarget::Union(_def_id) => None,
        })
}
