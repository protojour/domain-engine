use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use ontol_runtime::{
    ontology::{
        domain::{DataRelationshipTarget, Def, DefRepr},
        Ontology,
    },
    property::PropertyCardinality,
    PropId,
};
use serde::{Deserialize, Serialize};

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
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
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
