use fnv::FnvHashMap;
use indexmap::IndexMap;
use juniper::LookAheadMethods;
use ontol_runtime::DefId;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    virtual_schema::{
        data::{FieldData, NodeData, ObjectData, ObjectKind, TypeData, TypeKind, UnitTypeRef},
        VirtualSchema,
    },
};

/// TODO: An "official" selection data type for domain-engine
/// TODO: Must index by PropertyId, not string
#[derive(Debug)]
pub enum NaiveSelection {
    Node(IndexMap<String, NaiveSelection>),
    Union(FnvHashMap<DefId, IndexMap<String, NaiveSelection>>),
    Scalar,
}

pub fn analyze_selection(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    type_data: &TypeData,
    virtual_schema: &VirtualSchema,
) -> NaiveSelection {
    match &type_data.kind {
        TypeKind::Object(object_data) => {
            let mut map: IndexMap<String, NaiveSelection> = IndexMap::new();

            for field_look_ahead in look_ahead.children() {
                let field_name = field_look_ahead.field_name();

                map.insert(
                    field_name.into(),
                    analyze_field(field_look_ahead, &object_data.fields, virtual_schema),
                );
            }

            NaiveSelection::Node(map)
        }
        TypeKind::Union(union_data) => {
            let mut union_map: FnvHashMap<DefId, IndexMap<String, NaiveSelection>> =
                FnvHashMap::default();

            for field_look_ahead in look_ahead.children() {
                let field_name = field_look_ahead.field_name();

                let applies_for = field_look_ahead.applies_for();

                debug!("union field `{field_name}` applies for: {applies_for:?}",);

                if let Some(applies_for) = applies_for {
                    let variant_type_data = union_data
                        .variants
                        .iter()
                        .find_map(|type_index| {
                            let type_data = virtual_schema.type_data(*type_index);

                            if type_data.typename == applies_for {
                                Some(type_data)
                            } else {
                                None
                            }
                        })
                        .expect("BUG: Union variant not found");

                    let (fields, def_id) = match &variant_type_data.kind {
                        TypeKind::Object(ObjectData {
                            kind: ObjectKind::Node(NodeData { def_id, .. }),
                            fields,
                        }) => (fields, *def_id),
                        _ => panic!(
                            "Unable to extract def_id from union variant: Not an Object/Node"
                        ),
                    };

                    let variant_map = union_map.entry(def_id).or_default();
                    variant_map.insert(
                        field_name.into(),
                        analyze_field(field_look_ahead, fields, virtual_schema),
                    );
                }
            }

            NaiveSelection::Union(union_map)
        }
        TypeKind::CustomScalar(_) => {
            assert!(look_ahead.children().is_empty());
            NaiveSelection::Scalar
        }
    }
}

fn analyze_field(
    field_look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    field_map: &IndexMap<String, FieldData>,
    virtual_schema: &VirtualSchema,
) -> NaiveSelection {
    let field_name = field_look_ahead.field_name();
    let field_data = field_map.get(field_name).expect("Field not found");

    match field_data.field_type.unit {
        UnitTypeRef::Indexed(type_index) => {
            let selection = analyze_selection(
                field_look_ahead,
                virtual_schema.type_data(type_index),
                virtual_schema,
            );

            debug!("{field_name} selection: {selection:?}");

            selection
        }
        UnitTypeRef::ID(_) => NaiveSelection::Scalar,
        UnitTypeRef::NativeScalar(_) => NaiveSelection::Scalar,
    }
}
