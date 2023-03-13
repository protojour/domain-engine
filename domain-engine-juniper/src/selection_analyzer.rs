use indexmap::IndexMap;
use juniper::LookAheadMethods;
use ontol_runtime::DefId;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    virtual_schema::{
        data::{NodeData, ObjectData, ObjectKind, TypeData, TypeKind, UnionData, UnitTypeRef},
        VirtualSchema,
    },
};

/// TODO: An "official" selection data type for domain-engine
#[derive(Debug)]
pub enum NaiveSelection {
    Node(IndexMap<String, NaiveSelection>),
    Union(IndexMap<DefId, IndexMap<String, NaiveSelection>>),
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

            for child_look_ahead in look_ahead.children() {
                let field_name = child_look_ahead.field_name();
                let field_data = object_data.fields.get(field_name).expect("no such field");

                match field_data.field_type.unit {
                    UnitTypeRef::Indexed(type_index) => {
                        map.insert(
                            field_name.into(),
                            analyze_selection(
                                child_look_ahead,
                                virtual_schema.type_data(type_index),
                                virtual_schema,
                            ),
                        );
                    }
                    UnitTypeRef::ID(_) => (),
                    UnitTypeRef::NativeScalar(_) => (),
                }
            }

            NaiveSelection::Node(map)
        }
        TypeKind::Union(union_data) => {
            let mut union_map: IndexMap<DefId, IndexMap<String, NaiveSelection>> = IndexMap::new();

            for child_look_ahead in look_ahead.children() {
                let field_name = child_look_ahead.field_name();

                let applies_for = child_look_ahead.applies_for();

                debug!("union field `{field_name}` applies for: {applies_for:?}",);

                if let Some(applies_for) = applies_for {
                    let variant_type_data =
                        find_union_variant_by_typename(union_data, applies_for, virtual_schema)
                            .expect("BUG: Union variant not found");

                    let def_id = match &variant_type_data.kind {
                        TypeKind::Object(ObjectData {
                            kind: ObjectKind::Node(NodeData { def_id, .. }),
                            ..
                        }) => *def_id,
                        _ => panic!(
                            "Unable to extract def_id from union variant: Not an Object/Node"
                        ),
                    };

                    let variant_map = union_map.entry(def_id).or_default();

                    variant_map.insert(
                        field_name.into(),
                        analyze_selection(child_look_ahead, variant_type_data, virtual_schema),
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

fn find_union_variant_by_typename<'s>(
    union_data: &'s UnionData,
    typename: &str,
    virtual_schema: &'s VirtualSchema,
) -> Option<&'s TypeData> {
    for type_index in &union_data.variants {
        let type_data = virtual_schema.type_data(*type_index);

        if type_data.typename == typename {
            return Some(type_data);
        }
    }

    None
}
