use fnv::FnvHashMap;
use juniper::LookAheadMethods;
use ontol_runtime::{value::PropertyId, DefId, RelationId};
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    virtual_schema::{
        data::{FieldData, FieldKind, NodeData, ObjectData, ObjectKind, TypeData, TypeKind},
        VirtualSchema,
    },
};

pub struct SelectionProperty {
    pub property_id: PropertyId,
    pub selection: NaiveSelection,
}

/// TODO: An "official" selection data type for domain-engine
#[derive(Clone, Debug)]
pub enum NaiveSelection {
    Node(FnvHashMap<PropertyId, NaiveSelection>),
    Union(FnvHashMap<DefId, FnvHashMap<PropertyId, NaiveSelection>>),
    Scalar,
    IdScalar,
}

pub fn analyze(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    field_data: &FieldData,
    virtual_schema: &VirtualSchema,
) -> SelectionProperty {
    match (
        &field_data.kind,
        virtual_schema.lookup_type_data(field_data.field_type.unit),
    ) {
        (
            FieldKind::Connection { property_id, .. },
            Ok(TypeData {
                kind: TypeKind::Object(object_data),
                ..
            }),
        ) => {
            let mut selection = None;

            for field_look_ahead in look_ahead.children() {
                let field_name = field_look_ahead.field_name();
                let field_data = object_data.fields.get(field_name).unwrap();

                if let FieldKind::Edges = &field_data.kind {
                    selection =
                        Some(analyze(field_look_ahead, field_data, virtual_schema).selection);
                }
            }

            SelectionProperty {
                property_id: property_id.unwrap_or(unit_property()),
                selection: selection.unwrap_or(NaiveSelection::Node(Default::default())),
            }
        }
        (
            FieldKind::Edges,
            Ok(TypeData {
                kind: TypeKind::Object(object_data),
                ..
            }),
        ) => {
            let mut selection = None;

            for field_look_ahead in look_ahead.children() {
                let field_name = field_look_ahead.field_name();
                let field_data = object_data.fields.get(field_name).unwrap();

                if let FieldKind::Node = &field_data.kind {
                    selection = Some(analyze(field_look_ahead, field_data, virtual_schema));
                }
            }

            selection.unwrap_or_else(|| SelectionProperty {
                property_id: unit_property(),
                selection: NaiveSelection::Node(Default::default()),
            })
        }
        (FieldKind::Node, Ok(type_data)) => SelectionProperty {
            property_id: unit_property(),
            selection: analyze_data(look_ahead, type_data, virtual_schema),
        },
        (FieldKind::Property(property_id), Ok(type_data)) => SelectionProperty {
            property_id: *property_id,
            selection: analyze_data(look_ahead, type_data, virtual_schema),
        },
        (FieldKind::Property(property_id), Err(_scalar_ref)) => SelectionProperty {
            property_id: *property_id,
            selection: NaiveSelection::Scalar,
        },
        (FieldKind::Id(relation_id), Err(_scalar_ref)) => SelectionProperty {
            property_id: PropertyId::subject(*relation_id),
            selection: NaiveSelection::IdScalar,
        },
        (kind, res) => panic!("unhandled: {kind:?} res is ok: {}", res.is_ok()),
    }
}

pub fn analyze_data(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    type_data: &TypeData,
    virtual_schema: &VirtualSchema,
) -> NaiveSelection {
    match &type_data.kind {
        TypeKind::Object(object_data) => match &object_data.kind {
            ObjectKind::Node(_) => {
                let mut map: FnvHashMap<PropertyId, NaiveSelection> = FnvHashMap::default();

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    let SelectionProperty {
                        property_id,
                        selection,
                    } = analyze(field_look_ahead, field_data, virtual_schema);

                    map.insert(property_id, selection);
                }

                NaiveSelection::Node(map)
            }
            ObjectKind::Edge(_) => {
                panic!()
            }
            ObjectKind::Connection => {
                panic!()
            }
            ObjectKind::Query | ObjectKind::Mutation => panic!("Bug in virtual schema"),
        },
        TypeKind::Union(union_data) => {
            let mut union_map: FnvHashMap<DefId, FnvHashMap<PropertyId, NaiveSelection>> =
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
                    let field_data = fields.get(field_name).unwrap();

                    let SelectionProperty {
                        property_id,
                        selection,
                    } = analyze(field_look_ahead, field_data, virtual_schema);

                    variant_map.insert(property_id, selection);
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

const fn unit_property() -> PropertyId {
    PropertyId::subject(RelationId(DefId::unit()))
}
