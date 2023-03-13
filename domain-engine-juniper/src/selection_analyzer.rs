use fnv::FnvHashMap;
use indexmap::IndexMap;
use juniper::LookAheadMethods;
use ontol_runtime::DefId;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    virtual_schema::{
        data::{FieldData, FieldKind, NodeData, ObjectData, ObjectKind, TypeData, TypeKind},
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

pub fn analyze(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    field_data: &FieldData,
    virtual_schema: &VirtualSchema,
) -> NaiveSelection {
    match (
        &field_data.kind,
        virtual_schema.lookup_type_data(field_data.field_type.unit),
    ) {
        (
            FieldKind::ConnectionQuery(_arguments),
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
                    selection = Some(analyze(field_look_ahead, field_data, virtual_schema));
                }
            }

            selection.unwrap_or_else(|| NaiveSelection::Node(Default::default()))
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

            selection.unwrap_or_else(|| NaiveSelection::Node(Default::default()))
        }
        (FieldKind::Node | FieldKind::Data, Ok(type_data)) => {
            analyze_data(look_ahead, type_data, virtual_schema)
        }
        (FieldKind::Data | FieldKind::Id, Err(_scalar_ref)) => NaiveSelection::Scalar,
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
                let mut map: IndexMap<String, NaiveSelection> = IndexMap::new();

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    map.insert(
                        field_name.into(),
                        analyze(field_look_ahead, field_data, virtual_schema),
                    );
                }

                NaiveSelection::Node(map)
            }
            ObjectKind::Edge(_) => {
                panic!()
            }
            ObjectKind::Connection(_) => {
                panic!()
            }
            ObjectKind::Query | ObjectKind::Mutation => panic!("Bug in virtual schema"),
        },
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
                    let field_data = fields.get(field_name).unwrap();

                    variant_map.insert(
                        field_name.into(),
                        analyze(field_look_ahead, field_data, virtual_schema),
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
