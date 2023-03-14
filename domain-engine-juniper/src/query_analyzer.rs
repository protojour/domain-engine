use fnv::FnvHashMap;
use juniper::LookAheadMethods;
use ontol_runtime::{
    query::{EntityQuery, MapOrUnionQuery, MapQuery, Query},
    value::PropertyId,
    DefId, RelationId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    look_ahead_utils::ArgsWrapper,
    virtual_schema::{
        argument::FieldArg,
        data::{FieldData, FieldKind, NodeData, ObjectData, ObjectKind, TypeData, TypeKind},
        VirtualSchema,
    },
};

pub struct KeyedPropertySelection {
    pub key: PropertyId,
    pub selection: Query,
}

pub fn analyze_entity_query(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    field_data: &FieldData,
    virtual_schema: &VirtualSchema,
) -> EntityQuery {
    match analyze(look_ahead, field_data, virtual_schema).selection {
        Query::Entity(entity_query) => entity_query,
        query => panic!("BUG: not an entity query: {query:?}"),
    }
}

pub fn analyze_map_query(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    field_data: &FieldData,
    virtual_schema: &VirtualSchema,
) -> MapOrUnionQuery {
    match analyze(look_ahead, field_data, virtual_schema).selection {
        Query::Map(map) => MapOrUnionQuery::Map(map),
        Query::MapUnion(def_id, variants) => MapOrUnionQuery::Union(def_id, variants),
        query => panic!("BUG: not a map query: {query:?}"),
    }
}

pub fn analyze(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    field_data: &FieldData,
    virtual_schema: &VirtualSchema,
) -> KeyedPropertySelection {
    match (
        &field_data.kind,
        virtual_schema.lookup_type_data(field_data.field_type.unit),
    ) {
        (
            FieldKind::Connection {
                property_id,
                first,
                after,
            },
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

            let args_wrapper = ArgsWrapper::new(look_ahead.arguments());

            let limit = args_wrapper
                .deserialize::<u32>(first.name())
                .unwrap()
                .unwrap_or_else(|| virtual_schema.config().default_limit);
            let cursor = args_wrapper.deserialize::<String>(after.name()).unwrap();

            KeyedPropertySelection {
                key: property_id.unwrap_or(unit_property()),
                selection: match selection {
                    Some(Query::Map(object)) => Query::Entity(EntityQuery {
                        source: MapOrUnionQuery::Map(object),
                        limit,
                        cursor,
                    }),
                    Some(Query::MapUnion(def_id, variants)) => Query::Entity(EntityQuery {
                        source: MapOrUnionQuery::Union(def_id, variants),
                        limit,
                        cursor,
                    }),
                    Some(Query::Entity(_)) => panic!("Query in query"),
                    Some(Query::Leaf) | None => Query::Leaf,
                },
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

            selection.unwrap_or_else(|| KeyedPropertySelection {
                key: unit_property(),
                selection: Query::Leaf,
            })
        }
        (
            FieldKind::Node | FieldKind::CreateMutation { .. } | FieldKind::UpdateMutation { .. },
            Ok(type_data),
        ) => KeyedPropertySelection {
            key: unit_property(),
            selection: analyze_data(look_ahead, type_data, virtual_schema),
        },
        (FieldKind::Property(property_data), Ok(type_data)) => KeyedPropertySelection {
            key: property_data.property_id,
            selection: analyze_data(look_ahead, type_data, virtual_schema),
        },
        (FieldKind::Property(property_data), Err(_scalar_ref)) => KeyedPropertySelection {
            key: property_data.property_id,
            selection: Query::Leaf,
        },
        (FieldKind::Id(id_property_data), Err(_scalar_ref)) => KeyedPropertySelection {
            key: PropertyId::subject(id_property_data.relation_id),
            selection: Query::Leaf,
        },
        (kind, res) => panic!("unhandled: {kind:?} res is ok: {}", res.is_ok()),
    }
}

pub fn analyze_data(
    look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
    type_data: &TypeData,
    virtual_schema: &VirtualSchema,
) -> Query {
    match &type_data.kind {
        TypeKind::Object(object_data) => match &object_data.kind {
            ObjectKind::Node(node_data) => {
                let mut properties: FnvHashMap<PropertyId, Query> = FnvHashMap::default();

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    let KeyedPropertySelection {
                        key: property_id,
                        selection,
                    } = analyze(field_look_ahead, field_data, virtual_schema);

                    properties.insert(property_id, selection);
                }

                Query::Map(MapQuery {
                    def_id: node_data.def_id,
                    properties,
                })
            }
            ObjectKind::Edge(_)
            | ObjectKind::Connection
            | ObjectKind::Query
            | ObjectKind::Mutation => panic!("Bug in virtual schema"),
        },
        TypeKind::Union(union_data) => {
            let mut union_map: FnvHashMap<DefId, FnvHashMap<PropertyId, Query>> =
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

                    let KeyedPropertySelection {
                        key: property_id,
                        selection,
                    } = analyze(field_look_ahead, field_data, virtual_schema);

                    variant_map.insert(property_id, selection);
                }
            }

            Query::MapUnion(
                union_data.union_def_id,
                union_map
                    .into_iter()
                    .map(|(def_id, properties)| MapQuery { def_id, properties })
                    .collect(),
            )
        }
        TypeKind::CustomScalar(_) => {
            assert!(look_ahead.children().is_empty());
            Query::Leaf
        }
    }
}

const fn unit_property() -> PropertyId {
    PropertyId::subject(RelationId(DefId::unit()))
}
