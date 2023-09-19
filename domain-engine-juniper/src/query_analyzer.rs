use fnv::FnvHashMap;
use juniper::LookAheadMethods;
use ontol_runtime::{
    interface::graphql::{
        argument::FieldArg,
        data::{FieldData, FieldKind, NodeData, ObjectData, ObjectKind, TypeData, TypeKind},
    },
    query::{EntityQuery, Query, StructOrUnionQuery, StructQuery},
    value::PropertyId,
    DefId, RelationshipId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{context::SchemaCtx, gql_scalar::GqlScalar, look_ahead_utils::ArgsWrapper, ServiceCtx};

pub struct KeyedPropertySelection {
    pub key: PropertyId,
    pub selection: Query,
}

pub struct QueryAnalyzer<'a> {
    schema_ctx: &'a SchemaCtx,
    gql_ctx: &'a ServiceCtx,
}

impl<'a> QueryAnalyzer<'a> {
    pub fn new(schema_ctx: &'a SchemaCtx, gql_ctx: &'a ServiceCtx) -> Self {
        Self {
            schema_ctx,
            gql_ctx,
        }
    }

    pub fn analyze_entity_query(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> EntityQuery {
        match self.analyze(look_ahead, field_data).selection {
            Query::Entity(entity_query) => entity_query,
            query => panic!("BUG: not an entity query: {query:?}"),
        }
    }

    pub fn analyze_struct_query(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> StructOrUnionQuery {
        match self.analyze(look_ahead, field_data).selection {
            Query::Struct(struct_) => StructOrUnionQuery::Struct(struct_),
            Query::StructUnion(def_id, variants) => StructOrUnionQuery::Union(def_id, variants),
            query => panic!("BUG: not a struct query: {query:?}"),
        }
    }

    pub fn analyze(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> KeyedPropertySelection {
        match (
            &field_data.kind,
            self.schema_ctx.lookup_type_data(field_data.field_type.unit),
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
                        selection = Some(self.analyze(field_look_ahead, field_data).selection);
                    }
                }

                let args_wrapper = ArgsWrapper::new(look_ahead.arguments());

                let limit = args_wrapper
                    .deserialize::<u32>(first.name())
                    .unwrap()
                    .unwrap_or(self.gql_ctx.domain_engine.config().default_limit);
                let cursor = args_wrapper.deserialize::<String>(after.name()).unwrap();

                KeyedPropertySelection {
                    key: property_id.unwrap_or(unit_property()),
                    selection: match selection {
                        Some(Query::Struct(object)) => Query::Entity(EntityQuery {
                            source: StructOrUnionQuery::Struct(object),
                            limit,
                            cursor,
                        }),
                        Some(Query::StructUnion(def_id, variants)) => Query::Entity(EntityQuery {
                            source: StructOrUnionQuery::Union(def_id, variants),
                            limit,
                            cursor,
                        }),
                        Some(Query::Entity(_) | Query::EntityId) => panic!("Query in query"),
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
                        selection = Some(self.analyze(field_look_ahead, field_data));
                    }
                }

                selection.unwrap_or_else(|| KeyedPropertySelection {
                    key: unit_property(),
                    selection: Query::Leaf,
                })
            }
            (
                FieldKind::Node
                | FieldKind::CreateMutation { .. }
                | FieldKind::UpdateMutation { .. },
                Ok(type_data),
            ) => KeyedPropertySelection {
                key: unit_property(),
                selection: self.analyze_data(look_ahead, type_data),
            },
            (FieldKind::Property(property_data), Ok(type_data)) => KeyedPropertySelection {
                key: property_data.property_id,
                selection: self.analyze_data(look_ahead, type_data),
            },
            (FieldKind::Property(property_data), Err(_scalar_ref)) => KeyedPropertySelection {
                key: property_data.property_id,
                selection: Query::Leaf,
            },
            (FieldKind::Id(id_property_data), Err(_scalar_ref)) => KeyedPropertySelection {
                key: PropertyId::subject(id_property_data.relationship_id),
                selection: Query::Leaf,
            },
            (kind, res) => panic!("unhandled: {kind:?} res is ok: {}", res.is_ok()),
        }
    }

    pub fn analyze_data(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        type_data: &TypeData,
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
                        } = self.analyze(field_look_ahead, field_data);

                        properties.insert(property_id, selection);
                    }

                    Query::Struct(StructQuery {
                        def_id: node_data.def_id,
                        properties,
                    })
                }
                ObjectKind::Edge(_)
                | ObjectKind::Connection
                | ObjectKind::Query
                | ObjectKind::Mutation => panic!("Bug in ONTOL interface schema"),
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
                                let type_data = self.schema_ctx.type_data(*type_index);

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
                        } = self.analyze(field_look_ahead, field_data);

                        variant_map.insert(property_id, selection);
                    }
                }

                Query::StructUnion(
                    union_data.union_def_id,
                    union_map
                        .into_iter()
                        .map(|(def_id, properties)| StructQuery { def_id, properties })
                        .collect(),
                )
            }
            TypeKind::CustomScalar(_) => {
                assert!(look_ahead.children().is_empty());
                Query::Leaf
            }
        }
    }
}

const fn unit_property() -> PropertyId {
    PropertyId::subject(RelationshipId(DefId::unit()))
}
