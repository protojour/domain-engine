use fnv::FnvHashMap;
use juniper::{LookAheadMethods, LookAheadSelection};
use ontol_runtime::{
    condition::Condition,
    interface::graphql::{
        argument::FieldArg,
        data::{
            ConnectionData, EdgeData, FieldData, FieldKind, NodeData, ObjectData, ObjectKind,
            TypeData, TypeKind, TypeModifier,
        },
    },
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    sequence::Cursor,
    value::{Attribute, PropertyId},
    var::Var,
    DefId, MapKey, RelationshipId,
};
use smartstring::alias::String;
use tracing::trace;

use crate::{context::SchemaCtx, gql_scalar::GqlScalar, look_ahead_utils::ArgsWrapper, ServiceCtx};

pub struct KeyedPropertySelection {
    pub key: PropertyId,
    pub select: Select,
}

pub struct SelectAnalyzer<'a> {
    schema_ctx: &'a SchemaCtx,
    service_ctx: &'a ServiceCtx,
}

#[derive(Debug)]
pub enum AnalyzedQuery {
    NamedMap {
        key: [MapKey; 2],
        input: Attribute,
        selects: FnvHashMap<Var, EntitySelect>,
    },
    ClassicConnection(EntitySelect),
}

impl<'a> SelectAnalyzer<'a> {
    pub fn new(schema_ctx: &'a SchemaCtx, service_ctx: &'a ServiceCtx) -> Self {
        Self {
            schema_ctx,
            service_ctx,
        }
    }

    pub fn analyze_look_ahead(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> Result<AnalyzedQuery, juniper::FieldError<GqlScalar>> {
        match &field_data.kind {
            FieldKind::MapConnection {
                key,
                input_arg,
                queries: map_queries,
                ..
            }
            | FieldKind::MapFind {
                key,
                input_arg,
                queries: map_queries,
            } => {
                let input = ArgsWrapper::new(look_ahead.arguments())
                    .deserialize_domain_field_arg_as_attribute(
                        input_arg,
                        &self.schema_ctx.ontology,
                    )?;

                let mut output_selects: FnvHashMap<Var, EntitySelect> = Default::default();

                self.analyze_map(
                    look_ahead,
                    field_data,
                    unit_property(),
                    map_queries,
                    &mut output_selects,
                );

                Ok(AnalyzedQuery::NamedMap {
                    key: *key,
                    input,
                    selects: output_selects,
                })
            }
            _ => match self.analyze_selection(look_ahead, field_data).select {
                Select::Entity(entity_select) => {
                    Ok(AnalyzedQuery::ClassicConnection(entity_select))
                }
                select => panic!("BUG: not an entity select: {select:?}"),
            },
        }
    }

    pub fn analyze_struct_select(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> StructOrUnionSelect {
        match self.analyze_selection(look_ahead, field_data).select {
            Select::Struct(struct_) => StructOrUnionSelect::Struct(struct_),
            Select::StructUnion(def_id, variants) => StructOrUnionSelect::Union(def_id, variants),
            select => panic!("BUG: not a struct select: {select:?}"),
        }
    }

    fn analyze_map(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
        parent_property: PropertyId,
        input_queries: &FnvHashMap<PropertyId, Var>,
        output_selects: &mut FnvHashMap<Var, EntitySelect>,
    ) {
        match input_queries.get(&parent_property) {
            Some(var) => {
                let selection = self.analyze_selection(look_ahead, field_data);
                match selection.select {
                    Select::Entity(entity_select) => {
                        output_selects.insert(*var, entity_select);
                    }
                    select => panic!("BUG: not an entity select: {select:?}"),
                }
            }
            None => match self.schema_ctx.lookup_type_data(field_data.field_type.unit) {
                Ok(TypeData {
                    kind: TypeKind::Object(_),
                    ..
                }) => {}
                _ => {
                    panic!();
                }
            },
        }
    }

    fn analyze_selection(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> KeyedPropertySelection {
        match (
            &field_data.kind,
            self.schema_ctx.lookup_type_data(field_data.field_type.unit),
        ) {
            (FieldKind::MapFind { .. }, Ok(type_data)) => {
                match (field_data.field_type.modifier, type_data) {
                    (TypeModifier::Unit(_), type_data) => KeyedPropertySelection {
                        key: unit_property(),
                        select: match self.analyze_data(look_ahead, &type_data.kind) {
                            Select::Struct(struct_select) => Select::Entity(EntitySelect {
                                source: StructOrUnionSelect::Struct(struct_select),
                                condition: Condition::default(),
                                limit: 1,
                                after_cursor: None,
                            }),
                            select => select,
                        },
                    },
                    (TypeModifier::Array(..), _) => {
                        panic!("Should be a MapConnection");
                    }
                }
            }
            (
                FieldKind::MapConnection {
                    first_arg,
                    after_arg,
                    ..
                },
                Ok(TypeData {
                    kind: TypeKind::Object(object_data),
                    ..
                }),
            ) => {
                let args_wrapper = ArgsWrapper::new(look_ahead.arguments());

                let limit = args_wrapper
                    .deserialize::<usize>(first_arg.name())
                    .unwrap()
                    .unwrap_or(self.default_limit());
                let _cursor = args_wrapper
                    .deserialize::<String>(after_arg.name())
                    .unwrap();

                let mut inner_select = None;

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name_unaliased();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    if let FieldKind::Edges = &field_data.kind {
                        inner_select =
                            Some(self.analyze_selection(field_look_ahead, field_data).select);
                    }
                }

                KeyedPropertySelection {
                    key: unit_property(),
                    select: self.mk_entity_select(inner_select, limit, None, object_data),
                }
            }
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
                let args_wrapper = ArgsWrapper::new(look_ahead.arguments());

                let limit = args_wrapper
                    .deserialize::<usize>(first.name())
                    .unwrap()
                    .unwrap_or(self.default_limit());
                let _cursor = args_wrapper.deserialize::<String>(after.name()).unwrap();

                let mut inner_select = None;

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name_unaliased();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    if let FieldKind::Edges = &field_data.kind {
                        inner_select =
                            Some(self.analyze_selection(field_look_ahead, field_data).select);
                    }
                }

                KeyedPropertySelection {
                    key: property_id.unwrap_or(unit_property()),
                    select: self.mk_entity_select(inner_select, limit, None, object_data),
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
                    let field_name = field_look_ahead.field_name_unaliased();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    if let FieldKind::Node = &field_data.kind {
                        selection = Some(self.analyze_selection(field_look_ahead, field_data));
                    }
                }

                selection.unwrap_or_else(|| KeyedPropertySelection {
                    key: unit_property(),
                    select: Select::Leaf,
                })
            }
            (
                FieldKind::Node
                | FieldKind::CreateMutation { .. }
                | FieldKind::UpdateMutation { .. },
                Ok(type_data),
            ) => KeyedPropertySelection {
                key: unit_property(),
                select: self.analyze_data(look_ahead, &type_data.kind),
            },
            (FieldKind::Property(property_data), Ok(type_data)) => KeyedPropertySelection {
                key: property_data.property_id,
                select: self.analyze_data(look_ahead, &type_data.kind),
            },
            (FieldKind::Property(property_data), Err(_scalar_ref)) => KeyedPropertySelection {
                key: property_data.property_id,
                select: Select::Leaf,
            },
            (FieldKind::Id(id_property_data), Err(_scalar_ref)) => KeyedPropertySelection {
                key: PropertyId::subject(id_property_data.relationship_id),
                select: Select::Leaf,
            },
            (kind, res) => panic!("unhandled: {kind:?} res is ok: {}", res.is_ok()),
        }
    }

    pub fn analyze_data(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        type_kind: &TypeKind,
    ) -> Select {
        match type_kind {
            TypeKind::Object(object_data) => self.analyze_object_data(look_ahead, object_data),
            TypeKind::Union(union_data) => {
                let mut union_map: FnvHashMap<DefId, FnvHashMap<PropertyId, Select>> =
                    FnvHashMap::default();

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name_unaliased();

                    let applies_for = field_look_ahead.applies_for();

                    trace!("union field `{field_name}` applies for: {applies_for:?}",);

                    if let Some(applies_for) = applies_for {
                        let variant_type_data = union_data
                            .variants
                            .iter()
                            .find_map(|type_addr| {
                                let type_data = self.schema_ctx.type_data(*type_addr);

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
                        let field_data =
                            fields.get(field_look_ahead.field_name_unaliased()).unwrap();

                        let KeyedPropertySelection {
                            key: property_id,
                            select: selection,
                        } = self.analyze_selection(field_look_ahead, field_data);

                        variant_map.insert(property_id, selection);
                    }
                }

                Select::StructUnion(
                    union_data.union_def_id,
                    union_map
                        .into_iter()
                        .map(|(def_id, properties)| StructSelect { def_id, properties })
                        .collect(),
                )
            }
            TypeKind::CustomScalar(_) => {
                assert!(look_ahead.children().is_empty());
                Select::Leaf
            }
        }
    }

    pub fn analyze_object_data(
        &self,
        look_ahead: &juniper::executor::LookAheadSelection<GqlScalar>,
        object_data: &ObjectData,
    ) -> Select {
        match &object_data.kind {
            ObjectKind::Node(node_data) => {
                let mut properties: FnvHashMap<PropertyId, Select> = FnvHashMap::default();

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name_unaliased();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    let KeyedPropertySelection {
                        key: property_id,
                        select: selection,
                    } = self.analyze_selection(field_look_ahead, field_data);

                    properties.insert(property_id, selection);
                }

                Select::Struct(StructSelect {
                    def_id: node_data.def_id,
                    properties,
                })
            }
            ObjectKind::Edge(_)
            | ObjectKind::Connection(_)
            | ObjectKind::PageInfo
            | ObjectKind::Query
            | ObjectKind::Mutation => panic!("Bug in ONTOL interface schema"),
        }
    }

    fn mk_entity_select(
        &self,
        mut inner_select: Option<Select>,
        limit: usize,
        after_cursor: Option<Cursor>,
        object_data: &ObjectData,
    ) -> Select {
        let mut empty_handled = false;

        loop {
            match inner_select {
                Some(Select::Struct(object)) => {
                    return Select::Entity(EntitySelect {
                        source: StructOrUnionSelect::Struct(object),
                        condition: Condition::default(),
                        limit,
                        after_cursor,
                    })
                }
                Some(Select::StructUnion(def_id, variants)) => {
                    return Select::Entity(EntitySelect {
                        source: StructOrUnionSelect::Union(def_id, variants),
                        condition: Condition::default(),
                        limit,
                        after_cursor,
                    })
                }
                Some(Select::Entity(_) | Select::EntityId) => panic!("Select in select"),
                Some(Select::Leaf) | None if !empty_handled => {
                    inner_select = Some(self.empty_entity_select(object_data));
                    empty_handled = true;
                }
                Some(Select::Leaf) | None => return Select::Leaf,
            }
        }
    }

    fn empty_entity_select(&self, object_data: &ObjectData) -> Select {
        match &object_data.kind {
            ObjectKind::Edge(EdgeData { node_type_addr, .. })
            | ObjectKind::Connection(ConnectionData { node_type_addr }) => {
                let type_data = self.schema_ctx.type_data(*node_type_addr);

                self.analyze_data(&LookAheadSelection::default(), &type_data.kind)
            }
            _ => self.analyze_object_data(&LookAheadSelection::default(), object_data),
        }
    }

    fn default_limit(&self) -> usize {
        self.service_ctx.domain_engine.config().default_limit
    }
}

const fn unit_property() -> PropertyId {
    PropertyId::subject(RelationshipId(DefId::unit()))
}
