use std::collections::hash_map::Entry;

use fnv::{FnvHashMap, FnvHashSet};
use juniper::{FieldError, LookAheadChildren};
use ontol_runtime::{
    condition::Condition,
    interface::graphql::{
        argument::{AfterArg, FieldArg, FirstArg},
        data::{
            ConnectionData, EdgeData, FieldData, FieldKind, NodeData, ObjectData, ObjectKind,
            TypeData, TypeKind, TypeModifier,
        },
    },
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    value::{PropertyId, Value},
    var::Var,
    DefId, MapKey, RelationshipId,
};
use tracing::{debug, trace};

use crate::{
    context::SchemaCtx, cursor_util::GraphQLCursor, gql_scalar::GqlScalar,
    look_ahead_utils::ArgsWrapper, ServiceCtx,
};

pub(crate) struct KeyedPropertySelection {
    pub key: PropertyId,
    pub select: Select,
}

pub(crate) struct SelectAnalyzer<'a> {
    schema_ctx: &'a SchemaCtx,
    service_ctx: &'a ServiceCtx,
}

#[derive(Debug)]
pub(crate) enum AnalyzedQuery {
    Map {
        map_key: MapKey,
        input: Value,
        selects: FnvHashMap<Var, EntitySelect>,
    },
    Version,
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
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> Result<AnalyzedQuery, juniper::FieldError<GqlScalar>> {
        match &field_data.kind {
            FieldKind::MapConnection {
                map_key,
                input_arg,
                queries: map_queries,
                ..
            }
            | FieldKind::MapFind {
                map_key,
                input_arg,
                queries: map_queries,
            } => {
                let input = ArgsWrapper::new(look_ahead)
                    .deserialize_domain_field_arg_as_attribute(
                        input_arg,
                        (self.schema_ctx, self.service_ctx),
                    )?
                    .val;

                debug!("field map queries: {map_queries:?}");

                let mut output_selects: FnvHashMap<Var, EntitySelect> = Default::default();

                self.analyze_map(
                    look_ahead,
                    field_data,
                    unit_property(),
                    map_queries,
                    &mut output_selects,
                    &mut Default::default(),
                )?;

                Ok(AnalyzedQuery::Map {
                    map_key: *map_key,
                    input,
                    selects: output_selects,
                })
            }
            FieldKind::Version => Ok(AnalyzedQuery::Version),
            _ => panic!(),
        }
    }

    fn analyze_map(
        &self,
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
        parent_property: PropertyId,
        input_queries: &FnvHashMap<PropertyId, Var>,
        output_selects: &mut FnvHashMap<Var, EntitySelect>,
        recursion_guard: &mut FnvHashSet<PropertyId>,
    ) -> Result<(), FieldError<GqlScalar>> {
        if !recursion_guard.insert(parent_property) {
            return Ok(());
        }

        debug!("analyze_map {parent_property:}: {field_data:?}");

        match input_queries.get(&parent_property) {
            Some(var) => {
                let selection = self.analyze_selection(look_ahead, field_data)?;
                match selection.map(|sel| sel.select) {
                    Some(Select::Entity(entity_select)) => {
                        output_selects.insert(*var, entity_select);
                        Ok(())
                    }
                    Some(Select::Struct(struct_select)) => {
                        // Need to convert struct select to "entity" select..
                        output_selects.insert(
                            *var,
                            EntitySelect {
                                source: StructOrUnionSelect::Struct(struct_select),
                                condition: Condition::default(),
                                limit: self.default_query_limit(),
                                after_cursor: None,
                                include_total_len: false,
                            },
                        );
                        Ok(())
                    }
                    select => panic!("BUG: not an entity select: {select:?}"),
                }
            }
            None => match self.schema_ctx.lookup_type_data(field_data.field_type.unit) {
                Ok(TypeData {
                    kind: TypeKind::Object(object_data),
                    ..
                }) => {
                    if true {
                        for child_look_ahead in look_ahead.children() {
                            let field_name = child_look_ahead.field_original_name();

                            if let Some(field_data) = object_data.fields.get(field_name) {
                                if let FieldKind::Property(property_data) = &field_data.kind {
                                    self.analyze_map(
                                        child_look_ahead,
                                        field_data,
                                        property_data.property_id,
                                        input_queries,
                                        output_selects,
                                        recursion_guard,
                                    )?;
                                }
                            }
                        }
                    } else {
                        for field in object_data.fields.values() {
                            if let FieldKind::Property(property_data) = &field.kind {
                                self.analyze_map(
                                    look_ahead,
                                    field_data,
                                    property_data.property_id,
                                    input_queries,
                                    output_selects,
                                    recursion_guard,
                                )?;
                            }
                        }
                    }

                    Ok(())
                }
                Ok(TypeData {
                    kind: TypeKind::Union(_),
                    ..
                }) => todo!("union"),
                Ok(TypeData {
                    kind: TypeKind::CustomScalar(_),
                    ..
                }) => Ok(()),
                Err(_native_scalar) => Ok(()),
            },
        }
    }

    pub fn analyze_select(
        &self,
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> Result<Select, FieldError<GqlScalar>> {
        self.analyze_selection(look_ahead, field_data)?
            .map(|sel| sel.select)
            .ok_or_else(|| panic!("BUG: not a select"))
    }

    fn analyze_selection(
        &self,
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
    ) -> Result<Option<KeyedPropertySelection>, FieldError<GqlScalar>> {
        match (
            &field_data.kind,
            self.schema_ctx.lookup_type_data(field_data.field_type.unit),
        ) {
            (FieldKind::MapFind { .. }, Ok(type_data)) => {
                match (field_data.field_type.modifier, type_data) {
                    (TypeModifier::Unit(_), type_data) => Ok(Some(KeyedPropertySelection {
                        key: unit_property(),
                        select: match self.analyze_data(look_ahead.children(), &type_data.kind)? {
                            Select::Struct(struct_select) => Select::Entity(EntitySelect {
                                source: StructOrUnionSelect::Struct(struct_select),
                                condition: Condition::default(),
                                limit: 1,
                                after_cursor: None,
                                include_total_len: false,
                            }),
                            select => select,
                        },
                    })),
                    (TypeModifier::Array { .. }, _) => {
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
            ) => Ok(Some(KeyedPropertySelection {
                key: unit_property(),
                select: self.analyze_connection(look_ahead, first_arg, after_arg, object_data)?,
            })),
            (
                FieldKind::ConnectionProperty {
                    property_id,
                    first_arg: first,
                    after_arg: after,
                },
                Ok(TypeData {
                    kind: TypeKind::Object(object_data),
                    ..
                }),
            ) => Ok(Some(KeyedPropertySelection {
                key: *property_id,
                select: self.analyze_connection(look_ahead, first, after, object_data)?,
            })),
            (
                FieldKind::Edges | FieldKind::EntityMutation { .. },
                Ok(TypeData {
                    kind: TypeKind::Object(object_data),
                    ..
                }),
            ) => {
                let mut selection = None;

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_original_name();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    if let FieldKind::Node = &field_data.kind {
                        selection = self.analyze_selection(field_look_ahead, field_data)?;
                    }
                }

                Ok(Some(selection.unwrap_or_else(|| KeyedPropertySelection {
                    key: unit_property(),
                    select: Select::Leaf,
                })))
            }
            (FieldKind::Node | FieldKind::Nodes, Ok(type_data)) => {
                Ok(Some(KeyedPropertySelection {
                    key: unit_property(),
                    select: self.analyze_data(look_ahead.children(), &type_data.kind)?,
                }))
            }
            (FieldKind::Property(property_data), Ok(type_data)) => {
                Ok(Some(KeyedPropertySelection {
                    key: property_data.property_id,
                    select: self.analyze_data(look_ahead.children(), &type_data.kind)?,
                }))
            }
            (FieldKind::Property(property_data), Err(_scalar_ref)) => {
                Ok(Some(KeyedPropertySelection {
                    key: property_data.property_id,
                    select: Select::Leaf,
                }))
            }
            (FieldKind::Id(id_property_data), Err(_scalar_ref)) => {
                Ok(Some(KeyedPropertySelection {
                    key: PropertyId::subject(id_property_data.relationship_id),
                    select: Select::Leaf,
                }))
            }
            (FieldKind::OpenData | FieldKind::Deleted, _) => Ok(None),
            (kind, res) => panic!("unhandled: {kind:?} res is ok: {}", res.is_ok()),
        }
    }

    fn analyze_connection(
        &self,
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        first_arg: &FirstArg,
        after_arg: &AfterArg,
        connection_object_data: &ObjectData,
    ) -> Result<Select, FieldError<GqlScalar>> {
        let args_wrapper = ArgsWrapper::new(look_ahead);

        let limit = args_wrapper
            .deserialize_optional::<usize>(first_arg.name(&self.schema_ctx.ontology))?
            .unwrap_or(self.default_query_limit());
        let after_cursor = args_wrapper
            .deserialize_optional::<GraphQLCursor>(after_arg.name(&self.schema_ctx.ontology))?;

        let mut inner_select = Select::Leaf;
        let mut include_total_len = false;

        for field_look_ahead in look_ahead.children() {
            let field_name = field_look_ahead.field_original_name();
            let field_data = connection_object_data.fields.get(field_name).unwrap();

            match &field_data.kind {
                FieldKind::Nodes | FieldKind::Edges => {
                    if let Some(selection) = self.analyze_selection(field_look_ahead, field_data)? {
                        merge_selects(&mut inner_select, selection.select);
                    }
                }
                FieldKind::TotalCount => {
                    include_total_len = true;
                }
                _ => {}
            }
        }

        self.mk_entity_select(
            inner_select,
            limit,
            after_cursor,
            include_total_len,
            connection_object_data,
        )
    }

    pub fn analyze_data(
        &self,
        look_ahead_children: juniper::executor::LookAheadChildren<GqlScalar>,
        type_kind: &TypeKind,
    ) -> Result<Select, FieldError<GqlScalar>> {
        match type_kind {
            TypeKind::Object(object_data) => {
                self.analyze_object_data(look_ahead_children, object_data)
            }
            TypeKind::Union(union_data) => {
                let mut union_map: FnvHashMap<DefId, FnvHashMap<PropertyId, Select>> =
                    FnvHashMap::default();

                for field_look_ahead in look_ahead_children {
                    let field_name = field_look_ahead.field_original_name();

                    let applies_for = field_look_ahead.applies_for();

                    trace!("union field `{field_name}` applies for: {applies_for:?}",);

                    if let Some(applies_for) = applies_for {
                        let variant_type_data = union_data
                            .variants
                            .iter()
                            .find_map(|type_addr| {
                                let type_data = self.schema_ctx.type_data(*type_addr);

                                if &self.schema_ctx.ontology[type_data.typename] == applies_for {
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
                            fields.get(field_look_ahead.field_original_name()).unwrap();

                        if let Some(selection) =
                            self.analyze_selection(field_look_ahead, field_data)?
                        {
                            variant_map.insert(selection.key, selection.select);
                        }
                    }
                }

                Ok(Select::StructUnion(
                    union_data.union_def_id,
                    union_map
                        .into_iter()
                        .map(|(def_id, properties)| StructSelect { def_id, properties })
                        .collect(),
                ))
            }
            TypeKind::CustomScalar(_) => {
                assert!(look_ahead_children.is_empty());
                Ok(Select::Leaf)
            }
        }
    }

    pub fn analyze_object_data(
        &self,
        look_ahead_children: juniper::executor::LookAheadChildren<GqlScalar>,
        object_data: &ObjectData,
    ) -> Result<Select, FieldError<GqlScalar>> {
        match &object_data.kind {
            ObjectKind::Node(node_data) => {
                let mut properties: FnvHashMap<PropertyId, Select> = FnvHashMap::default();

                for field_look_ahead in look_ahead_children {
                    let field_name = field_look_ahead.field_original_name();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    if let Some(selection) = self.analyze_selection(field_look_ahead, field_data)? {
                        properties.insert(selection.key, selection.select);
                    }
                }

                Ok(Select::Struct(StructSelect {
                    def_id: node_data.def_id,
                    properties,
                }))
            }
            ObjectKind::Edge(_)
            | ObjectKind::Connection(_)
            | ObjectKind::PageInfo
            | ObjectKind::MutationResult
            | ObjectKind::Query
            | ObjectKind::Mutation => panic!("Bug in ONTOL interface schema"),
        }
    }

    fn mk_entity_select(
        &self,
        mut inner_select: Select,
        limit: usize,
        after_cursor: Option<GraphQLCursor>,
        include_total_len: bool,
        object_data: &ObjectData,
    ) -> Result<Select, FieldError<GqlScalar>> {
        let mut empty_handled = false;
        let after_cursor = after_cursor.map(|cur| cur.0);

        loop {
            match inner_select {
                Select::Struct(object) => {
                    return Ok(Select::Entity(EntitySelect {
                        source: StructOrUnionSelect::Struct(object),
                        condition: Condition::default(),
                        limit,
                        after_cursor,
                        include_total_len,
                    }));
                }
                Select::StructUnion(def_id, variants) => {
                    return Ok(Select::Entity(EntitySelect {
                        source: StructOrUnionSelect::Union(def_id, variants),
                        condition: Condition::default(),
                        limit,
                        after_cursor,
                        include_total_len,
                    }))
                }
                Select::Entity(_) | Select::EntityId => panic!("Select in select"),
                Select::Leaf if !empty_handled => {
                    inner_select = self.empty_entity_select(object_data)?;
                    empty_handled = true;
                }
                Select::Leaf => return Ok(Select::Leaf),
            }
        }
    }

    fn empty_entity_select(
        &self,
        object_data: &ObjectData,
    ) -> Result<Select, FieldError<GqlScalar>> {
        match &object_data.kind {
            ObjectKind::Edge(EdgeData { node_type_addr, .. })
            | ObjectKind::Connection(ConnectionData { node_type_addr }) => {
                let type_data = self.schema_ctx.type_data(*node_type_addr);

                self.analyze_data(LookAheadChildren::default(), &type_data.kind)
            }
            _ => self.analyze_object_data(LookAheadChildren::default(), object_data),
        }
    }

    fn default_query_limit(&self) -> usize {
        self.service_ctx
            .domain_engine
            .system()
            .default_query_limit()
    }
}

const fn unit_property() -> PropertyId {
    PropertyId::subject(RelationshipId(DefId::unit()))
}

fn merge_selects(existing: &mut Select, new: Select) {
    match (existing, new) {
        (Select::Struct(existing), Select::Struct(new)) => {
            merge_struct_selects(existing, new);
        }
        (Select::StructUnion(_, existing_selects), Select::StructUnion(_, new_selects)) => {
            for new_select in new_selects {
                let existing_select = existing_selects
                    .iter_mut()
                    .find(|sel| sel.def_id == new_select.def_id);

                if let Some(existing_select) = existing_select {
                    merge_struct_selects(existing_select, new_select);
                } else {
                    existing_selects.push(new_select);
                }
            }
        }
        (existing @ Select::Leaf, any_other) => {
            *existing = any_other;
        }
        (existing, new) => {
            panic!("unable to merge {existing:?} and {new:?}");
        }
    }
}

fn merge_struct_selects(existing: &mut StructSelect, new: StructSelect) {
    for (property_id, new) in new.properties {
        match existing.properties.entry(property_id) {
            Entry::Vacant(vacant) => {
                vacant.insert(new);
            }
            Entry::Occupied(mut occupied) => merge_selects(occupied.get_mut(), new),
        }
    }
}
