use std::collections::{BTreeMap, btree_map::Entry};

use fnv::{FnvHashMap, FnvHashSet};
use juniper::{FieldError, LookAheadChildren};
use ontol_runtime::{
    DefId, DefPropTag, MapKey, OntolDefTag, OntolDefTagExt, PropId,
    interface::{
        graphql::{
            argument::{AfterArg, FieldArg, FirstArg, MapInputArg},
            data::{
                ConnectionData, EdgeData, FieldData, FieldKind, NodeData, ObjectData, ObjectKind,
                TypeData, TypeKind, TypeModifier, UnitTypeRef,
            },
        },
        serde::operator::SerdeOperator,
    },
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    value::Value,
    var::Var,
};
use tracing::{debug, error, trace};

use crate::{
    cursor_util::GraphQLCursor,
    domain::{ServiceCtx, context::SchemaCtx, look_ahead_utils::ArgsWrapper},
    gql_scalar::GqlScalar,
};

pub(crate) struct KeyedPropertySelection {
    pub key: PropId,
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
            FieldKind::MapConnection(field) => self.analyze_map_field(
                look_ahead,
                field.map_key,
                &field.input_arg,
                &field.queries,
                field_data,
            ),
            FieldKind::MapFind(field) => self.analyze_map_field(
                look_ahead,
                field.map_key,
                &field.input_arg,
                &field.queries,
                field_data,
            ),
            FieldKind::Version => Ok(AnalyzedQuery::Version),
            _ => panic!(),
        }
    }

    fn analyze_map_field(
        &self,
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        map_key: MapKey,
        input_arg: &MapInputArg,
        map_queries: &FnvHashMap<PropId, Var>,
        field_data: &FieldData,
    ) -> Result<AnalyzedQuery, juniper::FieldError<GqlScalar>> {
        let input = ArgsWrapper::new(look_ahead)
            .deserialize_domain_field_arg_as_attribute(
                input_arg,
                (self.schema_ctx, self.service_ctx),
            )?
            .into_unit()
            .expect("not a unit");

        debug!(?map_queries, "analyze map field");

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
            map_key,
            input,
            selects: output_selects,
        })
    }

    fn analyze_map(
        &self,
        look_ahead: juniper::executor::LookAheadSelection<GqlScalar>,
        field_data: &FieldData,
        parent_property: PropId,
        input_queries: &FnvHashMap<PropId, Var>,
        output_selects: &mut FnvHashMap<Var, EntitySelect>,
        recursion_guard: &mut FnvHashSet<PropId>,
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
                                filter: Filter::default_for_domain(),
                                limit: Some(self.default_query_limit()),
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
                                if let FieldKind::Property {
                                    id: property_id, ..
                                } = &field_data.kind
                                {
                                    self.analyze_map(
                                        child_look_ahead,
                                        field_data,
                                        *property_id,
                                        input_queries,
                                        output_selects,
                                        recursion_guard,
                                    )?;
                                }
                            }
                        }
                    } else {
                        for (_, field) in object_data.fields.iter() {
                            if let FieldKind::Property {
                                id: property_id, ..
                            } = &field.kind
                            {
                                self.analyze_map(
                                    look_ahead,
                                    field_data,
                                    *property_id,
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
                                filter: Filter::default_for_domain(),
                                limit: Some(1),
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
                FieldKind::MapConnection(field),
                Ok(TypeData {
                    kind: TypeKind::Object(object_data),
                    ..
                }),
            ) => Ok(Some(KeyedPropertySelection {
                key: unit_property(),
                select: self.analyze_connection(
                    look_ahead,
                    &field.first_arg,
                    &field.after_arg,
                    object_data,
                )?,
            })),
            (
                FieldKind::ConnectionProperty(field),
                Ok(TypeData {
                    kind: TypeKind::Object(object_data),
                    ..
                }),
            ) => Ok(Some(KeyedPropertySelection {
                key: field.prop_id,
                select: self.analyze_connection(
                    look_ahead,
                    &field.first_arg,
                    &field.after_arg,
                    object_data,
                )?,
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
                    let Some(field_data) = object_data.fields.get(field_name) else {
                        continue;
                    };

                    if let FieldKind::Node = &field_data.kind {
                        selection = self.analyze_selection(field_look_ahead, field_data)?;
                    }
                }

                Ok(Some(selection.unwrap_or_else(|| KeyedPropertySelection {
                    key: unit_property(),
                    select: Select::Unit,
                })))
            }
            (FieldKind::Node | FieldKind::Nodes, Ok(type_data)) => {
                Ok(Some(KeyedPropertySelection {
                    key: unit_property(),
                    select: self.analyze_data(look_ahead.children(), &type_data.kind)?,
                }))
            }
            (FieldKind::Node | FieldKind::Nodes, Err(_scalar_ref)) => {
                Ok(Some(KeyedPropertySelection {
                    key: unit_property(),
                    select: Select::Unit,
                }))
            }
            (
                FieldKind::Property {
                    id: property_id, ..
                },
                Ok(type_data),
            ) => Ok(Some(KeyedPropertySelection {
                key: *property_id,
                select: self.analyze_data(look_ahead.children(), &type_data.kind)?,
            })),
            (
                FieldKind::FlattenedProperty {
                    id: relationship_id,
                    ..
                },
                Ok(type_data),
            ) => Ok(Some(KeyedPropertySelection {
                key: *relationship_id,
                select: self.analyze_data(look_ahead.children(), &type_data.kind)?,
            })),
            (
                FieldKind::Property {
                    id: property_id, ..
                },
                Err(_scalar_ref),
            ) => Ok(Some(KeyedPropertySelection {
                key: *property_id,
                select: Select::Unit,
            })),
            (
                FieldKind::FlattenedProperty {
                    id: relationship_id,
                    ..
                },
                Err(_scalar_ref),
            ) => Ok(Some(KeyedPropertySelection {
                key: *relationship_id,
                select: Select::Unit,
            })),
            (FieldKind::Id(id_property_data), Err(_scalar_ref)) => {
                Ok(Some(KeyedPropertySelection {
                    key: id_property_data.prop_id,
                    select: Select::Unit,
                }))
            }
            (
                FieldKind::OpenData
                | FieldKind::Deleted
                | FieldKind::FlattenedPropertyDiscriminator { .. },
                _,
            ) => Ok(None),
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
            .deserialize_optional::<usize>(&first_arg.name(&self.schema_ctx.ontology))?
            .unwrap_or(self.default_query_limit());
        let after_cursor = args_wrapper
            .deserialize_optional::<GraphQLCursor>(&after_arg.name(&self.schema_ctx.ontology))?;

        let mut inner_select = Select::Unit;
        let mut include_total_len = false;

        for field_look_ahead in look_ahead.children() {
            let field_name = field_look_ahead.field_original_name();
            let Some(field_data) = connection_object_data.fields.get(field_name) else {
                continue;
            };
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
                let mut union_map: BTreeMap<DefId, BTreeMap<PropId, Select>> = BTreeMap::default();

                for field_look_ahead in look_ahead_children {
                    let field_name = field_look_ahead.field_original_name();

                    let applies_for = field_look_ahead.applies_for();

                    trace!("union field `{field_name}` applies for: {applies_for:?}",);

                    if let Some(applies_for) = applies_for {
                        let variant_type_data = self
                            .schema_ctx
                            .type_data_by_typename(applies_for)
                            .expect("BUG: Union variant not found");

                        let (fields, def_id) = match &variant_type_data.kind {
                            TypeKind::Object(ObjectData {
                                kind: ObjectKind::Node(NodeData { def_id, .. }),
                                fields,
                                interface: _,
                            }) => (fields, *def_id),
                            _ => panic!(
                                "Unable to extract def_id from union variant: Not an Object/Node"
                            ),
                        };

                        let Some(field_data) = fields.get(field_look_ahead.field_original_name())
                        else {
                            continue;
                        };

                        let variant_map = union_map.entry(def_id).or_default();
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
                Ok(Select::Unit)
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
                let mut properties: BTreeMap<PropId, Select> = BTreeMap::default();

                for field_look_ahead in look_ahead_children {
                    let field_name = field_look_ahead.field_original_name();
                    if let Some(applies_for) = field_look_ahead.applies_for() {
                        let type_data = self
                            .schema_ctx
                            .type_data_by_typename(applies_for)
                            .expect("BUG: interface downcast failed");
                        let TypeKind::Object(inner_object_data) = &type_data.kind else {
                            continue;
                        };

                        let Some(field_data) = inner_object_data.fields.get(field_name) else {
                            continue;
                        };

                        let Some(selection) =
                            self.analyze_selection(field_look_ahead, field_data)?
                        else {
                            continue;
                        };

                        match &field_data.kind {
                            FieldKind::FlattenedProperty { proxy, .. }
                            | FieldKind::FlattenedPropertyDiscriminator { proxy, .. } => {
                                // The proxying step is handled here.
                                // analyze_selection just looks at the inner relationship id.
                                // proxying flattened properties means that the analysis is put
                                // within the "abstracted" structure that logically exists in ONTOL.

                                let def = self.schema_ctx.ontology.def(node_data.def_id);
                                let Some(flattened_relationship) =
                                    def.data_relationships.get(proxy)
                                else {
                                    error!("Flattened relationship not found");
                                    continue;
                                };

                                let Select::Struct(inner_struct_select) = properties
                                    .entry(*proxy)
                                    // Make the proxied structure:
                                    .or_insert(Select::Struct(StructSelect {
                                        def_id: flattened_relationship.target.def_id(),
                                        properties: Default::default(),
                                    }))
                                else {
                                    panic!();
                                };

                                inner_struct_select
                                    .properties
                                    .insert(selection.key, selection.select);
                            }
                            _ => {
                                properties.insert(selection.key, selection.select);
                            }
                        }
                    } else {
                        let Some(field_data) = object_data.fields.get(field_name) else {
                            continue;
                        };

                        if let Some(selection) =
                            self.analyze_selection(field_look_ahead, field_data)?
                        {
                            properties.insert(selection.key, selection.select);
                        }
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
            | ObjectKind::Query { .. }
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
                Select::VertexAddress => return Ok(Select::VertexAddress),
                Select::Struct(object) => {
                    return Ok(Select::Entity(EntitySelect {
                        source: StructOrUnionSelect::Struct(object),
                        filter: Filter::default_for_domain(),
                        limit: Some(limit),
                        after_cursor,
                        include_total_len,
                    }));
                }
                Select::StructUnion(def_id, variants) => {
                    return Ok(Select::Entity(EntitySelect {
                        source: StructOrUnionSelect::Union(def_id, variants),
                        filter: Filter::default_for_domain(),
                        limit: Some(limit),
                        after_cursor,
                        include_total_len,
                    }));
                }
                Select::Entity(_) | Select::EntityId => panic!("Select in select"),
                Select::Unit if !empty_handled => {
                    inner_select = self.empty_entity_select(object_data)?;
                    empty_handled = true;
                }
                Select::Unit => return Ok(Select::Unit),
            }
        }
    }

    fn empty_entity_select(
        &self,
        object_data: &ObjectData,
    ) -> Result<Select, FieldError<GqlScalar>> {
        match &object_data.kind {
            ObjectKind::Edge(EdgeData { node_type_addr, .. })
            | ObjectKind::Connection(ConnectionData {
                node_type_ref: UnitTypeRef::Addr(node_type_addr),
            }) => {
                let type_data = self.schema_ctx.type_data(*node_type_addr);

                self.analyze_data(LookAheadChildren::default(), &type_data.kind)
            }
            ObjectKind::Connection(ConnectionData {
                node_type_ref: UnitTypeRef::NativeScalar(native_scalar_ref),
            }) => match &self.schema_ctx.ontology[native_scalar_ref.operator_addr] {
                SerdeOperator::Octets(octets_operator)
                    if octets_operator.target_def_id == OntolDefTag::Vertex.def_id() =>
                {
                    Ok(Select::VertexAddress)
                }
                _ => Ok(Select::Unit),
            },
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

const fn unit_property() -> PropId {
    PropId(DefId::unit(), DefPropTag(0))
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
        (existing @ Select::Unit, any_other) => {
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
