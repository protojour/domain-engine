use fnv::FnvHashMap;
use juniper::LookAheadMethods;
use ontol_runtime::{
    interface::graphql::{
        argument::FieldArg,
        data::{
            FieldData, FieldKind, NodeData, ObjectData, ObjectKind, Optionality, TypeData,
            TypeKind, TypeModifier,
        },
        schema::TypingPurpose,
    },
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    value::{Attribute, PropertyId},
    var::Var,
    DefId, MapKey, RelationshipId,
};
use smartstring::alias::String;
use tracing::debug;

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
            FieldKind::Map {
                key,
                input_operator_id,
                scalar_input_name,
                queries,
            } => {
                let input = ArgsWrapper::new(look_ahead.arguments()).domain_deserialize(
                    *input_operator_id,
                    scalar_input_name.as_deref(),
                    TypingPurpose::Selection,
                    &self.schema_ctx.ontology,
                )?;

                let mut output_selects: FnvHashMap<Var, EntitySelect> = Default::default();

                self.analyze_map(
                    look_ahead,
                    field_data,
                    unit_property(),
                    queries,
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
        output_selections: &mut FnvHashMap<Var, EntitySelect>,
    ) {
        match input_queries.get(&parent_property) {
            Some(var) => match self.analyze_selection(look_ahead, field_data).select {
                Select::Entity(entity_select) => {
                    output_selections.insert(*var, entity_select);
                }
                select => panic!("BUG: not an entity select: {select:?}"),
            },
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
            (FieldKind::Map { .. }, Ok(type_data)) => {
                match (field_data.field_type.modifier, type_data) {
                    (TypeModifier::Unit(Optionality::Optional), type_data) => {
                        KeyedPropertySelection {
                            key: unit_property(),
                            select: match self.analyze_data(look_ahead, type_data) {
                                Select::Struct(struct_select) => Select::Entity(EntitySelect {
                                    source: StructOrUnionSelect::Struct(struct_select),
                                    limit: 1,
                                    cursor: None,
                                }),
                                select => select,
                            },
                        }
                    }
                    (TypeModifier::Unit(Optionality::Mandatory), _) => {
                        panic!()
                    }
                    (
                        TypeModifier::Array(..),
                        TypeData {
                            kind: TypeKind::Object(object_data),
                            ..
                        },
                    ) => {
                        let mut select = None;

                        for field_look_ahead in look_ahead.children() {
                            let field_name = field_look_ahead.field_name();
                            let field_data = object_data.fields.get(field_name).unwrap();

                            if let FieldKind::Edges = &field_data.kind {
                                select = Some(
                                    self.analyze_selection(field_look_ahead, field_data).select,
                                );
                            }
                        }

                        KeyedPropertySelection {
                            key: unit_property(),
                            select: match select {
                                Some(Select::Struct(object)) => Select::Entity(EntitySelect {
                                    source: StructOrUnionSelect::Struct(object),
                                    limit: self.default_limit(),
                                    cursor: None,
                                }),
                                Some(Select::StructUnion(def_id, variants)) => {
                                    Select::Entity(EntitySelect {
                                        source: StructOrUnionSelect::Union(def_id, variants),
                                        limit: self.default_limit(),
                                        cursor: None,
                                    })
                                }
                                Some(Select::Entity(_) | Select::EntityId) => {
                                    panic!("Select in select")
                                }
                                Some(Select::Leaf) | None => Select::Leaf,
                            },
                        }
                    }
                    _ => todo!(),
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
                    .deserialize::<u32>(first.name())
                    .unwrap()
                    .unwrap_or(self.service_ctx.domain_engine.config().default_limit);
                let cursor = args_wrapper.deserialize::<String>(after.name()).unwrap();

                let mut select = None;

                for field_look_ahead in look_ahead.children() {
                    let field_name = field_look_ahead.field_name();
                    let field_data = object_data.fields.get(field_name).unwrap();

                    if let FieldKind::Edges = &field_data.kind {
                        select = Some(self.analyze_selection(field_look_ahead, field_data).select);
                    }
                }

                KeyedPropertySelection {
                    key: property_id.unwrap_or(unit_property()),
                    select: match select {
                        Some(Select::Struct(object)) => Select::Entity(EntitySelect {
                            source: StructOrUnionSelect::Struct(object),
                            limit,
                            cursor,
                        }),
                        Some(Select::StructUnion(def_id, variants)) => {
                            Select::Entity(EntitySelect {
                                source: StructOrUnionSelect::Union(def_id, variants),
                                limit,
                                cursor,
                            })
                        }
                        Some(Select::Entity(_) | Select::EntityId) => panic!("Select in select"),
                        Some(Select::Leaf) | None => Select::Leaf,
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
                select: self.analyze_data(look_ahead, type_data),
            },
            (FieldKind::Property(property_data), Ok(type_data)) => KeyedPropertySelection {
                key: property_data.property_id,
                select: self.analyze_data(look_ahead, type_data),
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
        type_data: &TypeData,
    ) -> Select {
        match &type_data.kind {
            TypeKind::Object(object_data) => match &object_data.kind {
                ObjectKind::Node(node_data) => {
                    let mut properties: FnvHashMap<PropertyId, Select> = FnvHashMap::default();

                    for field_look_ahead in look_ahead.children() {
                        let field_name = field_look_ahead.field_name();
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
                | ObjectKind::Connection
                | ObjectKind::Query
                | ObjectKind::Mutation => panic!("Bug in ONTOL interface schema"),
            },
            TypeKind::Union(union_data) => {
                let mut union_map: FnvHashMap<DefId, FnvHashMap<PropertyId, Select>> =
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

    fn default_limit(&self) -> u32 {
        self.service_ctx.domain_engine.config().default_limit
    }
}

const fn unit_property() -> PropertyId {
    PropertyId::subject(RelationshipId(DefId::unit()))
}
