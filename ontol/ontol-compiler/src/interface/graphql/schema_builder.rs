use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    DefId, DomainIndex, MapDefFlags, MapKey, OntolDefTag, PropId,
    interface::{
        graphql::{
            argument::{self, DefaultArg, MapInputArg},
            data::{
                EntityData, EntityMutationField, FieldData, FieldKind, MapConnectionField,
                MapFindField, NativeScalarKind, NativeScalarRef, ObjectData, ObjectInterface,
                ObjectKind, Optionality, ScalarData, TypeAddr, TypeData, TypeKind, TypeModifier,
                TypeRef, UnitTypeRef,
            },
            schema::{GraphqlSchema, QueryLevel},
        },
        serde::{
            SerdeDef, SerdeModifier,
            operator::{SerdeOperator, SerdeOperatorAddr},
        },
    },
    ontology::{
        aspects::DefsAspect,
        map::{PropertyFlow, PropertyFlowData},
        ontol::TextConstant,
    },
    resolve_path::{ProbeDirection, ProbeFilter, ProbeOptions, ResolverGraph},
    var::Var,
};
use thin_vec::thin_vec;

use crate::{
    def::{DefKind, Defs},
    interface::serde::{SerdeKey, serde_generator::SerdeGenerator},
    misc::MiscCtx,
    phf_build::build_phf_index_map,
    properties::PropCtx,
    relation::{RelCtx, UnionMemberCache},
    repr::{repr_ctx::ReprCtx, repr_model::ReprKind},
};

use super::graphql_namespace::{GraphqlNamespace, NameCtx};

pub(super) struct SchemaBuilder<'a, 's, 'c, 'm> {
    /// Avoid deep recursion by pushing tasks to be performed later to this task list
    pub lazy_tasks: Vec<LazyTask>,
    /// The schema being generated
    pub schema: &'s mut GraphqlSchema,
    /// Tool to ensure global type names are unique
    pub type_namespace: &'s mut GraphqlNamespace<'a>,
    /// The partial ontology containing TypeInfo (does not yet have SerdeOperators)
    pub ontology_defs: &'a DefsAspect,
    /// Serde generator for generating new serialization operators
    pub serde_gen: &'a mut SerdeGenerator<'c, 'm>,
    /// The compiler's relations
    pub rel_ctx: &'c RelCtx,
    /// The compiler's properties
    pub prop_ctx: &'c PropCtx,
    /// Other data
    pub misc_ctx: &'c MiscCtx,
    /// The compiler's defs
    pub defs: &'c Defs<'m>,
    /// representation of concrete types
    pub repr_ctx: &'c ReprCtx,
    /// A resolver graph
    pub resolver_graph: &'c ResolverGraph,
    /// cache of which def is member of which unions
    pub union_member_cache: &'c UnionMemberCache,

    pub builtin_scalars: FnvHashMap<TextConstant, TypeAddr>,
}

pub(super) enum LazyTask {
    HarvestFields {
        type_addr: TypeAddr,
        def_id: DefId,
        property_field_producer: PropertyFieldProducer,
    },
}

#[derive(Clone, Copy)]
pub(super) enum PropertyFieldProducer {
    Property,
    FlattenedProperty(PropId),
    EdgeProperty,
}

impl PropertyFieldProducer {
    pub fn make_property(&self, prop_id: PropId, addr: SerdeOperatorAddr) -> FieldKind {
        match self {
            Self::Property => FieldKind::Property { id: prop_id, addr },
            Self::FlattenedProperty(proxy_id) => FieldKind::FlattenedProperty {
                proxy: *proxy_id,
                id: prop_id,
                addr,
            },
            Self::EdgeProperty => FieldKind::EdgeProperty { id: prop_id, addr },
        }
    }
}

pub(super) enum NewType {
    TypeData(TypeData, NewTypeActions),
    NativeScalar(NativeScalarRef),
}

#[derive(Default)]
pub(super) struct NewTypeActions {
    pub harvest_fields: Option<(DefId, PropertyFieldProducer)>,
}

/// An extension of QueryLevel used only inside the generator
#[derive(Clone, Copy)]
pub(super) enum QLevel {
    Node,
    Edge {
        rel_params: Option<(DefId, SerdeOperatorAddr)>,
    },
    Connection {
        rel_params: Option<(DefId, SerdeOperatorAddr)>,
    },
    MutationResult,
}

impl QLevel {
    pub fn as_query_level(&self) -> QueryLevel {
        match self {
            Self::Node => QueryLevel::Node,
            Self::Edge { rel_params } => QueryLevel::Edge {
                rel_params: rel_params.map(|(_, op_id)| op_id),
            },
            Self::Connection { rel_params } => QueryLevel::Connection {
                rel_params: rel_params.map(|(_, op_id)| op_id),
            },
            Self::MutationResult => QueryLevel::MutationResult,
        }
    }
}

impl<'c, 'm> SchemaBuilder<'_, '_, 'c, 'm> {
    pub fn exec_lazy_tasks(&mut self) {
        while !self.lazy_tasks.is_empty() {
            for lazy_task in std::mem::take(&mut self.lazy_tasks) {
                self.exec_lazy_task(lazy_task);
            }
        }
    }

    fn exec_lazy_task(&mut self, task: LazyTask) {
        match task {
            LazyTask::HarvestFields {
                type_addr,
                def_id,
                property_field_producer,
            } => {
                self.harvest_fields(type_addr, def_id, property_field_producer);
            }
        }
    }

    pub fn register_fundamental_types(
        &mut self,
        domain_index: DomainIndex,
        ontology_defs: &'c DefsAspect,
    ) {
        self.schema.query = self.schema.push_type_data(TypeData {
            typename: self.serde_gen.str_ctx.intern_constant("Query"),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Query {
                    domain_def_id: ontology_defs
                        .domain_by_index(domain_index)
                        .unwrap()
                        .def_id(),
                },
                interface: ObjectInterface::Implements(thin_vec![]),
            }),
        });

        self.schema.mutation = self.schema.push_type_data(TypeData {
            typename: self.serde_gen.str_ctx.intern_constant("Mutation"),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Mutation,
                interface: ObjectInterface::Implements(thin_vec![]),
            }),
        });

        {
            self.schema.page_info = self.schema.push_type_data(TypeData {
                typename: self.serde_gen.str_ctx.intern_constant("PageInfo"),
                input_typename: None,
                partial_input_typename: None,
                kind: TypeKind::Object(ObjectData {
                    fields: Default::default(),
                    kind: ObjectKind::PageInfo,
                    interface: ObjectInterface::Implements(thin_vec![]),
                }),
            });
            let data = object_data_mut(self.schema.page_info, self.schema);
            data.fields = build_phf_index_map([
                (
                    self.serde_gen.str_ctx.make_phf_key("endCursor"),
                    FieldData {
                        kind: FieldKind::PageInfo,
                        field_type: TypeRef {
                            modifier: TypeModifier::Unit(Optionality::Optional),
                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                operator_addr: self
                                    .serde_gen
                                    .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                                        OntolDefTag::Text.def_id(),
                                        SerdeModifier::NONE,
                                    )))
                                    .unwrap(),
                                kind: NativeScalarKind::String,
                            }),
                        },
                    },
                ),
                (
                    self.serde_gen.str_ctx.make_phf_key("hasNextPage"),
                    FieldData {
                        kind: FieldKind::PageInfo,
                        field_type: TypeRef {
                            modifier: TypeModifier::Unit(Optionality::Mandatory),
                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                operator_addr: self
                                    .serde_gen
                                    .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                                        OntolDefTag::Boolean.def_id(),
                                        SerdeModifier::NONE,
                                    )))
                                    .unwrap(),
                                kind: NativeScalarKind::Boolean,
                            }),
                        },
                    },
                ),
            ]);
        }

        self.schema.json_scalar = self.schema.push_type_data(TypeData {
            typename: self
                .serde_gen
                .str_ctx
                .intern_constant(&self.type_namespace.unique_literal("_ontol_json")),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::CustomScalar(ScalarData {
                operator_addr: SerdeOperatorAddr(0),
            }),
        });
    }

    pub fn register_standard_queries(
        &mut self,
        fields: &mut IndexMap<std::string::String, FieldData>,
    ) {
        fields.insert(
            "_version".into(),
            FieldData {
                kind: FieldKind::Version,
                field_type: TypeRef {
                    modifier: TypeModifier::Unit(Optionality::Mandatory),
                    unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                        operator_addr: self
                            .serde_gen
                            .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                                OntolDefTag::Text.def_id(),
                                SerdeModifier::NONE,
                            )))
                            .unwrap(),
                        kind: NativeScalarKind::String,
                    }),
                },
            },
        );
    }

    pub fn gen_def_type_ref(&mut self, def_id: DefId, level: QLevel) -> UnitTypeRef {
        if let Some(type_addr) = self
            .schema
            .type_addr_by_def
            .get(&(def_id, level.as_query_level()))
        {
            return UnitTypeRef::Addr(*type_addr);
        }

        match self.make_def_type(def_id, level) {
            NewType::TypeData(type_data, post_alloc) => {
                let type_addr = match &type_data.kind {
                    TypeKind::CustomScalar(_scalar_data) => {
                        match self.builtin_scalars.get(&type_data.typename) {
                            Some(type_addr) => *type_addr,
                            None => {
                                let typename = type_data.typename;
                                let type_addr = self.schema.push_type_data(type_data);
                                self.builtin_scalars.insert(typename, type_addr);

                                if &self.serde_gen.str_ctx[typename] == "_ontol_i64" {
                                    self.schema.i64_custom_scalar = Some(type_addr);
                                }

                                type_addr
                            }
                        }
                    }
                    _ => {
                        let type_addr = self.schema.push_type_data(type_data);

                        if let Some((def_id, property_field_producer)) = post_alloc.harvest_fields {
                            self.lazy_tasks.push(LazyTask::HarvestFields {
                                type_addr,
                                def_id,
                                property_field_producer,
                            })
                        }
                        type_addr
                    }
                };

                self.schema
                    .type_addr_by_def
                    .insert((def_id, level.as_query_level()), type_addr);

                UnitTypeRef::Addr(type_addr)
            }
            NewType::NativeScalar(scalar_ref) => UnitTypeRef::NativeScalar(scalar_ref),
        }
    }

    pub fn add_named_map_query(
        &mut self,
        name: &str,
        map_key: MapKey,
        prop_flow: &[PropertyFlow],
        fields: &mut IndexMap<std::string::String, FieldData>,
    ) {
        let input_serde_key = {
            let mut serde_modifier = SerdeModifier::graphql_default();

            if map_key.input.flags.contains(MapDefFlags::SEQUENCE) {
                serde_modifier.insert(SerdeModifier::LIST);
            }

            SerdeKey::Def(SerdeDef::new(map_key.input.def_id, serde_modifier))
        };

        let input_operator_addr = self.serde_gen.gen_addr_greedy(input_serde_key).unwrap();

        let queries: FnvHashMap<PropId, Var> = prop_flow
            .iter()
            .filter_map(|prop_flow| {
                if let PropertyFlowData::Match(var) = &prop_flow.data {
                    Some((prop_flow.id, *var))
                } else {
                    None
                }
            })
            .collect();

        let input_arg = match self.serde_gen.repr_ctx.get_repr_kind(&map_key.input.def_id) {
            Some(ReprKind::Scalar(..) | ReprKind::FmtStruct(..)) => {
                let scalar_input_name: String =
                    match self.serde_gen.defs.def_kind(map_key.input.def_id) {
                        DefKind::Type(type_def) => match type_def.ident {
                            Some(ident) => ident.into(),
                            None => return,
                        },
                        _ => return,
                    };

                MapInputArg {
                    operator_addr: input_operator_addr,
                    scalar_input_name: Some(
                        self.serde_gen.str_ctx.intern_constant(&scalar_input_name),
                    ),
                    default_arg: None,
                    hidden: false,
                }
            }
            _ => {
                let _unit_type_ref = self.gen_def_type_ref(map_key.input.def_id, QLevel::Node);
                let mut hidden = false;

                let default_arg = match self.serde_gen.get_operator(input_operator_addr) {
                    SerdeOperator::Struct(struct_op) => {
                        hidden = struct_op.is_struct_properties_empty();

                        let all_optional = struct_op
                            .properties
                            .iter()
                            .all(|(_, prop)| prop.is_optional());

                        if all_optional {
                            Some(DefaultArg::EmptyObject)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };

                MapInputArg {
                    operator_addr: input_operator_addr,
                    scalar_input_name: None,
                    default_arg,
                    hidden,
                }
            }
        };

        let field_data = if map_key.output.flags.contains(MapDefFlags::SEQUENCE) {
            FieldData {
                kind: FieldKind::MapConnection(Box::new(MapConnectionField {
                    map_key,
                    queries,
                    input_arg,
                    first_arg: argument::FirstArg,
                    after_arg: argument::AfterArg,
                })),
                field_type: TypeRef {
                    modifier: TypeModifier::Unit(Optionality::Mandatory),
                    unit: self.gen_def_type_ref(
                        map_key.output.def_id,
                        QLevel::Connection { rel_params: None },
                    ),
                },
            }
        } else {
            FieldData {
                kind: FieldKind::MapFind(Box::new(MapFindField {
                    map_key,
                    queries,
                    input_arg,
                })),
                field_type: TypeRef {
                    modifier: TypeModifier::Unit(Optionality::Optional),
                    unit: self.gen_def_type_ref(map_key.output.def_id, QLevel::Node),
                },
            }
        };

        fields.insert(name.into(), field_data);
    }

    pub fn add_entity_queries_and_mutations(
        &mut self,
        entity_data: EntityData,
        fields: &mut IndexMap<std::string::String, FieldData>,
    ) {
        let def = self.ontology_defs.def(entity_data.node_def_id);

        let mutation_result_ref =
            self.gen_def_type_ref(entity_data.node_def_id, QLevel::MutationResult);

        let mut mutation_namespace = GraphqlNamespace::default();

        // This is here for a reason
        let _id_unit_type_ref = self.gen_def_type_ref(entity_data.id_def_id, QLevel::Node);

        let entity_array_operator_addr = self
            .serde_gen
            .gen_addr_lazy(gql_list_serde_key(entity_data.node_def_id))
            .unwrap();

        let [up_path, create_down_path, update_down_path] = [
            (ProbeDirection::Up, ProbeFilter::Complete),
            // The CREATE resolve path must be PERFECT | PURE for create to be available
            (ProbeDirection::Down, ProbeFilter::Complete),
            // The UPDATE resolve path must be PURE (but not perfect) for update to be available
            (ProbeDirection::Down, ProbeFilter::Pure),
        ]
        .map(|(direction, filter)| {
            self.resolver_graph.probe_path(
                self.ontology_defs,
                def.id,
                ProbeOptions {
                    must_be_entity: true,
                    direction,
                    filter,
                },
            )
        });

        if up_path.is_none() && create_down_path.is_none() && update_down_path.is_none() {
            return;
        }

        fields.insert(
            mutation_namespace.typename(
                entity_data.node_def_id,
                NameCtx {
                    str_ctx: self.serde_gen.str_ctx,
                    defs: self.serde_gen.defs,
                },
            ),
            FieldData {
                kind: FieldKind::EntityMutation(Box::new(EntityMutationField {
                    def_id: def.id,
                    create_arg: create_down_path.is_some().then_some(
                        argument::EntityCreateInputsArg {
                            type_addr: entity_data.type_addr,
                            def_id: entity_data.node_def_id,
                            operator_addr: entity_array_operator_addr,
                        },
                    ),
                    update_arg: update_down_path.is_some().then_some(
                        argument::EntityUpdateInputsArg {
                            type_addr: entity_data.type_addr,
                            def_id: entity_data.node_def_id,
                            operator_addr: entity_array_operator_addr,
                        },
                    ),
                    delete_arg: up_path
                        .is_some()
                        .then_some(argument::EntityDeleteInputsArg {
                            def_id: entity_data.id_def_id,
                            operator_addr: self
                                .serde_gen
                                .gen_addr_lazy(gql_list_serde_key(entity_data.id_def_id))
                                .unwrap(),
                        }),
                    field_unit_type_addr: match mutation_result_ref {
                        UnitTypeRef::Addr(addr) => addr,
                        UnitTypeRef::NativeScalar(_) => unreachable!(),
                    },
                })),
                field_type: TypeRef::mandatory(mutation_result_ref)
                    .to_array(Optionality::Mandatory),
            },
        );
    }

    pub fn mk_typename<S: AsRef<str>>(
        &mut self,
        f: impl Fn(&mut GraphqlNamespace, NameCtx<'_, 'm>) -> S,
    ) -> TextConstant {
        let string = f(
            self.type_namespace,
            NameCtx {
                str_ctx: self.serde_gen.str_ctx,
                defs: self.serde_gen.defs,
            },
        );
        self.serde_gen.str_ctx.intern_constant(string.as_ref())
    }

    pub fn set_object_fields(
        &mut self,
        address: TypeAddr,
        fields: IndexMap<std::string::String, FieldData>,
    ) {
        object_data_mut(address, self.schema).fields = build_phf_index_map(
            fields
                .into_iter()
                .map(|(key, data)| (self.serde_gen.str_ctx.make_phf_key(&key), data)),
        );
    }
}

pub fn object_data_mut(index: TypeAddr, schema: &mut GraphqlSchema) -> &mut ObjectData {
    let type_data = schema.types.get_mut(index.0 as usize).unwrap();
    match &mut type_data.kind {
        TypeKind::Object(object_data) => object_data,
        _ => panic!("{index:?} is not an object"),
    }
}

pub(super) fn gql_serde_key(def_id: DefId) -> SerdeKey {
    SerdeKey::Def(SerdeDef::new(def_id, SerdeModifier::graphql_default()))
}

pub(super) fn gql_list_serde_key(def_id: DefId) -> SerdeKey {
    SerdeKey::Def(SerdeDef::new(
        def_id,
        SerdeModifier::graphql_default() | SerdeModifier::LIST,
    ))
}
