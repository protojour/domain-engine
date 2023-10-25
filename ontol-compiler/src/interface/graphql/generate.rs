use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    interface::serde::{
        operator::{SerdeOperator, SerdeOperatorAddr},
        SerdeDef, SerdeKey,
    },
    interface::{
        graphql::{
            argument::{self, DefaultArg, MapInputArg},
            data::{
                ConnectionData, EdgeData, EntityData, FieldData, FieldKind, NativeScalarKind,
                NativeScalarRef, NodeData, ObjectData, ObjectKind, Optionality, PropertyData,
                ScalarData, TypeAddr, TypeData, TypeKind, TypeModifier, TypeRef, UnionData,
                UnitTypeRef,
            },
            schema::{GraphqlSchema, QueryLevel, TypingPurpose},
        },
        serde::SerdeModifier,
    },
    ontology::{Ontology, PropertyCardinality, PropertyFlow, PropertyFlowData, ValueCardinality},
    smart_format,
    value::PropertyId,
    var::Var,
    DefId, MapKey, PackageId, Role,
};
use smartstring::alias::String;
use tracing::trace;

use crate::{
    codegen::task::CodegenTasks,
    def::{DefKind, Defs, LookupRelationshipMeta, RelParams},
    interface::serde::serde_generator::SerdeGenerator,
    primitive::Primitives,
    relation::{Properties, Relations},
    type_check::{
        repr::repr_model::{ReprKind, ReprScalarKind},
        seal::SealCtx,
    },
};

use super::graphql_namespace::{DomainDisambiguation, GraphqlNamespace};

pub fn generate_graphql_schema<'c>(
    package_id: PackageId,
    partial_ontology: &'c Ontology,
    primitives: &'c Primitives,
    map_namespace: Option<&'c IndexMap<&str, DefId>>,
    codegen_tasks: &'c CodegenTasks,
    serde_generator: &mut SerdeGenerator<'c, '_>,
) -> Option<GraphqlSchema> {
    let domain = partial_ontology.find_domain(package_id).unwrap();

    if !domain
        .type_names
        .iter()
        .any(|(_, def_id)| domain.type_info(*def_id).entity_info.is_some())
    {
        // A domain without entities doesn't get a GraphQL schema.
        return None;
    }

    let mut schema = new_schema_with_capacity(package_id, domain.type_names.len());
    let mut namespace = GraphqlNamespace::with_domain_disambiguation(DomainDisambiguation {
        root_domain: package_id,
        ontology: partial_ontology,
    });
    let mut builder = {
        let relations = serde_generator.relations;
        let defs = serde_generator.defs;
        let seal_ctx = serde_generator.seal_ctx;
        Builder {
            lazy_tasks: vec![],
            schema: &mut schema,
            namespace: &mut namespace,
            partial_ontology,
            serde_generator,
            relations,
            defs,
            primitives,
            seal_ctx,
        }
    };

    builder.register_fundamental_types();

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);
        if !type_info.public {
            continue;
        }

        if type_info.operator_addr.is_some() {
            trace!("adapt type `{name:?}`", name = type_info.name);

            let type_ref = builder.get_def_type_ref(type_info.def_id, QLevel::Node);

            if let Some(entity_data) = entity_check(builder.schema, type_ref) {
                builder.add_entity_queries_and_mutations(entity_data);
            }
        }
    }

    if let Some(map_namespace) = map_namespace {
        // Register named maps in the user-specified order (using the IndexMap from the namespace)
        for name in map_namespace.keys() {
            if let Some(map_key) = codegen_tasks
                .result_named_forward_maps
                .get(&(package_id, (*name).into()))
            {
                let prop_flow = codegen_tasks.result_propflow_table.get(map_key).unwrap();

                builder.add_named_map_query(name, map_key, prop_flow);
            }
        }
    }

    while !builder.lazy_tasks.is_empty() {
        for lazy_task in std::mem::take(&mut builder.lazy_tasks) {
            builder.exec_lazy_task(lazy_task);
        }
    }

    Some(schema)
}

fn entity_check(schema: &GraphqlSchema, type_ref: UnitTypeRef) -> Option<EntityData> {
    if let UnitTypeRef::Addr(type_addr) = type_ref {
        let type_data = &schema.type_data(type_addr);

        if let TypeData {
            kind:
                TypeKind::Object(ObjectData {
                    kind:
                        ObjectKind::Node(NodeData {
                            def_id: node_def_id,
                            entity_id: Some(id_def_id),
                            ..
                        }),
                    ..
                }),
            ..
        } = type_data
        {
            Some(EntityData {
                type_addr,
                node_def_id: *node_def_id,
                id_def_id: *id_def_id,
            })
        } else {
            None
        }
    } else {
        None
    }
}

enum LazyTask {
    HarvestFields {
        type_addr: TypeAddr,
        def_id: DefId,
        property_field_producer: PropertyFieldProducer,
        is_entrypoint: bool,
    },
}

enum NewType {
    Addr(TypeAddr, TypeData),
    NativeScalar(NativeScalarRef),
}

struct Builder<'a, 's, 'c, 'm> {
    /// Avoid deep recursion by pushing tasks to be performed later to this task list
    lazy_tasks: Vec<LazyTask>,
    /// The schema being generated
    schema: &'s mut GraphqlSchema,
    /// Tool to ensure global type names are unique
    namespace: &'s mut GraphqlNamespace<'a>,
    /// The partial ontology containing TypeInfo (does not yet have SerdeOperators)
    partial_ontology: &'a Ontology,
    /// Serde generator for generating new serialization operators
    serde_generator: &'a mut SerdeGenerator<'c, 'm>,
    /// The compiler's relations
    relations: &'c Relations,
    /// The compiler's defs
    defs: &'c Defs<'m>,
    /// The compiler's primitives
    primitives: &'c Primitives,
    /// The compiler's sealed type information
    seal_ctx: &'c SealCtx,
}

impl<'a, 's, 'c, 'm> Builder<'a, 's, 'c, 'm> {
    fn exec_lazy_task(&mut self, task: LazyTask) {
        match task {
            LazyTask::HarvestFields {
                type_addr,
                def_id,
                property_field_producer,
                is_entrypoint,
            } => {
                let Some(properties) = self.relations.properties_by_def_id(def_id) else {
                    return;
                };
                let mut fields = Default::default();

                trace!("Harvest fields for {def_id:?} / {type_addr:?}");

                if is_entrypoint {
                    let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");
                    if let ReprKind::StructIntersection(members) = repr_kind {
                        for (member_def_id, _) in members {
                            self.lazy_tasks.push(LazyTask::HarvestFields {
                                type_addr,
                                def_id: *member_def_id,
                                property_field_producer,
                                is_entrypoint: false,
                            });
                        }
                    }
                }

                self.harvest_struct_fields(
                    properties,
                    &mut fields,
                    property_field_producer,
                    &mut GraphqlNamespace::default(),
                );

                match &mut self.schema.types[type_addr.0 as usize].kind {
                    TypeKind::Object(object_data) => {
                        object_data.fields.extend(fields);
                    }
                    _ => panic!(),
                }
            }
        }
    }

    fn register_fundamental_types(&mut self) {
        self.schema.query = self.next_type_addr();
        self.schema.types.push(TypeData {
            typename: "Query".into(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Query,
            }),
        });

        self.schema.mutation = self.next_type_addr();
        self.schema.types.push(TypeData {
            typename: "Mutation".into(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Mutation,
            }),
        });

        {
            self.schema.page_info = self.next_type_addr();
            self.schema.types.push(TypeData {
                typename: "PageInfo".into(),
                input_typename: None,
                partial_input_typename: None,
                kind: TypeKind::Object(ObjectData {
                    fields: Default::default(),
                    kind: ObjectKind::PageInfo,
                }),
            });
            let data = object_data_mut(self.schema.page_info, self.schema);
            data.fields.insert(
                "endCursor".into(),
                FieldData {
                    kind: FieldKind::PageInfo,
                    field_type: TypeRef {
                        modifier: TypeModifier::Unit(Optionality::Optional),
                        unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_addr: self
                                .serde_generator
                                .gen_addr(SerdeKey::Def(SerdeDef::new(
                                    self.primitives.text,
                                    SerdeModifier::NONE,
                                )))
                                .unwrap(),
                            kind: NativeScalarKind::String,
                        }),
                    },
                },
            );
            data.fields.insert(
                "hasNextPage".into(),
                FieldData {
                    kind: FieldKind::PageInfo,
                    field_type: TypeRef {
                        modifier: TypeModifier::Unit(Optionality::Mandatory),
                        unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_addr: self
                                .serde_generator
                                .gen_addr(SerdeKey::Def(SerdeDef::new(
                                    self.primitives.bool,
                                    SerdeModifier::NONE,
                                )))
                                .unwrap(),
                            kind: NativeScalarKind::Boolean,
                        }),
                    },
                },
            );
        }

        self.schema.json_scalar = self.next_type_addr();
        self.schema.types.push(TypeData {
            typename: self.namespace.unique_literal("_ontol_json"),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::CustomScalar(ScalarData {
                operator_addr: SerdeOperatorAddr(0),
            }),
        });
    }

    pub fn get_def_type_ref(&mut self, def_id: DefId, level: QLevel) -> UnitTypeRef {
        if let Some(type_addr) = self
            .schema
            .type_addr_by_def
            .get(&(def_id, level.as_query_level()))
        {
            return UnitTypeRef::Addr(*type_addr);
        }

        match self.make_def_type(def_id, level) {
            NewType::Addr(type_addr, type_data) => {
                self.schema.types[type_addr.0 as usize] = type_data;
                UnitTypeRef::Addr(type_addr)
            }
            NewType::NativeScalar(scalar_ref) => UnitTypeRef::NativeScalar(scalar_ref),
        }
    }

    fn next_type_addr(&self) -> TypeAddr {
        TypeAddr(self.schema.types.len() as u32)
    }

    fn alloc_def_type_addr(&mut self, def_id: DefId, level: QLevel) -> TypeAddr {
        let index = self.next_type_addr();
        // note: this will be overwritten later
        self.schema.types.push(TypeData {
            typename: String::new(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::CustomScalar(ScalarData {
                operator_addr: SerdeOperatorAddr(0),
            }),
        });
        self.schema
            .type_addr_by_def
            .insert((def_id, level.as_query_level()), index);
        index
    }

    fn make_def_type(&mut self, def_id: DefId, level: QLevel) -> NewType {
        match level {
            QLevel::Node => self.make_node_type(def_id),
            QLevel::Edge { rel_params } => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let edge_addr = self.alloc_def_type_addr(def_id, level);
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);
                let node_type_addr = node_ref.unwrap_addr();

                let mut field_namespace = GraphqlNamespace::default();

                // FIXME: what if some of the relation data's fields are called "node"
                let fields: IndexMap<String, FieldData> = [(
                    field_namespace.unique_literal("node"),
                    FieldData {
                        kind: FieldKind::Node,
                        field_type: TypeRef::mandatory(node_ref),
                    },
                )]
                .into();

                let node_operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(type_info.def_id))
                    .unwrap();

                if let Some((rel_def_id, _operator_addr)) = rel_params {
                    let rel_type_info = self.partial_ontology.get_type_info(rel_def_id);
                    let rel_edge_ref = self.get_def_type_ref(rel_def_id, QLevel::Node);

                    let typename = self.namespace.edge(Some(rel_type_info), type_info);

                    self.lazy_tasks.push(LazyTask::HarvestFields {
                        type_addr: edge_addr,
                        def_id: rel_def_id,
                        property_field_producer: PropertyFieldProducer::EdgeProperty,
                        is_entrypoint: true,
                    });

                    NewType::Addr(
                        edge_addr,
                        TypeData {
                            typename,
                            input_typename: Some(
                                self.namespace.edge_input(Some(rel_type_info), type_info),
                            ),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_type_addr,
                                    node_operator_addr,
                                    rel_edge_ref: Some(rel_edge_ref),
                                }),
                            }),
                        },
                    )
                } else {
                    NewType::Addr(
                        edge_addr,
                        TypeData {
                            typename: self.namespace.edge(None, type_info),
                            input_typename: Some(self.namespace.edge_input(None, type_info)),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_type_addr,
                                    node_operator_addr,
                                    rel_edge_ref: None,
                                }),
                            }),
                        },
                    )
                }
            }
            QLevel::Connection { rel_params } => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let connection_index = self.alloc_def_type_addr(def_id, level);
                let edge_ref = self.get_def_type_ref(def_id, QLevel::Edge { rel_params });
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);
                let node_type_addr = node_ref.unwrap_addr();

                NewType::Addr(
                    connection_index,
                    TypeData {
                        typename: self.namespace.connection(type_info),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: [
                                (
                                    smart_format!("nodes"),
                                    FieldData {
                                        kind: FieldKind::Nodes,
                                        field_type: TypeRef::mandatory(node_ref)
                                            .to_array(Optionality::Optional),
                                    },
                                ),
                                (
                                    smart_format!("edges"),
                                    FieldData {
                                        kind: FieldKind::Edges,
                                        field_type: TypeRef::mandatory(edge_ref)
                                            .to_array(Optionality::Optional),
                                    },
                                ),
                                (
                                    smart_format!("pageInfo"),
                                    FieldData {
                                        kind: FieldKind::PageInfo,
                                        field_type: TypeRef::mandatory(UnitTypeRef::Addr(
                                            self.schema.page_info,
                                        )),
                                    },
                                ),
                            ]
                            .into(),
                            kind: ObjectKind::Connection(ConnectionData { node_type_addr }),
                        }),
                    },
                )
            }
        }
    }

    fn make_node_type(&mut self, def_id: DefId) -> NewType {
        if self.relations.properties_by_def_id(def_id).is_none() {
            return NewType::NativeScalar(NativeScalarRef {
                operator_addr: self
                    .serde_generator
                    .gen_addr(gql_serde_key(DefId::unit()))
                    .unwrap(),
                kind: NativeScalarKind::Unit,
            });
        }

        let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");

        match repr_kind {
            ReprKind::Unit | ReprKind::Struct | ReprKind::StructIntersection(_) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let type_addr = self.alloc_def_type_addr(def_id, QLevel::Node);

                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();

                self.lazy_tasks.push(LazyTask::HarvestFields {
                    type_addr,
                    def_id: type_info.def_id,
                    property_field_producer: PropertyFieldProducer::Property,
                    is_entrypoint: true,
                });

                let type_kind = TypeKind::Object(ObjectData {
                    fields: Default::default(),
                    kind: ObjectKind::Node(NodeData {
                        def_id: type_info.def_id,
                        entity_id: type_info
                            .entity_info
                            .as_ref()
                            .map(|entity_info| entity_info.id_value_def_id),
                        operator_addr,
                    }),
                });

                NewType::Addr(
                    type_addr,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.input(type_info)),
                        partial_input_typename: Some(self.namespace.partial_input(type_info)),
                        kind: type_kind,
                    },
                )
            }
            ReprKind::Union(variants) | ReprKind::StructUnion(variants) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let node_index = self.alloc_def_type_addr(def_id, QLevel::Node);

                let mut needs_scalar = false;
                let mut type_variants = vec![];

                for (variant_def_id, _) in variants {
                    match self.seal_ctx.get_repr_kind(variant_def_id) {
                        Some(ReprKind::Scalar(..)) => {
                            needs_scalar = true;
                            break;
                        }
                        _ => match self.get_def_type_ref(*variant_def_id, QLevel::Node) {
                            UnitTypeRef::Addr(type_addr) => {
                                type_variants.push(type_addr);
                            }
                            UnitTypeRef::NativeScalar(_) => {
                                needs_scalar = true;
                                break;
                            }
                        },
                    }
                }

                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();

                NewType::Addr(
                    node_index,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.union_input(type_info)),
                        partial_input_typename: Some(self.namespace.union_partial_input(type_info)),
                        kind: if needs_scalar {
                            TypeKind::CustomScalar(ScalarData { operator_addr })
                        } else {
                            TypeKind::Union(UnionData {
                                union_def_id: type_info.def_id,
                                variants: type_variants,
                                operator_addr,
                            })
                        },
                    },
                )
            }
            ReprKind::Seq | ReprKind::Intersection(_) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();
                let type_addr = self.alloc_def_type_addr(def_id, QLevel::Node);

                NewType::Addr(
                    type_addr,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.input(type_info)),
                        partial_input_typename: Some(self.namespace.partial_input(type_info)),
                        kind: TypeKind::CustomScalar(ScalarData { operator_addr }),
                    },
                )
            }
            ReprKind::Scalar(scalar_def_id, ReprScalarKind::I64(range), _) => {
                if range.start() >= &i32::MIN.into() && range.end() <= &i32::MAX.into() {
                    NewType::NativeScalar(NativeScalarRef {
                        operator_addr: self
                            .serde_generator
                            .gen_addr(gql_serde_key(def_id))
                            .unwrap(),
                        kind: NativeScalarKind::Int(*scalar_def_id),
                    })
                } else {
                    let type_info = self.partial_ontology.get_type_info(def_id);
                    let type_addr = self.alloc_def_type_addr(type_info.def_id, QLevel::Node);
                    self.schema.i64_custom_scalar = Some(type_addr);
                    NewType::Addr(
                        type_addr,
                        TypeData {
                            // FIXME: Must make sure that domain typenames take precedence over generated ones
                            typename: smart_format!("_ontol_i64"),
                            input_typename: None,
                            partial_input_typename: None,
                            kind: TypeKind::CustomScalar(ScalarData {
                                operator_addr: self
                                    .serde_generator
                                    .gen_addr(gql_serde_key(def_id))
                                    .unwrap(),
                            }),
                        },
                    )
                }
            }
            ReprKind::Scalar(def_id, ReprScalarKind::F64(_), _) => {
                NewType::NativeScalar(NativeScalarRef {
                    operator_addr: self
                        .serde_generator
                        .gen_addr(gql_serde_key(*def_id))
                        .unwrap(),
                    kind: NativeScalarKind::Number(*def_id),
                })
            }
            ReprKind::Scalar(..) => {
                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();

                NewType::NativeScalar(NativeScalarRef {
                    operator_addr,
                    kind: get_native_scalar_kind(
                        self.serde_generator,
                        self.serde_generator.get_operator(operator_addr),
                    ),
                })
            }
        }
    }

    fn harvest_struct_fields(
        &mut self,
        properties: &Properties,
        fields: &mut IndexMap<String, FieldData>,
        property_field_producer: PropertyFieldProducer,
        field_namespace: &mut GraphqlNamespace,
    ) {
        let Some(table) = &properties.table else {
            return;
        };

        for (property_id, property) in table {
            let property_id = *property_id;
            let meta = self.defs.relationship_meta(property_id.relationship_id);
            let (_, (prop_cardinality, value_cardinality), _) =
                meta.relationship.by(property_id.role);
            let (value_def_id, ..) = meta.relationship.by(property_id.role.opposite());
            let prop_key = match property_id.role {
                Role::Subject => {
                    let DefKind::TextLiteral(prop_key) = meta.relation_def_kind.value else {
                        panic!("Subject property is not a string literal");
                    };
                    *prop_key
                }
                Role::Object => meta
                    .relationship
                    .object_prop
                    .expect("Object property has no name"),
            };

            trace!("    register struct field `{prop_key}`: {property_id}");

            let value_properties = self.relations.properties_by_def_id(value_def_id);

            let is_entity_value = {
                let repr_kind = self.seal_ctx.get_repr_kind(&value_def_id);
                match repr_kind {
                    Some(ReprKind::StructUnion(variants)) => {
                        variants.iter().all(|(variant_def_id, _)| {
                            let variant_properties =
                                self.relations.properties_by_def_id(*variant_def_id);
                            variant_properties
                                .map(|properties| properties.identified_by.is_some())
                                .unwrap_or(false)
                        })
                    }
                    _ => value_properties
                        .map(|properties| properties.identified_by.is_some())
                        .unwrap_or(false),
                }
            };

            let field_data = if matches!(value_cardinality, ValueCardinality::One) {
                let modifier = TypeModifier::new_unit(Optionality::from_optional(matches!(
                    prop_cardinality,
                    PropertyCardinality::Optional
                )));

                let value_operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(value_def_id))
                    .unwrap();

                let field_type = if property.is_entity_id {
                    TypeRef {
                        modifier,
                        unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_addr: value_operator_addr,
                            kind: NativeScalarKind::ID,
                        }),
                    }
                } else {
                    let qlevel = match meta.relationship.rel_params {
                        RelParams::Unit => QLevel::Node,
                        RelParams::Type(rel_def_id) => {
                            let operator_addr = self
                                .serde_generator
                                .gen_addr(gql_serde_key(rel_def_id))
                                .unwrap();

                            QLevel::Edge {
                                rel_params: Some((rel_def_id, operator_addr)),
                            }
                        }
                        RelParams::IndexRange(_) => todo!(),
                    };
                    TypeRef {
                        modifier,
                        unit: self.get_def_type_ref(value_def_id, qlevel),
                    }
                };

                FieldData {
                    kind: property_field_producer.make_property(PropertyData {
                        property_id,
                        value_operator_addr,
                    }),
                    field_type,
                }
            } else if is_entity_value {
                let connection_ref = {
                    let rel_params = match meta.relationship.rel_params {
                        RelParams::Unit => None,
                        RelParams::Type(rel_def_id) => Some((
                            rel_def_id,
                            self.serde_generator
                                .gen_addr(gql_serde_key(rel_def_id))
                                .unwrap(),
                        )),
                        RelParams::IndexRange(_) => todo!(),
                    };

                    trace!("    connection/edge rel params {rel_params:?}");

                    self.get_def_type_ref(value_def_id, QLevel::Connection { rel_params })
                };

                trace!("Connection `{prop_key}` of prop {property_id:?}");

                FieldData::mandatory(
                    FieldKind::Connection {
                        property_id: Some(property_id),
                        first: argument::FirstArg,
                        after: argument::AfterArg,
                    },
                    connection_ref,
                )
            } else if let RelParams::Type(_) = meta.relationship.rel_params {
                todo!("Edge list with rel params");
            } else {
                let mut unit = self.get_def_type_ref(value_def_id, QLevel::Node);

                let value_operator_addr = self
                    .serde_generator
                    .gen_addr(gql_array_serde_key(value_def_id))
                    .unwrap();

                trace!("Array value operator addr: {value_operator_addr:?}");

                if let UnitTypeRef::NativeScalar(native) = &mut unit {
                    native.operator_addr = value_operator_addr;
                }

                FieldData {
                    kind: property_field_producer.make_property(PropertyData {
                        property_id,
                        value_operator_addr,
                    }),
                    field_type: TypeRef::mandatory(unit).to_array(Optionality::from_optional(
                        matches!(prop_cardinality, PropertyCardinality::Optional),
                    )),
                }
            };

            fields.insert(field_namespace.unique_literal(prop_key), field_data);
        }
    }

    pub fn add_named_map_query(
        &mut self,
        name: &str,
        [input_key, output_key]: &[MapKey; 2],
        prop_flow: &[PropertyFlow],
    ) {
        let input_serde_key = {
            let mut serde_modifier = SerdeModifier::graphql_default();

            if input_key.seq {
                serde_modifier.insert(SerdeModifier::ARRAY);
            }

            SerdeKey::Def(SerdeDef::new(input_key.def_id, serde_modifier))
        };

        let input_operator_addr = self.serde_generator.gen_addr(input_serde_key).unwrap();

        let queries: FnvHashMap<PropertyId, Var> = prop_flow
            .iter()
            .filter_map(|prop_flow| {
                if let PropertyFlowData::Match(var) = &prop_flow.data {
                    Some((prop_flow.id, *var))
                } else {
                    None
                }
            })
            .collect();

        let input_arg = match self
            .serde_generator
            .seal_ctx
            .get_repr_kind(&input_key.def_id)
        {
            Some(ReprKind::Scalar(..)) => {
                let scalar_input_name: String =
                    match self.serde_generator.defs.def_kind(input_key.def_id) {
                        DefKind::Type(type_def) => match type_def.ident {
                            Some(ident) => ident.into(),
                            None => return,
                        },
                        _ => return,
                    };

                MapInputArg {
                    operator_addr: input_operator_addr,
                    scalar_input_name: Some(scalar_input_name),
                    default_arg: None,
                }
            }
            _ => {
                let _unit_type_ref = self.get_def_type_ref(input_key.def_id, QLevel::Node);

                let default_arg = match self.serde_generator.get_operator(input_operator_addr) {
                    SerdeOperator::Struct(struct_op) => {
                        let all_optional =
                            struct_op.properties.values().all(|prop| prop.is_optional());

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
                }
            }
        };

        let field_data = if output_key.seq {
            FieldData {
                kind: FieldKind::MapConnection {
                    key: [*input_key, *output_key],
                    queries,
                    input_arg,
                    first_arg: argument::FirstArg,
                    after_arg: argument::AfterArg,
                },
                field_type: TypeRef {
                    modifier: TypeModifier::Unit(Optionality::Mandatory),
                    unit: self.get_def_type_ref(
                        output_key.def_id,
                        QLevel::Connection { rel_params: None },
                    ),
                },
            }
        } else {
            FieldData {
                kind: FieldKind::MapFind {
                    key: [*input_key, *output_key],
                    queries,
                    input_arg,
                },
                field_type: TypeRef {
                    modifier: TypeModifier::Unit(Optionality::Optional),
                    unit: self.get_def_type_ref(output_key.def_id, QLevel::Node),
                },
            }
        };

        object_data_mut(self.schema.query, self.schema)
            .fields
            .insert(name.into(), field_data);
    }

    pub fn add_entity_queries_and_mutations(&mut self, entity_data: EntityData) {
        let type_info = self.partial_ontology.get_type_info(entity_data.node_def_id);

        let node_ref = UnitTypeRef::Addr(entity_data.type_addr);
        let connection_ref = self.get_def_type_ref(
            entity_data.node_def_id,
            QLevel::Connection { rel_params: None },
        );

        let entity_operator_addr = self
            .serde_generator
            .gen_addr(gql_serde_key(entity_data.node_def_id))
            .unwrap();

        let id_type_info = self.partial_ontology.get_type_info(entity_data.id_def_id);
        let id_operator_addr = id_type_info.operator_addr.expect("No id_operator_addr");

        let id_unit_type_ref = self.get_def_type_ref(entity_data.id_def_id, QLevel::Node);

        if true {
            let query = object_data_mut(self.schema.query, self.schema);
            query.fields.insert(
                self.namespace.list(type_info),
                FieldData::mandatory(
                    FieldKind::Connection {
                        property_id: None,
                        first: argument::FirstArg,
                        after: argument::AfterArg,
                    },
                    connection_ref,
                ),
            );
        }

        {
            let mutation = object_data_mut(self.schema.mutation, self.schema);
            mutation.fields.insert(
                self.namespace.create(type_info),
                FieldData {
                    kind: FieldKind::CreateMutation {
                        input: argument::InputArg {
                            type_addr: entity_data.type_addr,
                            def_id: entity_data.node_def_id,
                            operator_addr: entity_operator_addr,
                            typing_purpose: TypingPurpose::Input,
                        },
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.update(type_info),
                FieldData {
                    kind: FieldKind::UpdateMutation {
                        input: argument::InputArg {
                            type_addr: entity_data.type_addr,
                            def_id: entity_data.node_def_id,
                            operator_addr: entity_operator_addr,
                            typing_purpose: TypingPurpose::PartialInput,
                        },
                        id: argument::IdArg {
                            operator_addr: id_operator_addr,
                            unit_type_ref: id_unit_type_ref,
                        },
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.delete(type_info),
                FieldData {
                    kind: FieldKind::DeleteMutation {
                        id: argument::IdArg {
                            operator_addr: id_operator_addr,
                            unit_type_ref: id_unit_type_ref,
                        },
                    },
                    field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(NativeScalarRef {
                        operator_addr: SerdeOperatorAddr(42),
                        kind: NativeScalarKind::Boolean,
                    })),
                },
            );
        }
    }
}

pub(super) fn object_data_mut(index: TypeAddr, schema: &mut GraphqlSchema) -> &mut ObjectData {
    let type_data = schema.types.get_mut(index.0 as usize).unwrap();
    match &mut type_data.kind {
        TypeKind::Object(object_data) => object_data,
        _ => panic!("{index:?} is not an object"),
    }
}

fn get_native_scalar_kind(
    serde_generator: &SerdeGenerator,
    serde_operator: &SerdeOperator,
) -> NativeScalarKind {
    match serde_operator {
        SerdeOperator::Unit => NativeScalarKind::Unit,
        SerdeOperator::False(_) | SerdeOperator::True(_) | SerdeOperator::Boolean(_) => {
            NativeScalarKind::Boolean
        }
        SerdeOperator::I64(..) => panic!("Must be a custom scalar"),
        SerdeOperator::I32(def_id, _) => NativeScalarKind::Int(*def_id),
        SerdeOperator::F64(def_id, _) => NativeScalarKind::Number(*def_id),
        SerdeOperator::String(_)
        | SerdeOperator::StringConstant(..)
        | SerdeOperator::TextPattern(_)
        | SerdeOperator::CapturingTextPattern(_) => NativeScalarKind::String,
        SerdeOperator::IdSingletonStruct(..) => panic!("Id should not appear in GraphQL"),
        SerdeOperator::Alias(alias_op) => get_native_scalar_kind(
            serde_generator,
            serde_generator.get_operator(alias_op.inner_addr),
        ),
        op @ (SerdeOperator::Union(_)
        | SerdeOperator::Struct(_)
        | SerdeOperator::DynamicSequence
        | SerdeOperator::RelationSequence(_)
        | SerdeOperator::ConstructorSequence(_)) => panic!("not a native scalar: {op:?}"),
    }
}

/// An extension of QueryLevel used only inside the generator
#[derive(Clone, Copy)]
pub enum QLevel {
    Node,
    Edge {
        rel_params: Option<(DefId, SerdeOperatorAddr)>,
    },
    Connection {
        rel_params: Option<(DefId, SerdeOperatorAddr)>,
    },
}

impl QLevel {
    fn as_query_level(&self) -> QueryLevel {
        match self {
            Self::Node => QueryLevel::Node,
            Self::Edge { rel_params } => QueryLevel::Edge {
                rel_params: rel_params.map(|(_, op_id)| op_id),
            },
            Self::Connection { rel_params } => QueryLevel::Connection {
                rel_params: rel_params.map(|(_, op_id)| op_id),
            },
        }
    }
}

#[derive(Clone, Copy)]
enum PropertyFieldProducer {
    Property,
    EdgeProperty,
}

impl PropertyFieldProducer {
    fn make_property(&self, data: PropertyData) -> FieldKind {
        match self {
            Self::Property => FieldKind::Property(data),
            Self::EdgeProperty => FieldKind::EdgeProperty(data),
        }
    }
}

fn gql_serde_key(def_id: DefId) -> SerdeKey {
    SerdeKey::Def(SerdeDef::new(def_id, SerdeModifier::graphql_default()))
}

fn gql_array_serde_key(def_id: DefId) -> SerdeKey {
    SerdeKey::Def(SerdeDef::new(
        def_id,
        SerdeModifier::graphql_default() | SerdeModifier::ARRAY,
    ))
}

fn new_schema_with_capacity(package_id: PackageId, cap: usize) -> GraphqlSchema {
    GraphqlSchema {
        package_id,
        query: TypeAddr(0),
        mutation: TypeAddr(0),
        page_info: TypeAddr(0),
        i64_custom_scalar: None,
        json_scalar: TypeAddr(0),
        types: Vec::with_capacity(cap),
        type_addr_by_def: FnvHashMap::with_capacity_and_hasher(cap, Default::default()),
    }
}
