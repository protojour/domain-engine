use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    graphql::{
        argument,
        data::{
            EdgeData, EntityData, FieldData, FieldKind, NativeScalarKind, NativeScalarRef,
            NodeData, ObjectData, ObjectKind, Optionality, PropertyData, ScalarData, TypeData,
            TypeIndex, TypeKind, TypeModifier, TypeRef, UnionData, UnitTypeRef,
        },
        schema::{GraphqlSchema, QueryLevel, TypingPurpose},
    },
    ontology::{Ontology, PropertyCardinality, ValueCardinality},
    serde::{
        operator::{SerdeOperator, SerdeOperatorId},
        SerdeKey,
    },
    smart_format, DataModifier, DefId, DefVariant, PackageId, Role,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    def::{DefKind, Defs, LookupRelationshipMeta, RelParams},
    protocol::serde::serde_generator::SerdeGenerator,
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
    serde_generator: &mut SerdeGenerator<'c, '_>,
) -> GraphqlSchema {
    let domain = partial_ontology.find_domain(package_id).unwrap();

    let mut schema = GraphqlSchema {
        package_id,
        query: TypeIndex(0),
        mutation: TypeIndex(0),
        types: Vec::with_capacity(domain.type_names.len()),
        type_index_by_def: FnvHashMap::with_capacity_and_hasher(
            domain.type_names.len(),
            Default::default(),
        ),
    };
    let mut namespace = GraphqlNamespace::with_domain_disambiguation(DomainDisambiguation {
        root_domain: package_id,
        ontology: partial_ontology,
    });
    let relations = serde_generator.relations;
    let defs = serde_generator.defs;
    let seal_ctx = serde_generator.seal_ctx;
    let mut builder = Builder {
        lazy_tasks: vec![],
        schema: &mut schema,
        namespace: &mut namespace,
        partial_ontology,
        serde_generator,
        relations,
        defs,
        seal_ctx,
    };

    builder.register_query();
    builder.register_mutation();

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);
        if !type_info.public {
            continue;
        }

        if let Some(operator_id) = type_info.operator_id {
            debug!(
                "adapt type `{name:?}` {operator_id:?}",
                name = type_info.name
            );

            let type_ref = builder.get_def_type_ref(type_info.def_id, QLevel::Node);

            if let Some(entity_info) = entity_check(builder.schema, type_ref) {
                builder.add_entity_queries_and_mutations(entity_info);
            }
        }
    }

    while !builder.lazy_tasks.is_empty() {
        for lazy_task in std::mem::take(&mut builder.lazy_tasks) {
            builder.exec_lazy_task(lazy_task);
        }
    }

    schema
}

fn entity_check(schema: &GraphqlSchema, type_ref: UnitTypeRef) -> Option<EntityData> {
    if let UnitTypeRef::Indexed(type_index) = type_ref {
        let type_data = &schema.type_data(type_index);

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
                type_index,
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
    /// The compiler's sealed type information
    seal_ctx: &'c SealCtx,
}

enum LazyTask {
    HarvestFields {
        type_index: TypeIndex,
        def_id: DefId,
        property_field_producer: PropertyFieldProducer,
        is_entrypoint: bool,
    },
}

enum NewType {
    Indexed(TypeIndex, TypeData),
    NativeScalar(NativeScalarRef),
}

impl<'a, 's, 'c, 'm> Builder<'a, 's, 'c, 'm> {
    fn exec_lazy_task(&mut self, task: LazyTask) {
        match task {
            LazyTask::HarvestFields {
                type_index,
                def_id,
                property_field_producer,
                is_entrypoint,
            } => {
                let Some(properties) = self.relations.properties_by_def_id(def_id) else {
                    return;
                };
                let mut fields = Default::default();

                debug!("Harvest fields for {def_id:?} / {type_index:?}");

                if is_entrypoint {
                    let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");
                    if let ReprKind::StructIntersection(members) = repr_kind {
                        for (member_def_id, _) in members {
                            self.lazy_tasks.push(LazyTask::HarvestFields {
                                type_index,
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
                    &mut GraphqlNamespace::new(),
                );

                match &mut self.schema.types[type_index.0 as usize].kind {
                    TypeKind::Object(object_data) => {
                        object_data.fields.extend(fields);
                    }
                    _ => panic!(),
                }
            }
        }
    }

    fn register_query(&mut self) {
        let index = self.next_type_index();
        self.schema.types.push(TypeData {
            typename: "Query".into(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Query,
            }),
        });
        self.schema.query = index;
    }

    pub fn register_mutation(&mut self) {
        let index = self.next_type_index();
        self.schema.types.push(TypeData {
            typename: "Mutation".into(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Mutation,
            }),
        });
        self.schema.mutation = index;
    }

    pub fn get_def_type_ref(&mut self, def_id: DefId, level: QLevel) -> UnitTypeRef {
        if let Some(type_index) = self
            .schema
            .type_index_by_def
            .get(&(def_id, level.as_query_level()))
        {
            return UnitTypeRef::Indexed(*type_index);
        }

        match self.make_def_type(def_id, level) {
            NewType::Indexed(type_index, type_data) => {
                self.schema.types[type_index.0 as usize] = type_data;
                UnitTypeRef::Indexed(type_index)
            }
            NewType::NativeScalar(scalar_ref) => UnitTypeRef::NativeScalar(scalar_ref),
        }
    }

    fn next_type_index(&self) -> TypeIndex {
        TypeIndex(self.schema.types.len() as u32)
    }

    fn alloc_def_type_index(&mut self, def_id: DefId, level: QLevel) -> TypeIndex {
        let index = self.next_type_index();
        // note: this will be overwritten later
        self.schema.types.push(TypeData {
            typename: String::new(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::CustomScalar(ScalarData {
                serde_operator_id: SerdeOperatorId(0),
            }),
        });
        self.schema
            .type_index_by_def
            .insert((def_id, level.as_query_level()), index);
        index
    }

    fn make_def_type(&mut self, def_id: DefId, level: QLevel) -> NewType {
        match level {
            QLevel::Node => self.make_node_type(def_id),
            QLevel::Edge { rel_params } => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let edge_index = self.alloc_def_type_index(def_id, level);
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);

                let mut field_namespace = GraphqlNamespace::new();

                // FIXME: what if some of the relation data's fields are called "node"
                let fields: IndexMap<String, FieldData> = [(
                    field_namespace.unique_literal("node"),
                    FieldData {
                        kind: FieldKind::Node,
                        field_type: TypeRef::mandatory(node_ref),
                    },
                )]
                .into();

                if let Some((rel_def_id, _operator_id)) = rel_params {
                    let rel_type_info = self.partial_ontology.get_type_info(rel_def_id);
                    let rel_edge_ref = self.get_def_type_ref(rel_def_id, QLevel::Node);

                    let typename = self.namespace.edge(Some(rel_type_info), type_info);

                    self.lazy_tasks.push(LazyTask::HarvestFields {
                        type_index: edge_index,
                        def_id: rel_def_id,
                        property_field_producer: PropertyFieldProducer::EdgeProperty,
                        is_entrypoint: true,
                    });

                    NewType::Indexed(
                        edge_index,
                        TypeData {
                            typename,
                            input_typename: Some(
                                self.namespace.edge_input(Some(rel_type_info), type_info),
                            ),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_operator_id: type_info.operator_id.unwrap(),
                                    rel_edge_ref: Some(rel_edge_ref),
                                }),
                            }),
                        },
                    )
                } else {
                    NewType::Indexed(
                        edge_index,
                        TypeData {
                            typename: self.namespace.edge(None, type_info),
                            input_typename: Some(self.namespace.edge_input(None, type_info)),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_operator_id: type_info.operator_id.unwrap(),
                                    rel_edge_ref: None,
                                }),
                            }),
                        },
                    )
                }
            }
            QLevel::Connection { rel_params } => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let connection_index = self.alloc_def_type_index(def_id, level);
                let edge_ref = self.get_def_type_ref(def_id, QLevel::Edge { rel_params });

                NewType::Indexed(
                    connection_index,
                    TypeData {
                        typename: self.namespace.connection(type_info),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: [(
                                smart_format!("edges"),
                                FieldData {
                                    kind: FieldKind::Edges,
                                    field_type: TypeRef::mandatory(edge_ref)
                                        .to_array(Optionality::Optional),
                                },
                            )]
                            .into(),
                            kind: ObjectKind::Connection,
                        }),
                    },
                )
            }
        }
    }

    fn make_node_type(&mut self, def_id: DefId) -> NewType {
        if self.relations.properties_by_def_id(def_id).is_none() {
            return NewType::NativeScalar(NativeScalarRef {
                operator_id: self
                    .serde_generator
                    .gen_operator_id(gql_serde_key(DefId::unit()))
                    .unwrap(),
                kind: NativeScalarKind::Unit,
            });
        }

        let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");

        match repr_kind {
            ReprKind::Unit | ReprKind::Struct | ReprKind::StructIntersection(_) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let type_index = self.alloc_def_type_index(def_id, QLevel::Node);

                let operator_id = type_info.operator_id.unwrap();

                self.lazy_tasks.push(LazyTask::HarvestFields {
                    type_index,
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
                        operator_id,
                    }),
                });

                NewType::Indexed(
                    type_index,
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
                let node_index = self.alloc_def_type_index(def_id, QLevel::Node);

                let mut needs_scalar = false;
                let mut type_variants = vec![];

                for (variant_def_id, _) in variants {
                    match self.seal_ctx.get_repr_kind(variant_def_id) {
                        Some(ReprKind::Scalar(..)) => {
                            needs_scalar = true;
                            break;
                        }
                        _ => match self.get_def_type_ref(*variant_def_id, QLevel::Node) {
                            UnitTypeRef::Indexed(type_index) => {
                                type_variants.push(type_index);
                            }
                            UnitTypeRef::NativeScalar(_) => {
                                needs_scalar = true;
                                break;
                            }
                        },
                    }
                }

                NewType::Indexed(
                    node_index,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.union_input(type_info)),
                        partial_input_typename: Some(self.namespace.union_partial_input(type_info)),
                        kind: if needs_scalar {
                            let operator_id = self
                                .serde_generator
                                .gen_operator_id(gql_serde_key(def_id))
                                .unwrap();
                            TypeKind::CustomScalar(ScalarData {
                                serde_operator_id: operator_id,
                            })
                        } else {
                            TypeKind::Union(UnionData {
                                union_def_id: type_info.def_id,
                                variants: type_variants,
                            })
                        },
                    },
                )
            }
            ReprKind::Seq | ReprKind::Intersection(_) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let operator_id = self
                    .serde_generator
                    .gen_operator_id(gql_serde_key(def_id))
                    .unwrap();
                let type_index = self.alloc_def_type_index(def_id, QLevel::Node);

                NewType::Indexed(
                    type_index,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.input(type_info)),
                        partial_input_typename: Some(self.namespace.partial_input(type_info)),
                        kind: TypeKind::CustomScalar(ScalarData {
                            serde_operator_id: operator_id,
                        }),
                    },
                )
            }
            ReprKind::Scalar(scalar_def_id, ReprScalarKind::I64(range), _) => {
                if range.start() >= &i32::MIN.into() && range.end() <= &i32::MAX.into() {
                    NewType::NativeScalar(NativeScalarRef {
                        operator_id: self
                            .serde_generator
                            .gen_operator_id(gql_serde_key(def_id))
                            .unwrap(),
                        kind: NativeScalarKind::Int(*scalar_def_id),
                    })
                } else {
                    let type_info = self.partial_ontology.get_type_info(def_id);
                    NewType::Indexed(
                        self.alloc_def_type_index(type_info.def_id, QLevel::Node),
                        TypeData {
                            // FIXME: Must make sure that domain typenames take precedence over generated ones
                            typename: smart_format!("i64"),
                            input_typename: None,
                            partial_input_typename: None,
                            kind: TypeKind::CustomScalar(ScalarData {
                                serde_operator_id: self
                                    .serde_generator
                                    .gen_operator_id(gql_serde_key(DefId::unit()))
                                    .unwrap(),
                            }),
                        },
                    )
                }
            }
            ReprKind::Scalar(def_id, ReprScalarKind::F64(_), _) => {
                NewType::NativeScalar(NativeScalarRef {
                    operator_id: self
                        .serde_generator
                        .gen_operator_id(gql_serde_key(*def_id))
                        .unwrap(),
                    kind: NativeScalarKind::Number(*def_id),
                })
            }
            ReprKind::Scalar(..) => {
                let operator_id = self
                    .serde_generator
                    .gen_operator_id(gql_serde_key(def_id))
                    .unwrap();

                NewType::NativeScalar(NativeScalarRef {
                    operator_id,
                    kind: get_native_scalar_kind(
                        self.serde_generator,
                        self.serde_generator.get_operator(operator_id),
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

            debug!("    register struct field `{prop_key}`: {property_id}");

            let value_properties = self.relations.properties_by_def_id(value_def_id);
            let is_entity_value = value_properties
                .map(|properties| properties.identified_by.is_some())
                .unwrap_or(false);

            let field_data = if matches!(value_cardinality, ValueCardinality::One) {
                let modifier = TypeModifier::new_unit(Optionality::from_optional(matches!(
                    prop_cardinality,
                    PropertyCardinality::Optional
                )));

                let value_operator_id = self
                    .serde_generator
                    .gen_operator_id(gql_serde_key(value_def_id))
                    .unwrap();

                let field_type = if property.is_entity_id {
                    TypeRef {
                        modifier,
                        unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_id: value_operator_id,
                            kind: NativeScalarKind::ID,
                        }),
                    }
                } else {
                    let qlevel = match meta.relationship.rel_params {
                        RelParams::Unit => QLevel::Node,
                        RelParams::Type(rel_def_id) => {
                            let operator_id = self
                                .serde_generator
                                .gen_operator_id(gql_serde_key(rel_def_id))
                                .unwrap();
                            let repr_kind = self.seal_ctx.get_repr_kind(&rel_def_id);

                            if matches!(repr_kind, Some(ReprKind::Struct)) {
                                // BUG(repr): The ReprKind should be Unit when the properties are None.
                                if self.relations.properties_by_def_id(rel_def_id).is_none() {
                                    QLevel::Node
                                } else {
                                    QLevel::Edge {
                                        rel_params: Some((rel_def_id, operator_id)),
                                    }
                                }
                            } else {
                                QLevel::Edge {
                                    rel_params: Some((rel_def_id, operator_id)),
                                }
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
                        value_operator_id,
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
                                .gen_operator_id(gql_serde_key(rel_def_id))
                                .unwrap(),
                        )),
                        RelParams::IndexRange(_) => todo!(),
                    };

                    debug!("    connection/edge rel params {rel_params:?}");

                    self.get_def_type_ref(value_def_id, QLevel::Connection { rel_params })
                };

                debug!("Connection `{prop_key}` of prop {property_id:?}");

                FieldData::mandatory(
                    FieldKind::Connection {
                        property_id: Some(property_id),
                        first: argument::First,
                        after: argument::After,
                    },
                    connection_ref,
                )
            } else if let RelParams::Type(_) = meta.relationship.rel_params {
                todo!("Edge list with rel params");
            } else {
                let unit = self.get_def_type_ref(value_def_id, QLevel::Node);

                let value_operator_id = self
                    .serde_generator
                    .gen_operator_id(gql_array_serde_key(value_def_id))
                    .unwrap();

                FieldData {
                    kind: property_field_producer.make_property(PropertyData {
                        property_id,
                        value_operator_id,
                    }),
                    field_type: TypeRef::mandatory(unit).to_array(Optionality::from_optional(
                        matches!(prop_cardinality, PropertyCardinality::Optional),
                    )),
                }
            };

            fields.insert(field_namespace.unique_literal(prop_key), field_data);
        }
    }

    pub fn add_entity_queries_and_mutations(&mut self, entity_data: EntityData) {
        let type_info = self.partial_ontology.get_type_info(entity_data.node_def_id);

        let node_ref = UnitTypeRef::Indexed(entity_data.type_index);
        let connection_ref = self.get_def_type_ref(
            entity_data.node_def_id,
            QLevel::Connection { rel_params: None },
        );

        let id_type_info = self.partial_ontology.get_type_info(entity_data.id_def_id);
        let id_operator_id = id_type_info.operator_id.expect("No id_operator_id");

        {
            let query = object_data_mut(self.schema.query, self.schema);
            query.fields.insert(
                self.namespace.list(type_info),
                FieldData::mandatory(
                    FieldKind::Connection {
                        property_id: None,
                        first: argument::First,
                        after: argument::After,
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
                        input: argument::Input(
                            entity_data.type_index,
                            entity_data.node_def_id,
                            TypingPurpose::Input,
                        ),
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.update(type_info),
                FieldData {
                    kind: FieldKind::UpdateMutation {
                        input: argument::Input(
                            entity_data.type_index,
                            entity_data.node_def_id,
                            TypingPurpose::PartialInput,
                        ),
                        id: argument::Id(id_operator_id),
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.delete(type_info),
                FieldData {
                    kind: FieldKind::DeleteMutation {
                        id: argument::Id(id_operator_id),
                    },
                    field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(NativeScalarRef {
                        operator_id: SerdeOperatorId(42),
                        kind: NativeScalarKind::Boolean,
                    })),
                },
            );
        }
    }
}

pub(super) fn object_data_mut(index: TypeIndex, schema: &mut GraphqlSchema) -> &mut ObjectData {
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
        SerdeOperator::I64(def_id, _) => NativeScalarKind::Int(*def_id),
        SerdeOperator::F64(def_id, _) => NativeScalarKind::Number(*def_id),
        SerdeOperator::String(_)
        | SerdeOperator::StringConstant(..)
        | SerdeOperator::TextPattern(_)
        | SerdeOperator::CapturingTextPattern(_) => NativeScalarKind::String,
        SerdeOperator::PrimaryId(..) => panic!("Id should not appear in GraphQL"),
        SerdeOperator::Alias(alias_op) => get_native_scalar_kind(
            serde_generator,
            serde_generator.get_operator(alias_op.inner_operator_id),
        ),
        op @ (SerdeOperator::Union(_)
        | SerdeOperator::Struct(_)
        | SerdeOperator::DynamicSequence
        | SerdeOperator::RelationSequence(_)
        | SerdeOperator::ConstructorSequence(_)) => panic!("not a native scalar: {op:?}"),
    }
}

/// An extension of QueryLevel used only inside the generator
pub enum QLevel {
    Node,
    Edge {
        rel_params: Option<(DefId, SerdeOperatorId)>,
    },
    Connection {
        rel_params: Option<(DefId, SerdeOperatorId)>,
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
    SerdeKey::Def(DefVariant::new(def_id, DataModifier::default()))
}

fn gql_array_serde_key(def_id: DefId) -> SerdeKey {
    SerdeKey::Def(DefVariant::new(
        def_id,
        DataModifier::default() | DataModifier::ARRAY,
    ))
}
