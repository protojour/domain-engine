use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    debug::NoFmt,
    interface::{
        discriminator::{
            leaf_discriminant_scalar_union_for_has_attribute, Discriminant,
            LeafDiscriminantScalarUnion, VariantDiscriminator,
        },
        graphql::{
            argument,
            data::{
                ConnectionData, ConnectionPropertyField, EdgeData, FieldData, FieldKind,
                NativeScalarKind, NativeScalarRef, NodeData, ObjectData, ObjectInterface,
                ObjectKind, Optionality, ScalarData, TypeAddr, TypeData, TypeKind, TypeModifier,
                TypeRef, UnionData, UnitTypeRef,
            },
            schema::InterfaceImplementor,
        },
        serde::{
            operator::{SerdeOperator, SerdeStructFlags},
            SerdeDef, SerdeModifier,
        },
    },
    ontology::ontol::TextConstant,
    phf::PhfIndexMap,
    property::{PropertyCardinality, ValueCardinality},
    DefId, RelationshipId,
};
use thin_vec::thin_vec;
use tracing::{trace, trace_span, warn};

use crate::{
    def::{rel_def_meta, DefKind, RelDefMeta, RelParams, TypeDefFlags},
    interface::serde::{serde_generator::SerdeGenerator, SerdeKey},
    phf_build::build_phf_index_map,
    relation::Property,
    repr::repr_model::{ReprKind, ReprScalarKind},
};

use super::{graphql_namespace::GraphqlNamespace, schema_builder::*};

impl<'a, 's, 'c, 'm> SchemaBuilder<'a, 's, 'c, 'm> {
    pub fn make_def_type(&mut self, def_id: DefId, level: QLevel) -> NewType {
        match level {
            QLevel::Node => self.make_node_type(def_id),
            QLevel::Edge { rel_params } => {
                let def = self.partial_ontology.def(def_id);
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);
                let node_type_addr = node_ref.unwrap_addr();

                let mut field_namespace = GraphqlNamespace::default();

                // FIXME: what if some of the relation data's fields are called "node"
                let fields: PhfIndexMap<FieldData> = build_phf_index_map([(
                    self.serde_gen
                        .str_ctx
                        .make_phf_key(&field_namespace.unique_literal("node")),
                    FieldData {
                        kind: FieldKind::Node,
                        field_type: TypeRef::mandatory(node_ref),
                    },
                )]);

                let node_operator_addr =
                    self.serde_gen.gen_addr_lazy(gql_serde_key(def.id)).unwrap();

                if let Some((rel_def_id, _operator_addr)) = rel_params {
                    let rel_def = self.partial_ontology.def(rel_def_id);
                    let rel_edge_ref = self.get_def_type_ref(rel_def_id, QLevel::Node);

                    let typename = self.mk_typename_constant(|namespace, strings| {
                        namespace.edge(Some(rel_def), def, strings)
                    });

                    NewType::TypeData(
                        TypeData {
                            typename,
                            input_typename: Some(self.mk_typename_constant(
                                |namespace, strings| {
                                    namespace.edge_input(Some(rel_def), def, strings)
                                },
                            )),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_type_addr,
                                    node_operator_addr,
                                    rel_edge_ref: Some(rel_edge_ref),
                                }),
                                interface: ObjectInterface::Implements(thin_vec![]),
                            }),
                        },
                        NewTypeActions {
                            harvest_fields: Some((rel_def_id, PropertyFieldProducer::EdgeProperty)),
                        },
                    )
                } else {
                    NewType::TypeData(
                        TypeData {
                            typename: self.mk_typename_constant(|namespace, strings| {
                                namespace.edge(None, def, strings)
                            }),
                            input_typename: Some(self.mk_typename_constant(
                                |namespace, strings| namespace.edge_input(None, def, strings),
                            )),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_type_addr,
                                    node_operator_addr,
                                    rel_edge_ref: None,
                                }),
                                interface: ObjectInterface::Implements(thin_vec![]),
                            }),
                        },
                        NewTypeActions::default(),
                    )
                }
            }
            QLevel::Connection { rel_params } => {
                let def = self.partial_ontology.def(def_id);
                let edge_ref = self.get_def_type_ref(def_id, QLevel::Edge { rel_params });
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);
                let node_type_addr = node_ref.unwrap_addr();

                let rel_def = rel_params
                    .map(|(rel_params_def_id, _)| self.partial_ontology.def(rel_params_def_id));

                NewType::TypeData(
                    TypeData {
                        typename: self.mk_typename_constant(|namespace, strings| {
                            namespace.connection(rel_def, def, strings)
                        }),
                        input_typename: Some(self.mk_typename_constant(|namespace, strings| {
                            namespace.patch_edges_input(rel_def, def, strings)
                        })),
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: build_phf_index_map([
                                (
                                    self.serde_gen.str_ctx.make_phf_key("nodes"),
                                    FieldData {
                                        kind: FieldKind::Nodes,
                                        field_type: TypeRef::mandatory(node_ref)
                                            .to_array(Optionality::Optional),
                                    },
                                ),
                                (
                                    self.serde_gen.str_ctx.make_phf_key("edges"),
                                    FieldData {
                                        kind: FieldKind::Edges,
                                        field_type: TypeRef::mandatory(edge_ref)
                                            .to_array(Optionality::Optional),
                                    },
                                ),
                                (
                                    self.serde_gen.str_ctx.make_phf_key("pageInfo"),
                                    FieldData {
                                        kind: FieldKind::PageInfo,
                                        field_type: TypeRef::mandatory(UnitTypeRef::Addr(
                                            self.schema.page_info,
                                        )),
                                    },
                                ),
                                (
                                    self.serde_gen.str_ctx.make_phf_key("totalCount"),
                                    FieldData {
                                        kind: FieldKind::TotalCount,
                                        field_type: TypeRef {
                                            modifier: TypeModifier::Unit(Optionality::Mandatory),
                                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                                operator_addr: self
                                                    .serde_gen
                                                    .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                                                        self.primitives.i64,
                                                        SerdeModifier::NONE,
                                                    )))
                                                    .unwrap(),
                                                kind: NativeScalarKind::Int(self.primitives.i64),
                                            }),
                                        },
                                    },
                                ),
                            ]),
                            kind: ObjectKind::Connection(ConnectionData { node_type_addr }),
                            interface: ObjectInterface::Implements(thin_vec![]),
                        }),
                    },
                    NewTypeActions::default(),
                )
            }
            QLevel::MutationResult => {
                let def = self.partial_ontology.def(def_id);
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);

                NewType::TypeData(
                    TypeData {
                        typename: self.mk_typename_constant(|namespace, strings| {
                            namespace.mutation_result(def, strings)
                        }),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: build_phf_index_map([
                                (
                                    self.serde_gen.str_ctx.make_phf_key("node"),
                                    FieldData {
                                        kind: FieldKind::Node,
                                        field_type: TypeRef::optional(node_ref),
                                    },
                                ),
                                (
                                    self.serde_gen.str_ctx.make_phf_key("deleted"),
                                    FieldData {
                                        kind: FieldKind::Deleted,
                                        field_type: TypeRef {
                                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                                operator_addr: self
                                                    .serde_gen
                                                    .gen_addr_lazy(gql_serde_key(
                                                        self.primitives.bool,
                                                    ))
                                                    .unwrap(),
                                                kind: NativeScalarKind::Boolean,
                                            }),
                                            modifier: TypeModifier::Unit(Optionality::Mandatory),
                                        },
                                    },
                                ),
                            ]),
                            kind: ObjectKind::MutationResult,
                            interface: ObjectInterface::Implements(thin_vec![]),
                        }),
                    },
                    NewTypeActions::default(),
                )
            }
        }
    }

    fn make_node_type(&mut self, def_id: DefId) -> NewType {
        if self.relations.properties_by_def_id(def_id).is_none() {
            return NewType::NativeScalar(NativeScalarRef {
                operator_addr: self
                    .serde_gen
                    .gen_addr_lazy(gql_serde_key(DefId::unit()))
                    .unwrap(),
                kind: NativeScalarKind::Unit,
            });
        }

        let repr_kind = self.repr_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");

        match repr_kind {
            ReprKind::Unit | ReprKind::Struct | ReprKind::StructIntersection(_) => {
                let def = self.partial_ontology.def(def_id);

                let operator_addr = self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap();

                let type_kind = TypeKind::Object(ObjectData {
                    fields: Default::default(),
                    kind: ObjectKind::Node(NodeData {
                        def_id: def.id,
                        entity_id: def.entity().map(|entity| entity.id_value_def_id),
                        operator_addr,
                    }),
                    interface: ObjectInterface::Implements(thin_vec![]),
                });

                NewType::TypeData(
                    TypeData {
                        typename: self.mk_typename_constant(|namespace, strings| {
                            namespace.typename(def, strings)
                        }),
                        input_typename: Some(self.mk_typename_constant(|namespace, strings| {
                            namespace.input(def, strings)
                        })),
                        partial_input_typename: Some(self.mk_typename_constant(
                            |namespace, strings| namespace.partial_input(def, strings),
                        )),
                        kind: type_kind,
                    },
                    NewTypeActions {
                        harvest_fields: Some((def.id, PropertyFieldProducer::Property)),
                    },
                )
            }
            ReprKind::Union(variants) | ReprKind::StructUnion(variants) => {
                let def = self.partial_ontology.def(def_id);

                let mut needs_scalar = false;
                let mut type_variants = thin_vec![];

                for (variant_def_id, _) in variants {
                    match self.repr_ctx.get_repr_kind(variant_def_id) {
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

                let operator_addr = self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap();

                NewType::TypeData(
                    TypeData {
                        typename: self.mk_typename_constant(|namespace, strings| {
                            namespace.typename(def, strings)
                        }),
                        input_typename: Some(self.mk_typename_constant(|namespace, strings| {
                            namespace.union_input(def, strings)
                        })),
                        partial_input_typename: Some(self.mk_typename_constant(
                            |namespace, strings| namespace.union_partial_input(def, strings),
                        )),
                        kind: if needs_scalar {
                            TypeKind::CustomScalar(ScalarData { operator_addr })
                        } else {
                            TypeKind::Union(UnionData {
                                union_def_id: def.id,
                                variants: type_variants,
                                operator_addr,
                            })
                        },
                    },
                    NewTypeActions::default(),
                )
            }
            ReprKind::Seq | ReprKind::Intersection(_) => {
                let def = self.partial_ontology.def(def_id);
                let operator_addr = self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap();

                NewType::TypeData(
                    TypeData {
                        typename: self.mk_typename_constant(|namespace, strings| {
                            namespace.typename(def, strings)
                        }),
                        input_typename: Some(self.mk_typename_constant(|namespace, strings| {
                            namespace.input(def, strings)
                        })),
                        partial_input_typename: Some(self.mk_typename_constant(
                            |namespace, strings| namespace.partial_input(def, strings),
                        )),
                        kind: TypeKind::CustomScalar(ScalarData { operator_addr }),
                    },
                    NewTypeActions::default(),
                )
            }
            ReprKind::Scalar(scalar_def_id, ReprScalarKind::I64(range), _) => {
                if range.start() >= &i32::MIN.into() && range.end() <= &i32::MAX.into() {
                    NewType::NativeScalar(NativeScalarRef {
                        operator_addr: self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap(),
                        kind: NativeScalarKind::Int(*scalar_def_id),
                    })
                } else {
                    NewType::TypeData(
                        TypeData {
                            // FIXME: Must make sure that domain typenames take precedence over generated ones
                            typename: self.serde_gen.str_ctx.intern_constant("_ontol_i64"),
                            input_typename: None,
                            partial_input_typename: None,
                            kind: TypeKind::CustomScalar(ScalarData {
                                operator_addr: self
                                    .serde_gen
                                    .gen_addr_lazy(gql_serde_key(def_id))
                                    .unwrap(),
                            }),
                        },
                        NewTypeActions::default(),
                    )
                }
            }
            ReprKind::Scalar(def_id, ReprScalarKind::F64(_), _) => {
                NewType::NativeScalar(NativeScalarRef {
                    operator_addr: self
                        .serde_gen
                        .gen_addr_lazy(gql_serde_key(*def_id))
                        .unwrap(),
                    kind: NativeScalarKind::Number(*def_id),
                })
            }
            ReprKind::Scalar(..) => {
                let operator_addr = self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap();

                NewType::NativeScalar(NativeScalarRef {
                    operator_addr,
                    kind: get_native_scalar_kind(
                        self.serde_gen,
                        self.serde_gen.get_operator(operator_addr),
                    ),
                })
            }
            ReprKind::Extern => panic!(),
        }
    }

    pub fn harvest_fields(
        &mut self,
        type_addr: TypeAddr,
        def_id: DefId,
        property_field_producer: PropertyFieldProducer,
    ) {
        let mut interface_variants: Vec<(RelationshipId, RelationId, &[VariantDiscriminator])> =
            Default::default();

        // See if it should be an interface of flattened unions
        if let Some(table) = self.relations.properties_table_by_def_id(def_id) {
            for rel_id in table.keys().copied() {
                let meta = rel_def_meta(rel_id, self.defs);
                let object = meta.relationship.object;

                if let DefKind::Type(_) = meta.relation_def_kind.value {
                    let union_discriminator =
                        self.relations.union_discriminators.get(&object.0).unwrap();

                    interface_variants.push((
                        rel_id,
                        RelationId(meta.relationship.relation_def_id),
                        &union_discriminator.variants,
                    ));
                }
            }
        };

        if !interface_variants.is_empty() {
            self.harvest_fields_with_variant(
                type_addr,
                def_id,
                &HarvestVariant::FlattenedUnionInterface(
                    interface_variants
                        .iter()
                        .map(|(relationship_id, _, discriminators)| {
                            (*relationship_id, *discriminators)
                        })
                        .collect(),
                ),
                property_field_producer,
            );

            self.harvest_interface_implementation_permutations(
                type_addr,
                def_id,
                property_field_producer,
                &interface_variants,
                0,
                &mut vec![],
            );
        } else {
            self.harvest_fields_with_variant(
                type_addr,
                def_id,
                &HarvestVariant::Object,
                property_field_producer,
            );
        }
    }

    fn harvest_interface_implementation_permutations(
        &mut self,
        interface_addr: TypeAddr,
        interface_def_id: DefId,
        property_field_producer: PropertyFieldProducer,
        interface_variants: &[(RelationshipId, RelationId, &'c [VariantDiscriminator])],
        interface_variant_index: usize,
        permutations: &mut Vec<(RelationshipId, RelationId, &'c VariantDiscriminator)>,
    ) {
        if interface_variant_index < interface_variants.len() {
            let (relationship_id, relation_id, discriminators) =
                &interface_variants[interface_variant_index];
            for discriminator in discriminators.iter() {
                permutations.push((*relationship_id, *relation_id, discriminator));
                self.harvest_interface_implementation_permutations(
                    interface_addr,
                    interface_def_id,
                    property_field_producer,
                    interface_variants,
                    interface_variant_index + 1,
                    permutations,
                );
                permutations.pop();
            }
            return;
        }

        let (mut typename, entity_id, operator_addr) = {
            let type_data = &self.schema.types[interface_addr.0 as usize];
            let TypeKind::Object(object_data) = &type_data.kind else {
                panic!()
            };
            let ObjectKind::Node(node_data) = &object_data.kind else {
                panic!()
            };

            (
                self.serde_gen.str_ctx[type_data.typename].to_string(),
                node_data.entity_id,
                node_data.operator_addr,
            )
        };

        // Generate typename variant
        for (_, relation_id, discriminator) in permutations.iter() {
            let relation_def = self.partial_ontology.def(relation_id.0);
            let variant_def = self.partial_ontology.def(discriminator.def_id());

            fn typename_append(output: &mut std::string::String, name: &str) {
                if name
                    .chars()
                    .next()
                    .map(|char| char.is_lowercase())
                    .unwrap_or(true)
                {
                    output.push('_');
                }
                output.push_str(name);
            }

            if let Some(relation_name) = relation_def.name() {
                typename_append(&mut typename, &self.serde_gen.str_ctx[relation_name]);
            }

            if let Some(variant_name) = variant_def.name() {
                typename_append(&mut typename, &self.serde_gen.str_ctx[variant_name]);
            }
        }

        let permutation_addr = self.schema.push_type_data(TypeData {
            typename: self.serde_gen.str_ctx.intern_constant(&typename),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Node(NodeData {
                    def_id: interface_def_id,
                    entity_id,
                    operator_addr,
                }),
                interface: ObjectInterface::Implements(thin_vec![interface_addr]),
            }),
        });

        self.schema
            .interface_implementors
            .entry(interface_addr)
            .or_default()
            .push(InterfaceImplementor {
                addr: permutation_addr,
                attribute_predicate: permutations
                    .iter()
                    .copied()
                    .map(|(relationship_id, _, discriminator)| {
                        (relationship_id, discriminator.def_id())
                    })
                    .collect(),
            });

        let harvest_variant = HarvestVariant::FlattenedUnionPermutation(
            permutations
                .iter()
                .copied()
                .map(|(relationship_id, _, discriminator)| (relationship_id, discriminator))
                .collect(),
        );

        self.harvest_fields_with_variant(
            permutation_addr,
            interface_def_id,
            &harvest_variant,
            property_field_producer,
        );
    }

    fn harvest_fields_with_variant(
        &mut self,
        type_addr: TypeAddr,
        def_id: DefId,
        harvest_variant: &HarvestVariant,
        property_field_producer: PropertyFieldProducer,
    ) {
        let _entered = trace_span!("harvest", id = ?def_id).entered();
        // trace!("Harvest fields for {def_id:?} / {type_addr:?}");

        let mut struct_flags = SerdeStructFlags::empty();
        let mut field_namespace = GraphqlNamespace::default();
        let mut fields: IndexMap<std::string::String, FieldData> = Default::default();

        if let DefKind::Type(type_def) = self.defs.def_kind(def_id) {
            if type_def.flags.contains(TypeDefFlags::OPEN) {
                struct_flags |= SerdeStructFlags::OPEN_DATA;
            }
        }

        let repr_kind = self.repr_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");

        if let Some(properties) = self.relations.properties_by_def_id(def_id) {
            if let Some(table) = &properties.table {
                for (rel_id, property) in table {
                    self.harvest_struct_field(
                        *rel_id,
                        property,
                        property_field_producer,
                        &mut field_namespace,
                        harvest_variant,
                        &mut fields,
                    );
                }
            }
        };

        if let ReprKind::StructIntersection(members) = repr_kind {
            for (member_def_id, _) in members {
                let Some(properties) = self.relations.properties_by_def_id(*member_def_id) else {
                    continue;
                };

                if let Some(table) = &properties.table {
                    for (rel_id, property) in table {
                        self.harvest_struct_field(
                            *rel_id,
                            property,
                            property_field_producer,
                            &mut field_namespace,
                            harvest_variant,
                            &mut fields,
                        );
                    }
                }
            }
        }

        if let Some(union_memberships) = self.union_member_cache.cache.get(&def_id) {
            for union_def_id in union_memberships {
                let _entered = trace_span!("membership", id = ?union_def_id).entered();

                let Some(properties) = self.relations.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for (rel_id, property) in table {
                    let meta = rel_def_meta(*rel_id, self.defs);

                    if meta.relationship.subject.0 == *union_def_id {
                        self.harvest_struct_field(
                            *rel_id,
                            property,
                            property_field_producer,
                            &mut field_namespace,
                            harvest_variant,
                            &mut fields,
                        );
                    }
                }
            }
        }

        if struct_flags.contains(SerdeStructFlags::OPEN_DATA) {
            fields.insert(
                "_open_data".into(),
                FieldData {
                    kind: FieldKind::OpenData,
                    field_type: TypeRef {
                        modifier: TypeModifier::Unit(Optionality::Optional),
                        unit: UnitTypeRef::Addr(self.schema.json_scalar),
                    },
                },
            );
        }

        if fields.is_empty() {
            return;
        }

        let mut fields_vec: Vec<_> = fields
            .into_iter()
            .map(|(key, value)| (self.serde_gen.str_ctx.make_phf_key(&key), value))
            .collect();

        // note: The sort is stable.
        fields_vec.sort_by(|(_, a), (_, b)| {
            self.field_order_category(&a.kind)
                .cmp(&self.field_order_category(&b.kind))
        });

        match &mut self.schema.types[type_addr.0 as usize].kind {
            TypeKind::Object(object_data) => {
                let existing_fields = object_data
                    .fields
                    .iter()
                    .map(|(key, data)| (key.clone(), data.clone()));

                if matches!(harvest_variant, HarvestVariant::FlattenedUnionInterface(_)) {
                    object_data.interface = ObjectInterface::Interface;
                }

                object_data.fields = build_phf_index_map(existing_fields.chain(fields_vec));
            }
            _ => panic!(),
        }
    }

    // Get the general category of each field, for deciding the overall order.
    fn field_order_category(&self, field_kind: &FieldKind) -> u8 {
        match field_kind {
            FieldKind::Property { .. } => 0,
            FieldKind::EdgeProperty { id: rel_id, .. } => {
                rel_def_meta(*rel_id, self.defs)
                    .relationship
                    .projection
                    .subject
                    .0
            }
            // Connections are placed after "plain" fields
            FieldKind::ConnectionProperty(field) => {
                100 + rel_def_meta(field.rel_id, self.defs)
                    .relationship
                    .projection
                    .subject
                    .0
            }
            _ => 255,
        }
    }

    fn harvest_struct_field(
        &mut self,
        rel_id: RelationshipId,
        property: &Property,
        property_field_producer: PropertyFieldProducer,
        field_namespace: &mut GraphqlNamespace,
        harvest_variant: &HarvestVariant,
        output: &mut IndexMap<String, FieldData>,
    ) {
        let meta = rel_def_meta(rel_id, self.defs);
        match (meta.relation_def_kind.value, harvest_variant) {
            (DefKind::TextLiteral(prop_key), _) => self.harvest_data_struct_field(
                (rel_id, prop_key, property),
                meta,
                property_field_producer,
                field_namespace,
                output,
            ),
            (DefKind::Type(_), HarvestVariant::FlattenedUnionInterface(discriminator_table)) => {
                let discriminators = discriminator_table.get(&rel_id).unwrap();

                let mut prop_keys: FnvHashSet<TextConstant> = Default::default();
                for discriminator in discriminators.iter() {
                    if let Discriminant::HasAttribute(_, prop_key, _) = &discriminator.discriminant
                    {
                        prop_keys.insert(*prop_key);
                    }
                }

                let scalar_union =
                    leaf_discriminant_scalar_union_for_has_attribute(discriminators.iter());

                if prop_keys.len() == 1 {
                    let unit_type_ref = if scalar_union == LeafDiscriminantScalarUnion::TEXT {
                        UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_addr: self
                                .serde_gen
                                .gen_addr_lazy(gql_serde_key(self.primitives.text))
                                .unwrap(),
                            kind: NativeScalarKind::String,
                        })
                    } else if scalar_union == LeafDiscriminantScalarUnion::INT {
                        self.get_def_type_ref(self.primitives.i64, QLevel::Node)
                    } else {
                        UnitTypeRef::Addr(self.schema.json_scalar)
                    };

                    let prop_key = &self.serde_gen.str_ctx[prop_keys.into_iter().next().unwrap()];

                    let resolvers: FnvHashMap<DefId, RelationshipId> = discriminators
                        .iter()
                        .filter_map(|discriminator| match &discriminator.discriminant {
                            Discriminant::HasAttribute(relationship_id, ..) => {
                                Some((discriminator.def_id(), *relationship_id))
                            }
                            _ => None,
                        })
                        .collect();

                    output.insert(
                        field_namespace.unique_literal(prop_key),
                        FieldData {
                            kind: FieldKind::FlattenedPropertyDiscriminator {
                                proxy: rel_id,
                                resolvers: Box::new(resolvers),
                            },
                            field_type: TypeRef::mandatory(unit_type_ref),
                        },
                    );
                } else {
                    warn!("No uniform discriminator property");
                }
            }
            (DefKind::Type(_), HarvestVariant::FlattenedUnionPermutation(discriminator_table)) => {
                // concrete type for a specific permutation of flattened union variants

                let union_discriminator = discriminator_table.get(&rel_id).unwrap();

                let Some(table) = self
                    .relations
                    .properties_table_by_def_id(union_discriminator.def_id())
                else {
                    return;
                };

                for (inner_property_id, property) in table {
                    // don't include the discriminator property in the permuted type,
                    // it belongs to the interface.
                    self.harvest_struct_field(
                        *inner_property_id,
                        property,
                        PropertyFieldProducer::FlattenedProperty(rel_id),
                        field_namespace,
                        harvest_variant,
                        output,
                    );
                }
            }
            _ => {
                panic!("Invalid property")
            }
        }
    }

    fn harvest_data_struct_field(
        &mut self,
        (rel_id, prop_key, property): (RelationshipId, &str, &Property),
        meta: RelDefMeta,
        property_field_producer: PropertyFieldProducer,
        field_namespace: &mut GraphqlNamespace,
        output: &mut IndexMap<String, FieldData>,
    ) {
        let (_, (prop_cardinality, value_cardinality), _) = meta.relationship.subject();
        let (value_def_id, ..) = meta.relationship.object();
        trace!("    harvest data struct field `{prop_key}`: {rel_id} ({value_def_id:?}) ({prop_cardinality:?}, {value_cardinality:?})");

        let value_properties = self.relations.properties_by_def_id(value_def_id);

        let is_entity_value = {
            let repr_kind = self.repr_ctx.get_repr_kind(&value_def_id);
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

        let field_data = if matches!(value_cardinality, ValueCardinality::Unit) {
            let modifier = TypeModifier::new_unit(Optionality::from_optional(matches!(
                prop_cardinality,
                PropertyCardinality::Optional
            )));

            let value_operator_addr = self
                .serde_gen
                .gen_addr_lazy(gql_serde_key(value_def_id))
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
                            .serde_gen
                            .gen_addr_lazy(gql_serde_key(rel_def_id))
                            .unwrap();

                        QLevel::Edge {
                            rel_params: Some((rel_def_id, operator_addr)),
                        }
                    }
                    RelParams::IndexRange(_) => todo!(),
                };

                let unit = self.get_def_type_ref(value_def_id, qlevel);

                TypeRef { modifier, unit }
            };

            FieldData {
                kind: property_field_producer.make_property(rel_id, value_operator_addr),
                field_type,
            }
        } else if is_entity_value {
            let connection_ref = {
                let rel_params = match meta.relationship.rel_params {
                    RelParams::Unit => None,
                    RelParams::Type(rel_def_id) => Some((
                        rel_def_id,
                        self.serde_gen
                            .gen_addr_lazy(gql_serde_key(rel_def_id))
                            .unwrap(),
                    )),
                    RelParams::IndexRange(_) => todo!(),
                };

                self.get_def_type_ref(value_def_id, QLevel::Connection { rel_params })
            };

            FieldData::mandatory(
                FieldKind::ConnectionProperty(Box::new(ConnectionPropertyField {
                    rel_id,
                    first_arg: argument::FirstArg,
                    after_arg: argument::AfterArg,
                })),
                connection_ref,
            )
        } else {
            let mut unit = if let RelParams::Type(rel_def_id) = meta.relationship.rel_params {
                let rel_operator_addr = self
                    .serde_gen
                    .gen_addr_lazy(gql_serde_key(rel_def_id))
                    .unwrap();
                self.get_def_type_ref(
                    value_def_id,
                    QLevel::Edge {
                        rel_params: Some((rel_def_id, rel_operator_addr)),
                    },
                )
            } else {
                self.get_def_type_ref(value_def_id, QLevel::Node)
            };

            let value_operator_addr = self
                .serde_gen
                .gen_addr_lazy(gql_list_serde_key(value_def_id))
                .unwrap();

            if let UnitTypeRef::NativeScalar(native) = &mut unit {
                native.operator_addr = value_operator_addr;
            }

            FieldData {
                kind: property_field_producer.make_property(rel_id, value_operator_addr),
                field_type: TypeRef::mandatory(unit).to_array(Optionality::from_optional(
                    matches!(prop_cardinality, PropertyCardinality::Optional),
                )),
            }
        };

        output.insert(field_namespace.unique_literal(prop_key), field_data);
    }
}

pub(super) fn get_native_scalar_kind(
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
        | SerdeOperator::CapturingTextPattern(_)
        | SerdeOperator::Serial(_) => NativeScalarKind::String,
        SerdeOperator::IdSingletonStruct(..) => panic!("Id should not appear in GraphQL"),
        SerdeOperator::Alias(alias_op) => get_native_scalar_kind(
            serde_generator,
            serde_generator.get_operator(alias_op.inner_addr),
        ),
        op @ (SerdeOperator::AnyPlaceholder
        | SerdeOperator::Union(_)
        | SerdeOperator::Struct(_)
        | SerdeOperator::DynamicSequence
        | SerdeOperator::RelationList(_)
        | SerdeOperator::RelationIndexSet(_)
        | SerdeOperator::ConstructorSequence(_)) => panic!("not a native scalar: {:?}", NoFmt(op)),
    }
}

enum HarvestVariant<'c> {
    Object,
    FlattenedUnionInterface(FnvHashMap<RelationshipId, &'c [VariantDiscriminator]>),
    FlattenedUnionPermutation(FnvHashMap<RelationshipId, &'c VariantDiscriminator>),
}

#[derive(Clone, Copy)]
struct RelationId(DefId);
