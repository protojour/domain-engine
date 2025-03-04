use std::ops::Deref;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    DefId, OntolDefTag, PropId,
    debug::OntolDebug,
    interface::{
        discriminator::{
            Discriminant, LeafDiscriminantScalarUnion,
            leaf_discriminant_scalar_union_for_has_attribute,
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
            SerdeDef, SerdeModifier,
            operator::{SerdeOperator, SerdeStructFlags},
        },
    },
    ontology::ontol::TextConstant,
    phf::PhfIndexMap,
    property::{PropertyCardinality, ValueCardinality},
};
use thin_vec::thin_vec;
use tracing::{trace, trace_span, warn};

use crate::{
    def::{DefKind, TypeDefFlags},
    interface::serde::{SerdeKey, serde_generator::SerdeGenerator},
    misc::UnionDiscriminatorVariant,
    phf_build::build_phf_index_map,
    properties::{Property, identifies_any},
    relation::{RelParams, RelReprMeta, rel_def_meta, rel_repr_meta},
    repr::repr_model::{ReprKind, ReprScalarKind, UnionBound},
};

use super::{graphql_namespace::GraphqlNamespace, schema_builder::*};

impl<'c> SchemaBuilder<'_, '_, 'c, '_> {
    pub fn make_def_type(&mut self, def_id: DefId, level: QLevel) -> NewType {
        match level {
            QLevel::Node => self.make_node_type(def_id),
            QLevel::Edge { rel_params } => {
                let def = self.ontology_defs.def(def_id);
                let node_ref = self.gen_def_type_ref(def_id, QLevel::Node);
                match node_ref {
                    UnitTypeRef::Addr(node_type_addr) => {
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
                            let rel_edge_ref = self.gen_def_type_ref(rel_def_id, QLevel::Node);

                            let typename = self.mk_typename(|namespace, ctx| {
                                namespace.edge(Some(rel_def_id), def_id, ctx)
                            });

                            NewType::TypeData(
                                TypeData {
                                    typename,
                                    input_typename: Some(self.mk_typename(|namespace, ctx| {
                                        namespace.edge_input(Some(rel_def_id), def_id, ctx)
                                    })),
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
                                    harvest_fields: Some((
                                        rel_def_id,
                                        PropertyFieldProducer::EdgeProperty,
                                    )),
                                },
                            )
                        } else {
                            NewType::TypeData(
                                TypeData {
                                    typename: self.mk_typename(|namespace, ctx| {
                                        namespace.edge(None, def_id, ctx)
                                    }),
                                    input_typename: Some(self.mk_typename(|namespace, ctx| {
                                        namespace.edge_input(None, def_id, ctx)
                                    })),
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
                    UnitTypeRef::NativeScalar(scalar_ref) => NewType::NativeScalar(scalar_ref),
                }
            }
            QLevel::Connection { rel_params } => {
                let edge_ref = self.gen_def_type_ref(def_id, QLevel::Edge { rel_params });
                let node_ref = self.gen_def_type_ref(def_id, QLevel::Node);

                let rel_params_def_id = rel_params.map(|(def_id, _)| def_id);

                NewType::TypeData(
                    TypeData {
                        typename: self.mk_typename(|namespace, ctx| {
                            namespace.connection(rel_params_def_id, def_id, ctx)
                        }),
                        input_typename: Some(self.mk_typename(|namespace, ctx| {
                            namespace.patch_edges_input(rel_params_def_id, def_id, ctx)
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
                                                        OntolDefTag::I64.def_id(),
                                                        SerdeModifier::NONE,
                                                    )))
                                                    .unwrap(),
                                                kind: NativeScalarKind::Int(
                                                    OntolDefTag::I64.def_id(),
                                                ),
                                            }),
                                        },
                                    },
                                ),
                            ]),
                            kind: ObjectKind::Connection(ConnectionData {
                                node_type_ref: node_ref,
                            }),
                            interface: ObjectInterface::Implements(thin_vec![]),
                        }),
                    },
                    NewTypeActions::default(),
                )
            }
            QLevel::MutationResult => {
                let node_ref = self.gen_def_type_ref(def_id, QLevel::Node);

                NewType::TypeData(
                    TypeData {
                        typename: self
                            .mk_typename(|namespace, ctx| namespace.mutation_result(def_id, ctx)),
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
                                                        OntolDefTag::Boolean.def_id(),
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
        if self.prop_ctx.properties_by_def_id(def_id).is_none() {
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
                let def = self.ontology_defs.def(def_id);

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
                        typename: self
                            .mk_typename(|namespace, ctx| namespace.typename(def_id, ctx)),
                        input_typename: Some(
                            self.mk_typename(|namespace, ctx| namespace.input(def_id, ctx)),
                        ),
                        partial_input_typename: Some(
                            self.mk_typename(|namespace, ctx| namespace.partial_input(def_id, ctx)),
                        ),
                        kind: type_kind,
                    },
                    NewTypeActions {
                        harvest_fields: Some((def.id, PropertyFieldProducer::Property)),
                    },
                )
            }
            ReprKind::Union(variants, _) => {
                let def = self.ontology_defs.def(def_id);

                let mut needs_scalar = false;
                let mut type_variants = thin_vec![];

                for (variant_def_id, _) in variants {
                    match self.repr_ctx.get_repr_kind(variant_def_id) {
                        Some(ReprKind::Scalar(..)) => {
                            needs_scalar = true;
                            break;
                        }
                        _ => match self.gen_def_type_ref(*variant_def_id, QLevel::Node) {
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
                        typename: self
                            .mk_typename(|namespace, ctx| namespace.typename(def_id, ctx)),
                        input_typename: Some(
                            self.mk_typename(|namespace, ctx| namespace.union_input(def_id, ctx)),
                        ),
                        partial_input_typename: Some(self.mk_typename(|namespace, ctx| {
                            namespace.union_partial_input(def_id, ctx)
                        })),
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
                let operator_addr = self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap();

                NewType::TypeData(
                    TypeData {
                        typename: self
                            .mk_typename(|namespace, ctx| namespace.typename(def_id, ctx)),
                        input_typename: Some(
                            self.mk_typename(|namespace, ctx| namespace.input(def_id, ctx)),
                        ),
                        partial_input_typename: Some(
                            self.mk_typename(|namespace, ctx| namespace.partial_input(def_id, ctx)),
                        ),
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
            ReprKind::Scalar(..) | ReprKind::FmtStruct(..) => {
                let operator_addr = self.serde_gen.gen_addr_lazy(gql_serde_key(def_id)).unwrap();

                NewType::NativeScalar(NativeScalarRef {
                    operator_addr,
                    kind: get_native_scalar_kind(
                        self.serde_gen,
                        self.serde_gen.get_operator(operator_addr),
                    ),
                })
            }
            ReprKind::Extern | ReprKind::Macro => panic!(),
        }
    }

    pub fn harvest_fields(
        &mut self,
        type_addr: TypeAddr,
        def_id: DefId,
        property_field_producer: PropertyFieldProducer,
    ) {
        let mut interface_variants: Vec<(PropId, RelationId, &[UnionDiscriminatorVariant])> =
            Default::default();

        // See if it should be an interface of flattened unions
        if let Some(table) = self.prop_ctx.properties_table_by_def_id(def_id) {
            for (prop_id, property) in table.iter() {
                let meta = rel_repr_meta(property.rel_id, self.rel_ctx, self.defs, self.repr_ctx);
                let object = meta.relationship.object;

                if let ReprKind::Unit = meta.relation_repr_kind.deref() {
                    let union_discriminator =
                        self.misc_ctx.union_discriminators.get(&object.0).unwrap();

                    interface_variants.push((
                        *prop_id,
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
                        .map(|(prop_id, _, discriminators)| (*prop_id, *discriminators))
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
        interface_variants: &[(PropId, RelationId, &'c [UnionDiscriminatorVariant])],
        interface_variant_index: usize,
        permutations: &mut Vec<(PropId, RelationId, &'c UnionDiscriminatorVariant)>,
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
            let relation_def = self.ontology_defs.def(relation_id.0);
            let variant_def = self.ontology_defs.def(discriminator.def_id);

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

            if let Some(relation_ident) = relation_def.ident() {
                typename_append(&mut typename, &self.serde_gen.str_ctx[relation_ident]);
            }

            if let Some(variant_ident) = variant_def.ident() {
                typename_append(&mut typename, &self.serde_gen.str_ctx[variant_ident]);
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
                    .map(|(prop_id, _, discriminator)| (prop_id, discriminator.def_id))
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

        if let Some(properties) = self.prop_ctx.properties_by_def_id(def_id) {
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
                let Some(properties) = self.prop_ctx.properties_by_def_id(*member_def_id) else {
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

                let Some(properties) = self.prop_ctx.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for (rel_id, property) in table {
                    let meta = rel_def_meta(property.rel_id, self.rel_ctx, self.defs);

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
            FieldKind::EdgeProperty { id: prop_id, .. } => {
                let property = self.prop_ctx.property_by_id(*prop_id).unwrap();
                rel_def_meta(property.rel_id, self.rel_ctx, self.defs)
                    .relationship
                    .edge_projection
                    .map(|p| p.subject.0)
                    .unwrap_or(0)
            }
            // Connections are placed after "plain" fields
            FieldKind::ConnectionProperty(field) => {
                let property = self.prop_ctx.property_by_id(field.prop_id).unwrap();
                100 + rel_def_meta(property.rel_id, self.rel_ctx, self.defs)
                    .relationship
                    .edge_projection
                    .map(|p| p.subject.0)
                    .unwrap_or(0)
            }
            _ => 255,
        }
    }

    fn harvest_struct_field(
        &mut self,
        prop_id: PropId,
        property: &Property,
        property_field_producer: PropertyFieldProducer,
        field_namespace: &mut GraphqlNamespace,
        harvest_variant: &HarvestVariant,
        output: &mut IndexMap<String, FieldData>,
    ) {
        let meta = rel_repr_meta(property.rel_id, self.rel_ctx, self.defs, self.repr_ctx);
        match (meta.relation_repr_kind.deref(), harvest_variant) {
            (ReprKind::Scalar(_, ReprScalarKind::TextConstant(lit_def_id), _), _) => {
                let literal = self.defs.text_literal(*lit_def_id).unwrap();
                self.harvest_data_struct_field(
                    (prop_id, literal, property),
                    meta,
                    property_field_producer,
                    field_namespace,
                    output,
                )
            }
            (ReprKind::Unit, HarvestVariant::FlattenedUnionInterface(discriminator_table)) => {
                let variants = discriminator_table.get(&prop_id).unwrap();

                let mut prop_keys: FnvHashSet<TextConstant> = Default::default();
                for variant in variants.iter() {
                    if let Discriminant::HasAttribute(_, prop_key, ..) = &variant.discriminant {
                        prop_keys.insert(*prop_key);
                    }
                }

                let scalar_union = leaf_discriminant_scalar_union_for_has_attribute(
                    variants.iter().map(|v| &v.discriminant),
                );

                if prop_keys.len() == 1 {
                    let unit_type_ref = if scalar_union == LeafDiscriminantScalarUnion::TEXT {
                        UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_addr: self
                                .serde_gen
                                .gen_addr_lazy(gql_serde_key(OntolDefTag::Text.def_id()))
                                .unwrap(),
                            kind: NativeScalarKind::String,
                        })
                    } else if scalar_union == LeafDiscriminantScalarUnion::INT {
                        self.gen_def_type_ref(OntolDefTag::I64.def_id(), QLevel::Node)
                    } else {
                        UnitTypeRef::Addr(self.schema.json_scalar)
                    };

                    let prop_key = &self.serde_gen.str_ctx[prop_keys.into_iter().next().unwrap()];

                    let resolvers: FnvHashMap<DefId, PropId> = variants
                        .iter()
                        .filter_map(|variant| match &variant.discriminant {
                            Discriminant::HasAttribute(prop_id, ..) => {
                                Some((variant.def_id, *prop_id))
                            }
                            _ => None,
                        })
                        .collect();

                    output.insert(
                        field_namespace.unique_literal(prop_key),
                        FieldData {
                            kind: FieldKind::FlattenedPropertyDiscriminator {
                                proxy: prop_id,
                                resolvers: Box::new(resolvers),
                            },
                            field_type: TypeRef::mandatory(unit_type_ref),
                        },
                    );
                } else {
                    warn!("No uniform discriminator property");
                }
            }
            (ReprKind::Unit, HarvestVariant::FlattenedUnionPermutation(discriminator_table)) => {
                // concrete type for a specific permutation of flattened union variants

                let union_discriminator = discriminator_table.get(&prop_id).unwrap();

                let Some(table) = self
                    .prop_ctx
                    .properties_table_by_def_id(union_discriminator.def_id)
                else {
                    return;
                };

                for (inner_property_id, property) in table {
                    // don't include the discriminator property in the permuted type,
                    // it belongs to the interface.
                    self.harvest_struct_field(
                        *inner_property_id,
                        property,
                        PropertyFieldProducer::FlattenedProperty(prop_id),
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
        (prop_id, prop_key, _property): (PropId, &str, &Property),
        meta: RelReprMeta,
        property_field_producer: PropertyFieldProducer,
        field_namespace: &mut GraphqlNamespace,
        output: &mut IndexMap<String, FieldData>,
    ) {
        let (_, (prop_cardinality, value_cardinality), _) = meta.relationship.subject();
        let (value_def_id, ..) = meta.relationship.object();
        trace!(
            "    harvest data struct field `{prop_key}`: {prop_id} ({value_def_id:?}) ({prop_cardinality:?}, {value_cardinality:?})"
        );

        let value_properties = self.prop_ctx.properties_by_def_id(value_def_id);

        let is_entity_value = {
            let repr_kind = self.repr_ctx.get_repr_kind(&value_def_id);
            match repr_kind {
                Some(ReprKind::Union(variants, UnionBound::Struct)) => {
                    variants.iter().all(|(variant_def_id, _)| {
                        let variant_properties =
                            self.prop_ctx.properties_by_def_id(*variant_def_id);
                        variant_properties
                            .map(|properties| properties.identified_by.is_some())
                            .unwrap_or(false)
                    })
                }
                Some(ReprKind::Scalar(_, ReprScalarKind::Vertex, _)) => true,
                _ => value_properties
                    .map(|properties| properties.identified_by.is_some())
                    .unwrap_or(false),
            }
        };

        let field_data = if matches!(value_cardinality, ValueCardinality::Unit) {
            let mut modifier = TypeModifier::new_unit(Optionality::from_optional(matches!(
                prop_cardinality,
                PropertyCardinality::Optional
            )));

            let value_operator_addr = self
                .serde_gen
                .gen_addr_lazy(gql_serde_key(value_def_id))
                .unwrap();

            let field_type = if identifies_any(value_def_id, self.prop_ctx, self.repr_ctx) {
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
                    RelParams::Def(rel_def_id) => {
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

                let unit = self.gen_def_type_ref(value_def_id, qlevel);

                // If the field is a union, it should be optional.
                // The reason is that the client explicitly needs to select its variants,
                // and when the runtime variant is not selected, NULL should be returned.
                if let UnitTypeRef::Addr(addr) = &unit {
                    if matches!(self.schema.type_data(*addr).kind, TypeKind::Union(_)) {
                        match &mut modifier {
                            TypeModifier::Unit(optionality) => {
                                *optionality = Optionality::Optional;
                            }
                            TypeModifier::Array { element, .. } => {
                                *element = Optionality::Optional;
                            }
                        }
                    }
                }

                TypeRef { modifier, unit }
            };

            FieldData {
                kind: property_field_producer.make_property(prop_id, value_operator_addr),
                field_type,
            }
        } else if is_entity_value {
            let connection_ref = {
                let rel_params = match meta.relationship.rel_params {
                    RelParams::Unit => None,
                    RelParams::Def(rel_def_id) => Some((
                        rel_def_id,
                        self.serde_gen
                            .gen_addr_lazy(gql_serde_key(rel_def_id))
                            .unwrap(),
                    )),
                    RelParams::IndexRange(_) => todo!(),
                };

                self.gen_def_type_ref(value_def_id, QLevel::Connection { rel_params })
            };

            FieldData::mandatory(
                FieldKind::ConnectionProperty(Box::new(ConnectionPropertyField {
                    prop_id,
                    first_arg: argument::FirstArg,
                    after_arg: argument::AfterArg,
                })),
                connection_ref,
            )
        } else {
            let mut unit = if let RelParams::Def(rel_def_id) = meta.relationship.rel_params {
                let rel_operator_addr = self
                    .serde_gen
                    .gen_addr_lazy(gql_serde_key(rel_def_id))
                    .unwrap();
                self.gen_def_type_ref(
                    value_def_id,
                    QLevel::Edge {
                        rel_params: Some((rel_def_id, rel_operator_addr)),
                    },
                )
            } else {
                self.gen_def_type_ref(value_def_id, QLevel::Node)
            };

            let value_operator_addr = self
                .serde_gen
                .gen_addr_lazy(gql_list_serde_key(value_def_id))
                .unwrap();

            if let UnitTypeRef::NativeScalar(native) = &mut unit {
                native.operator_addr = value_operator_addr;
            }

            FieldData {
                kind: property_field_producer.make_property(prop_id, value_operator_addr),
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
        SerdeOperator::Octets(octets_op) => {
            if octets_op.target_def_id == OntolDefTag::Vertex.def_id() {
                NativeScalarKind::ID
            } else {
                NativeScalarKind::String
            }
        }
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
        | SerdeOperator::ConstructorSequence(_)) => {
            panic!("not a native scalar: {:?}", op.debug(&()))
        }
    }
}

enum HarvestVariant<'c> {
    Object,
    FlattenedUnionInterface(FnvHashMap<PropId, &'c [UnionDiscriminatorVariant]>),
    FlattenedUnionPermutation(FnvHashMap<PropId, &'c UnionDiscriminatorVariant>),
}

#[derive(Clone, Copy)]
struct RelationId(DefId);
