use fnv::FnvHashMap;
use indoc::indoc;
use itertools::Itertools;
use ontol_runtime::{
    interface::{
        serde::{SerdeDef, SerdeModifier},
        DomainInterface,
    },
    ontology::{
        domain::{
            self, BasicDef, DataRelationshipInfo, DataRelationshipKind, DataRelationshipSource,
            DataRelationshipTarget, Def, Domain, EdgeCardinal, EdgeCardinalFlags,
            EdgeCardinalProjection, EdgeInfo, Entity,
        },
        map::MapMeta,
        ontol::{OntolDomainMeta, TextConstant, TextLikeType},
        Ontology,
    },
    property::ValueCardinality,
    rustdoc::RustDoc,
    DefId, DefIdSet, EdgeId, PackageId, RelationshipId,
};
use std::{
    collections::{BTreeSet, HashMap},
    ops::Deref,
    sync::Arc,
};

use crate::{
    def::{
        rel_def_meta, rel_repr_meta, BuiltinRelationKind, DefKind, RelDefMeta, RelParams, TypeDef,
        TypeDefFlags,
    },
    interface::{
        graphql::generate_schema::generate_graphql_schema,
        serde::{serde_generator::SerdeGenerator, SerdeKey, EDGE_PROPERTY},
    },
    namespace::Space,
    package::ONTOL_PKG,
    primitive::PrimitiveKind,
    relation::{Properties, UnionMemberCache},
    repr::repr_model::{ReprKind, ReprScalarKind},
    strings::StringCtx,
    Compiler,
};

impl<'m> Compiler<'m> {
    pub(crate) fn into_ontology_inner(mut self) -> Ontology {
        let package_ids = self.package_ids();
        let unique_domain_names = self.unique_domain_names();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut package_config_table = std::mem::take(&mut self.package_config_table);
        let mut docs_table = std::mem::take(&mut self.namespaces.docs);

        for def_id in self.defs.iter_package_def_ids(ONTOL_PKG) {
            #[allow(clippy::single_match)]
            match self.defs.def_kind(def_id) {
                DefKind::Primitive(kind, _ident) => {
                    if let Some(field_docs) = PrimitiveKind::get_field_rustdoc(kind) {
                        docs_table.insert(def_id, field_docs.to_string());
                    }
                }
                DefKind::BuiltinRelType(kind, _ident) => {
                    if let Some(field_docs) = BuiltinRelationKind::get_field_rustdoc(kind) {
                        docs_table.insert(def_id, field_docs.to_string());
                    }
                }
                DefKind::Type(type_def) => {
                    if type_def.flags.contains(TypeDefFlags::BUILTIN_SYMBOL) {
                        let ident = type_def.ident.unwrap();

                        // TODO: Structured documentation of `is`-relations.
                        // Then a prose-based documentation string will probably be superfluous?
                        docs_table.insert(
                            def_id,
                            format!(
                                indoc! {"
                                    The [symbol](def.md#symbol) `'{ident}'`.
                                    Used in [ordering](interfaces.md#ordering).
                                    ```ontol
                                    direction: {ident}
                                    ```
                                    "
                                },
                                ident = ident
                            ),
                        );
                    } else if let Some(slt) = self.defs.string_like_types.get(&def_id) {
                        if let Some(field_docs) = TextLikeType::get_field_rustdoc(slt) {
                            docs_table.insert(def_id, field_docs.to_string());
                        }
                    }
                }
                _ => {}
            }
        }

        let union_member_cache = {
            let mut cache: FnvHashMap<DefId, BTreeSet<DefId>> = Default::default();

            for package_id in package_ids.iter() {
                let namespace = namespaces.get(package_id).unwrap();

                for (_, union_def_id) in &namespace.types {
                    let Some(ReprKind::StructUnion(variants)) =
                        self.repr_ctx.get_repr_kind(union_def_id)
                    else {
                        continue;
                    };

                    for (variant, _) in variants.iter() {
                        cache.entry(*variant).or_default().insert(*union_def_id);
                    }
                }
            }
            UnionMemberCache { cache }
        };

        let mut str_ctx = self.str_ctx.detach();
        let mut serde_gen = self.serde_generator(&mut str_ctx, &union_member_cache);
        let mut builder = Ontology::builder();
        let mut ontology_union_variants: FnvHashMap<DefId, BTreeSet<DefId>> = Default::default();

        let dynamic_sequence_operator_addr = serde_gen.make_dynamic_sequence_addr();

        let map_namespaces: FnvHashMap<_, _> = namespaces
            .iter_mut()
            .map(|(package_id, namespace)| {
                (*package_id, std::mem::take(namespace.space_mut(Space::Map)))
            })
            .collect();

        // For now, create serde operators for every domain
        for package_id in package_ids.iter().cloned() {
            let domain_def_id = self.package_def_ids.get(&package_id).cloned().unwrap();
            let domain_name = *unique_domain_names
                .get(&package_id)
                .expect("Anonymous domain");
            let mut domain = Domain::new(domain_def_id, domain_name);

            let namespace = namespaces.remove(&package_id).unwrap();
            let type_namespace = namespace.types;

            if let Some(package_config) = package_config_table.remove(&package_id) {
                builder.add_package_config(package_id, package_config);
            }

            let mut edges: FnvHashMap<EdgeId, EdgeInfo> = Default::default();

            for type_def_id in self.defs.iter_package_def_ids(package_id) {
                if let Some(ReprKind::Union(members) | ReprKind::StructUnion(members)) =
                    self.repr_ctx.get_repr_kind(&type_def_id)
                {
                    ontology_union_variants.insert(
                        type_def_id,
                        members.iter().map(|(member, _)| *member).collect(),
                    );
                }
            }

            for (type_name, type_def_id) in type_namespace {
                let type_name_constant = serde_gen.str_ctx.intern_constant(type_name);
                let data_relationships = self.collect_relationships_and_edges(
                    type_def_id,
                    &union_member_cache,
                    &mut edges,
                    serde_gen.str_ctx,
                );
                let def_kind = self.defs.def_kind(type_def_id);

                domain.add_def(Def {
                    id: type_def_id,
                    public: match def_kind {
                        DefKind::Type(TypeDef { flags, .. }) => {
                            flags.contains(TypeDefFlags::PUBLIC)
                        }
                        _ => true,
                    },
                    kind: match self.domain_entity(
                        type_def_id,
                        type_name_constant,
                        &mut serde_gen,
                        &data_relationships,
                    ) {
                        Some(entity) => domain::DefKind::Entity(entity),
                        None => def_kind.as_ontology_type_kind(BasicDef {
                            name: Some(type_name_constant),
                        }),
                    },
                    operator_addr: serde_gen.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    store_key: Some(
                        self.edge_ctx
                            .store_keys
                            .get(&type_def_id)
                            .copied()
                            .unwrap_or(type_name_constant),
                    ),
                    data_relationships,
                });
            }

            for type_def_id in namespace.anonymous {
                domain.add_def(Def {
                    id: type_def_id,
                    public: false,
                    kind: self
                        .defs
                        .def_kind(type_def_id)
                        .as_ontology_type_kind(BasicDef { name: None }),
                    operator_addr: serde_gen.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    store_key: self.edge_ctx.store_keys.get(&type_def_id).copied(),
                    data_relationships: self.collect_relationships_and_edges(
                        type_def_id,
                        &union_member_cache,
                        &mut edges,
                        serde_gen.str_ctx,
                    ),
                });
            }

            if package_id == ONTOL_PKG {
                for def_id in self.defs.text_literals.values().copied() {
                    domain.add_def(Def {
                        id: def_id,
                        public: false,
                        kind: domain::DefKind::Data(BasicDef { name: None }),
                        operator_addr: None,
                        store_key: None,
                        data_relationships: Default::default(),
                    });
                }
            }

            for (edge_id, edge) in &self.edge_ctx.symbolic_edges {
                if edge_id.0.package_id() != package_id {
                    continue;
                }

                let mut edge_info = EdgeInfo {
                    cardinals: Vec::with_capacity(edge.variables.len()),
                    store_key: None,
                };

                for variable in edge.variables.values() {
                    let entity_count = variable
                        .members
                        .keys()
                        .filter(|def_id| self.entity_ctx.entities.contains_key(def_id))
                        .count();

                    let mut flags = EdgeCardinalFlags::empty();

                    if entity_count == variable.members.len() {
                        flags.insert(EdgeCardinalFlags::ENTITY);

                        if variable.one_to_one_count > 0 {
                            flags.insert(EdgeCardinalFlags::UNIQUE);
                        }
                    } else if entity_count > 0 {
                        panic!("FIXME: mix of entity/non-entity");
                    };

                    edge_info.cardinals.push(EdgeCardinal {
                        target: variable.members.keys().copied().collect(),
                        flags,
                    });
                }

                let old_edge = edges.insert(*edge_id, edge_info);
                assert!(old_edge.is_none());
            }

            domain.set_edges(edges.into_iter());
            builder.add_domain(package_id, domain);
        }

        // interface handling
        let domain_interfaces = {
            let mut interfaces: FnvHashMap<PackageId, Vec<DomainInterface>> = Default::default();
            for package_id in package_ids.iter().cloned() {
                if package_id == ONTOL_PKG {
                    continue;
                }

                if let Some(schema) = generate_graphql_schema(
                    package_id,
                    builder.partial_ontology(),
                    &self.primitives,
                    map_namespaces.get(&package_id),
                    &self.code_ctx,
                    &self.resolver_graph,
                    &union_member_cache,
                    &mut serde_gen,
                ) {
                    interfaces
                        .entry(package_id)
                        .or_default()
                        .push(DomainInterface::GraphQL(Arc::new(schema)));
                }
            }
            interfaces
        };

        let (serde_operators, _) = serde_gen.finish();

        let mut property_flows = vec![];

        let map_meta_table = self
            .code_ctx
            .result_map_proc_table
            .into_iter()
            .map(|(key, procedure)| {
                let propflow_range = if let Some(current_prop_flows) =
                    self.code_ctx.result_propflow_table.remove(&key)
                {
                    let start: u32 = property_flows.len().try_into().unwrap();
                    let len: u32 = current_prop_flows.len().try_into().unwrap();
                    property_flows.extend(current_prop_flows);
                    Some(start..(start + len))
                } else {
                    None
                };

                let metadata = self
                    .code_ctx
                    .result_metadata_table
                    .remove(&key)
                    .unwrap_or_else(|| panic!("metadata not found for {key:?}"));

                (
                    key,
                    MapMeta {
                        procedure,
                        propflow_range,
                        direction: metadata.direction,
                        lossiness: metadata.lossiness,
                    },
                )
            })
            .collect();

        let docs = docs_table
            .into_iter()
            .map(|(def_id, docs)| (def_id, str_ctx.intern_constant(&docs)))
            .collect();

        builder
            .text_constants(str_ctx.into_arcstr_vec())
            .ontol_domain_meta(OntolDomainMeta {
                bool: self.primitives.bool,
                i64: self.primitives.i64,
                f64: self.primitives.f64,
                text: self.primitives.text,
                ascending: self.primitives.symbols.ascending,
                descending: self.primitives.symbols.descending,
                open_data_relationship: self.primitives.open_data_relationship,
                order_relationship: self.primitives.relations.order,
                direction_relationship: self.primitives.relations.direction,
                edge_property: EDGE_PROPERTY.into(),
            })
            .union_variants(
                ontology_union_variants
                    .into_iter()
                    .map(|(union_id, set)| (union_id, DefIdSet::from(set)))
                    .collect(),
            )
            .extended_entity_info(self.entity_ctx.entities)
            .lib(self.code_ctx.result_lib)
            .docs(docs)
            .const_procs(self.code_ctx.result_const_procs)
            .map_meta_table(map_meta_table)
            .static_conditions(self.code_ctx.result_static_conditions)
            .named_forward_maps(self.code_ctx.result_named_downmaps)
            .serde_operators(serde_operators)
            .dynamic_sequence_operator_addr(dynamic_sequence_operator_addr)
            .property_flows(property_flows)
            .string_like_types(self.defs.string_like_types)
            .text_patterns(self.text_patterns.text_patterns)
            .externs(self.def_ty_ctx.ontology_externs)
            .value_generators(self.rel_ctx.value_generators)
            .domain_interfaces(domain_interfaces)
            .build()
    }

    fn collect_relationships_and_edges(
        &self,
        type_def_id: DefId,
        union_member_cache: &UnionMemberCache,
        edges: &mut FnvHashMap<EdgeId, EdgeInfo>,
        str_ctx: &mut StringCtx<'m>,
    ) -> FnvHashMap<RelationshipId, DataRelationshipInfo> {
        let mut relationships = FnvHashMap::default();
        self.collect_inherent_relationships_and_edges(
            type_def_id,
            &mut relationships,
            edges,
            str_ctx,
        );

        if let Some(ReprKind::StructIntersection(members)) =
            self.repr_ctx.get_repr_kind(&type_def_id)
        {
            for (member_def_id, _) in members {
                self.collect_inherent_relationships_and_edges(
                    *member_def_id,
                    &mut relationships,
                    edges,
                    str_ctx,
                );
            }
        }

        if let Some(union_memberships) = union_member_cache.cache.get(&type_def_id) {
            for union_def_id in union_memberships {
                let Some(properties) = self.rel_ctx.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for property_id in table.keys() {
                    self.collect_prop_relationship_and_edge(
                        *property_id,
                        DataRelationshipSource::ByUnionProxy,
                        &mut relationships,
                        edges,
                        str_ctx,
                    );
                }
            }
        }

        relationships
    }

    fn collect_inherent_relationships_and_edges(
        &self,
        type_def_id: DefId,
        relationships: &mut FnvHashMap<RelationshipId, DataRelationshipInfo>,
        edges: &mut FnvHashMap<EdgeId, EdgeInfo>,
        str_ctx: &mut StringCtx<'m>,
    ) {
        let Some(properties) = self.rel_ctx.properties_by_def_id(type_def_id) else {
            return;
        };
        if let Some(table) = &properties.table {
            for rel_id in table.keys() {
                self.collect_prop_relationship_and_edge(
                    *rel_id,
                    DataRelationshipSource::Inherent,
                    relationships,
                    edges,
                    str_ctx,
                );
            }
        }
    }

    fn collect_prop_relationship_and_edge(
        &self,
        rel_id: RelationshipId,
        source: DataRelationshipSource,
        relationships: &mut FnvHashMap<RelationshipId, DataRelationshipInfo>,
        edges: &mut FnvHashMap<EdgeId, EdgeInfo>,
        str_ctx: &mut StringCtx<'m>,
    ) {
        let meta = rel_repr_meta(rel_id, &self.defs, &self.repr_ctx);

        let (source_def_id, _, _) = meta.relationship.subject();
        let (target_def_id, _, _) = meta.relationship.object();

        let Some(target_repr_kind) = self.repr_ctx.get_repr_kind(&target_def_id) else {
            return;
        };
        let name = match meta.relation_repr_kind.deref() {
            ReprKind::Scalar(_, ReprScalarKind::TextConstant(constant_def_id), _) => {
                let lit = self.defs.text_literal(*constant_def_id).unwrap();
                str_ctx.intern_constant(lit)
            }
            ReprKind::Unit => {
                // FIXME: This doesn't _really_ have a subject "name". It represents a flattened structure:
                str_ctx.intern_constant("")
            }
            _ => return,
        };

        let edge_params = match meta.relationship.rel_params {
            RelParams::Type(def_id) => Some(def_id),
            RelParams::Unit | RelParams::IndexRange(_) => None,
        };

        let edge_id = meta.relationship.projection.id;
        let edge_projection = meta.relationship.projection;

        let (data_relationship_kind, target) = match target_repr_kind {
            ReprKind::Union(members) | ReprKind::StructUnion(members) => {
                let kinds_and_targets = members
                    .iter()
                    .map(|(member_def_id, _)| {
                        self.data_relationship_kind_and_target(
                            rel_id,
                            edge_projection,
                            source_def_id,
                            *member_def_id,
                        )
                    })
                    .collect_vec();

                let target = DataRelationshipTarget::Union(target_def_id);
                if kinds_and_targets
                    .iter()
                    .all(|(kind, _)| matches!(kind, DataRelationshipKind::Edge(_)))
                {
                    (DataRelationshipKind::Edge(edge_projection), target)
                } else {
                    (
                        DataRelationshipKind::Tree,
                        DataRelationshipTarget::Union(target_def_id),
                    )
                }
            }
            _ => self.data_relationship_kind_and_target(
                rel_id,
                edge_projection,
                source_def_id,
                target_def_id,
            ),
        };

        // collect edge
        if let DataRelationshipKind::Edge(_) = &data_relationship_kind {
            if !self.edge_ctx.symbolic_edges.contains_key(&edge_id) {
                // fallback/legacy mode:
                let edge_info = edges.entry(edge_id).or_insert_with(|| EdgeInfo {
                    cardinals: vec![],
                    store_key: self.edge_ctx.store_keys.get(&edge_id.0).copied(),
                });

                if edge_info.cardinals.is_empty() {
                    // initialize edge cardinals first
                    for i in 0_u8..2 {
                        let mut flags = EdgeCardinalFlags::ENTITY;

                        if i == edge_projection.subject.0 {
                            if matches!(
                                meta.relationship.object_cardinality.1,
                                ValueCardinality::Unit
                            ) {
                                flags.insert(EdgeCardinalFlags::UNIQUE)
                            };

                            edge_info.cardinals.push(EdgeCardinal {
                                target: DefIdSet::from_iter([
                                    self.identifier_to_vertex_def_id(source_def_id)
                                ]),
                                flags,
                            });
                        } else {
                            if matches!(
                                meta.relationship.subject_cardinality.1,
                                ValueCardinality::Unit
                            ) {
                                flags.insert(EdgeCardinalFlags::UNIQUE);
                            }

                            edge_info.cardinals.push(EdgeCardinal {
                                target: DefIdSet::from_iter([
                                    self.identifier_to_vertex_def_id(target_def_id)
                                ]),
                                flags,
                            });
                        }
                    }

                    if let Some(edge_params) = edge_params {
                        edge_info.cardinals.resize_with(3, || EdgeCardinal {
                            target: DefIdSet::from_iter([edge_params]),
                            flags: EdgeCardinalFlags::empty(),
                        });
                    }
                }

                // replace cardinal target with union members if union was found
                if let DataRelationshipTarget::Union(union_def_id) = &target {
                    let cardinal_target =
                        &mut edge_info.cardinals[edge_projection.object.0 as usize].target;

                    cardinal_target.clear();

                    let Some(ReprKind::Union(members) | ReprKind::StructUnion(members)) =
                        self.repr_ctx.get_repr_kind(union_def_id)
                    else {
                        panic!("not a union");
                    };

                    for (member, _span) in members {
                        cardinal_target.insert(*member);
                    }
                }
            }
        }

        // collect relationship
        relationships.insert(
            rel_id,
            DataRelationshipInfo {
                name,
                kind: data_relationship_kind,
                cardinality: meta.relationship.subject_cardinality,
                source,
                target,
            },
        );
    }

    fn data_relationship_kind_and_target(
        &self,
        rel_id: RelationshipId,
        edge_projection: EdgeCardinalProjection,
        source_def_id: DefId,
        target_def_id: DefId,
    ) -> (DataRelationshipKind, DataRelationshipTarget) {
        let target_properties = self.rel_ctx.properties_by_def_id(target_def_id);

        if let Some(identifies) = target_properties.and_then(|p| p.identifies) {
            let meta = rel_def_meta(identifies, &self.defs);
            if meta.relationship.object.0 == source_def_id {
                (
                    DataRelationshipKind::Id,
                    DataRelationshipTarget::Unambiguous(target_def_id),
                )
            } else if self
                .rel_ctx
                .properties_by_def_id(source_def_id)
                // It can be an edge (for now) only the the source (subject) is an entity.
                // i.e. there is no support for "indirect" edge properties
                .map(|props| props.identified_by.is_some())
                .unwrap_or(false)
            {
                (
                    DataRelationshipKind::Edge(edge_projection),
                    DataRelationshipTarget::Unambiguous(target_def_id),
                )
            } else {
                (
                    DataRelationshipKind::Tree,
                    DataRelationshipTarget::Unambiguous(target_def_id),
                )
            }
        } else if target_properties.and_then(|p| p.identified_by).is_some() {
            (
                DataRelationshipKind::Edge(edge_projection),
                DataRelationshipTarget::Unambiguous(target_def_id),
            )
        } else {
            let source_properties = self.rel_ctx.properties_by_def_id(source_def_id);
            let is_entity_id = source_properties
                .map(|properties| properties.identified_by == Some(rel_id))
                .unwrap_or(false);

            let kind = if is_entity_id {
                DataRelationshipKind::Id
            } else {
                DataRelationshipKind::Tree
            };

            (kind, DataRelationshipTarget::Unambiguous(target_def_id))
        }
    }

    fn domain_entity(
        &self,
        type_def_id: DefId,
        name: TextConstant,
        serde_generator: &mut SerdeGenerator,
        data_relationships: &FnvHashMap<RelationshipId, DataRelationshipInfo>,
    ) -> Option<Entity> {
        let properties = self.rel_ctx.properties_by_def_id(type_def_id)?;
        let id_relationship_id = properties.identified_by?;

        let identifies_meta = rel_def_meta(id_relationship_id, &self.defs);

        // inherent properties are the properties that are _not_ entity relationships:
        let mut inherent_property_count = 0;

        for data_relationship in data_relationships.values() {
            if matches!(
                data_relationship.kind,
                DataRelationshipKind::Tree | DataRelationshipKind::Id
            ) {
                inherent_property_count += 1;
            }
        }

        let inherent_primary_id_meta = self.find_inherent_primary_id(type_def_id, properties);

        let id_value_generator = if let Some(inherent_primary_id_meta) = &inherent_primary_id_meta {
            self.rel_ctx
                .value_generators
                .get(&inherent_primary_id_meta.rel_id)
                .cloned()
        } else {
            None
        };

        Some(Entity {
            name,
            // The entity is self-identifying if it has an inherent primary_id and that is its only inherent property.
            // TODO: Is the entity still self-identifying if it has only an external primary id (i.e. it is a unit type)?
            is_self_identifying: inherent_primary_id_meta.is_some() && inherent_property_count <= 1,
            id_relationship_id: match inherent_primary_id_meta {
                Some(inherent_meta) => inherent_meta.rel_id,
                None => id_relationship_id,
            },
            id_value_def_id: identifies_meta.relationship.subject.0,
            id_value_generator,
            id_operator_addr: serde_generator
                .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                    identifies_meta.relationship.subject.0,
                    SerdeModifier::NONE,
                )))
                .unwrap(),
        })
    }

    fn identifier_to_vertex_def_id(&self, def_id: DefId) -> DefId {
        if let Some(properties) = self.rel_ctx.properties_by_def_id(def_id) {
            if let Some(identifies_rel) = properties.identifies {
                let meta = rel_def_meta(identifies_rel, &self.defs);
                return meta.relationship.object.0;
            }
        }

        def_id
    }

    fn find_inherent_primary_id(
        &self,
        _entity_id: DefId,
        properties: &Properties,
    ) -> Option<RelDefMeta<'_, 'm>> {
        let id_relationship_id = properties.identified_by?;
        let inherent_id = self
            .rel_ctx
            .inherent_id_map
            .get(&id_relationship_id)
            .cloned()?;
        let map = properties.table.as_ref()?;
        let _property = map.get(&inherent_id)?;

        Some(rel_def_meta(inherent_id, &self.defs))
    }

    fn unique_domain_names(&self) -> FnvHashMap<PackageId, TextConstant> {
        let mut map: HashMap<TextConstant, PackageId> = HashMap::new();
        map.insert(self.str_ctx.get_constant("ontol").unwrap(), ONTOL_PKG);

        for (package_id, name) in self.package_names.iter().cloned() {
            if map.contains_key(&name) {
                todo!(
                    "Two distinct domains are called `{name}`. This is not handled yet",
                    name = &self[name]
                );
            }

            map.insert(name, package_id);
        }

        // invert
        map.into_iter()
            .map(|(name, package_id)| (package_id, name))
            .collect()
    }
}
