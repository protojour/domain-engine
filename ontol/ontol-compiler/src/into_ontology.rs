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
            DataRelationshipTarget, Def, DefRepr, DefReprUnionBound, Domain, EdgeCardinal,
            EdgeCardinalFlags, EdgeCardinalProjection, EdgeInfo, Entity,
        },
        map::MapMeta,
        ontol::{OntolDomainMeta, TextConstant, TextLikeType, ValueGenerator},
        Ontology,
    },
    rustdoc::RustDoc,
    DefId, DefIdSet, FnvIndexMap, PackageId, PropId,
};
use std::{
    collections::{BTreeSet, HashMap},
    ops::Deref,
    sync::Arc,
};

use crate::{
    def::{BuiltinRelationKind, DefKind, TypeDef, TypeDefFlags},
    edge::{CardinalKind, EdgeId},
    interface::{
        graphql::generate_schema::generate_graphql_schema,
        httpjson::generate_httpjson_interface,
        serde::{serde_generator::SerdeGenerator, SerdeKey, EDGE_PROPERTY},
    },
    namespace::{DocId, Space},
    package::ONTOL_PKG,
    primitive::PrimitiveKind,
    properties::Properties,
    relation::{rel_def_meta, rel_repr_meta, RelDefMeta, RelId, Relationship, UnionMemberCache},
    repr::repr_model::{ReprKind, ReprScalarKind, UnionBound},
    strings::StringCtx,
    Compiler,
};

impl<'m> Compiler<'m> {
    pub(crate) fn into_ontology_inner(mut self) -> Ontology {
        let package_ids: Vec<PackageId> = self.domain_ids.keys().copied().collect();
        let unique_domain_names = self.unique_domain_names();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut package_config_table = std::mem::take(&mut self.package_config_table);
        let mut docs_table = std::mem::take(&mut self.namespaces.docs);

        for def_id in self.defs.iter_package_def_ids(ONTOL_PKG) {
            #[allow(clippy::single_match)]
            match self.defs.def_kind(def_id) {
                DefKind::Primitive(kind, _ident) => {
                    if let Some(field_docs) = PrimitiveKind::get_field_rustdoc(kind) {
                        docs_table.insert(DocId::Def(def_id), field_docs);
                    }
                }
                DefKind::BuiltinRelType(kind, _ident) => {
                    if let Some(field_docs) = BuiltinRelationKind::get_field_rustdoc(kind) {
                        docs_table.insert(DocId::Def(def_id), field_docs);
                    }
                }
                DefKind::Type(type_def) => {
                    if type_def.flags.contains(TypeDefFlags::BUILTIN_SYMBOL) {
                        let ident = type_def.ident.unwrap();

                        // TODO: Structured documentation of `is`-relations.
                        // Then a prose-based documentation string will probably be superfluous?
                        docs_table.insert(
                            DocId::Def(def_id),
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
                            )
                            .into(),
                        );
                    } else if let Some(slt) = self.defs.text_like_types.get(&def_id) {
                        if let Some(field_docs) = TextLikeType::get_field_rustdoc(slt) {
                            docs_table.insert(DocId::Def(def_id), field_docs);
                        }
                    }
                }
                _ => {}
            }
        }

        let union_member_cache = {
            let mut cache: FnvHashMap<DefId, BTreeSet<DefId>> = Default::default();

            for package_id in package_ids.iter() {
                let domain_def_id = self.package_def_ids.get(package_id).cloned().unwrap();
                let namespace = namespaces.get(&domain_def_id).unwrap();

                for (_, union_def_id) in &namespace.types {
                    let Some(ReprKind::Union(variants, UnionBound::Struct)) =
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
            let mut domain = Domain::new(
                self.domain_ids.get(&package_id).cloned().unwrap(),
                domain_def_id,
                domain_name,
            );

            let namespace = namespaces.remove(&domain_def_id).unwrap();
            let type_namespace = namespace.types;

            if let Some(package_config) = package_config_table.remove(&package_id) {
                builder.add_package_config(package_id, package_config);
            }

            for type_def_id in self.defs.iter_package_def_ids(package_id) {
                if let Some(ReprKind::Union(members, _bound)) =
                    self.repr_ctx.get_repr_kind(&type_def_id)
                {
                    ontology_union_variants.insert(
                        type_def_id,
                        members.iter().map(|(member, _)| *member).collect(),
                    );
                }
            }

            for (def_ident, type_def_id) in type_namespace {
                let def_ident_constant = serde_gen.str_ctx.intern_constant(def_ident);
                let data_relationships = self.collect_relationships_and_edges(
                    type_def_id,
                    &union_member_cache,
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
                    kind: self.namespaced_to_ontology_def_kind(
                        type_def_id,
                        def_kind,
                        def_ident_constant,
                        &mut serde_gen,
                        &data_relationships,
                    ),
                    operator_addr: serde_gen.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    store_key: Some(
                        self.edge_ctx
                            .store_keys
                            .get(&type_def_id)
                            .copied()
                            .unwrap_or(def_ident_constant),
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
                        .as_ontology_type_kind(BasicDef {
                            ident: None,
                            repr: self
                                .repr_ctx
                                .get_repr_kind(&type_def_id)
                                .map(|repr_kind| make_def_repr(repr_kind, &mut serde_gen))
                                .unwrap_or(DefRepr::Unknown),
                        }),
                    operator_addr: serde_gen.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    store_key: self.edge_ctx.store_keys.get(&type_def_id).copied(),
                    data_relationships: self.collect_relationships_and_edges(
                        type_def_id,
                        &union_member_cache,
                        serde_gen.str_ctx,
                    ),
                });
            }

            if package_id == ONTOL_PKG {
                for def_id in self.defs.text_literals.values().copied() {
                    let literal = serde_gen.defs.get_string_representation(def_id);
                    let constant = serde_gen.str_ctx.intern_constant(literal);

                    domain.add_def(Def {
                        id: def_id,
                        public: false,
                        kind: domain::DefKind::Data(BasicDef {
                            ident: None,
                            repr: DefRepr::TextConstant(constant),
                        }),
                        operator_addr: None,
                        store_key: None,
                        data_relationships: Default::default(),
                    });
                }
            }

            builder.add_domain(package_id, domain);
        }

        // interface handling
        let domain_interfaces = {
            let mut interfaces: FnvHashMap<PackageId, Vec<DomainInterface>> = Default::default();
            for package_id in package_ids.iter().cloned() {
                let domain_def_id = self.package_def_ids.get(&package_id).cloned().unwrap();
                if package_id == ONTOL_PKG {
                    continue;
                }

                if let Some(httpjson) = generate_httpjson_interface(
                    package_id,
                    builder.partial_ontology(),
                    &mut serde_gen,
                ) {
                    interfaces
                        .entry(package_id)
                        .or_default()
                        .push(DomainInterface::HttpJson(httpjson));
                }

                if let Some(schema) = generate_graphql_schema(
                    package_id,
                    builder.partial_ontology(),
                    map_namespaces.get(&domain_def_id),
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

        let mut def_docs = FnvHashMap::default();
        // let mut rel_docs = FnvHashMap::default();
        let mut prop_docs = FnvHashMap::default();

        for (doc_id, docs) in &docs_table {
            match doc_id {
                DocId::Def(def_id) => {
                    def_docs.insert(*def_id, str_ctx.intern_constant(docs));
                }
                DocId::Rel(_rel_id) => {}
            }
        }

        let mut value_generators: FnvHashMap<PropId, ValueGenerator> = Default::default();
        for properties in self.prop_ctx.properties_by_def_id.values() {
            if let Some(table) = &properties.table {
                for (prop_id, property) in table {
                    if let Some(value_generator) =
                        self.misc_ctx.value_generators.remove(&property.rel_id)
                    {
                        value_generators.insert(*prop_id, value_generator);
                    }

                    if let Some(docs) = docs_table.get(&DocId::Rel(property.rel_id)) {
                        prop_docs.insert(*prop_id, str_ctx.intern_constant(docs));
                    }
                }
            }
        }

        builder
            .text_constants(str_ctx.into_arcstr_vec())
            .ontol_domain_meta(OntolDomainMeta {
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
            .def_docs(def_docs)
            .prop_docs(prop_docs)
            .const_procs(self.code_ctx.result_const_procs)
            .map_meta_table(map_meta_table)
            .static_conditions(self.code_ctx.result_static_conditions)
            .named_forward_maps(self.code_ctx.result_named_downmaps)
            .serde_operators(serde_operators)
            .dynamic_sequence_operator_addr(dynamic_sequence_operator_addr)
            .property_flows(property_flows)
            .text_like_types(self.defs.text_like_types)
            .text_patterns(self.text_patterns.text_patterns)
            .externs(self.def_ty_ctx.ontology_externs)
            .value_generators(value_generators)
            .domain_interfaces(domain_interfaces)
            .build()
    }

    fn collect_relationships_and_edges(
        &self,
        type_def_id: DefId,
        union_member_cache: &UnionMemberCache,
        str_ctx: &mut StringCtx<'m>,
    ) -> FnvIndexMap<PropId, DataRelationshipInfo> {
        let mut relationships = FnvIndexMap::default();
        self.collect_inherent_relationships_and_edges(type_def_id, &mut relationships, str_ctx);

        if let Some(ReprKind::StructIntersection(members)) =
            self.repr_ctx.get_repr_kind(&type_def_id)
        {
            for (member_def_id, _) in members {
                self.collect_inherent_relationships_and_edges(
                    *member_def_id,
                    &mut relationships,
                    str_ctx,
                );
            }
        }

        if let Some(union_memberships) = union_member_cache.cache.get(&type_def_id) {
            for union_def_id in union_memberships {
                let Some(properties) = self.prop_ctx.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for (prop_id, property) in table.iter() {
                    self.collect_prop_relationship_and_edge(
                        *prop_id,
                        property.rel_id,
                        DataRelationshipSource::ByUnionProxy,
                        &mut relationships,
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
        relationships: &mut FnvIndexMap<PropId, DataRelationshipInfo>,
        str_ctx: &mut StringCtx<'m>,
    ) {
        let Some(properties) = self.prop_ctx.properties_by_def_id(type_def_id) else {
            return;
        };
        if let Some(table) = &properties.table {
            for (prop_id, property) in table.iter() {
                self.collect_prop_relationship_and_edge(
                    *prop_id,
                    property.rel_id,
                    DataRelationshipSource::Inherent,
                    relationships,
                    str_ctx,
                );
            }
        }
    }

    fn collect_prop_relationship_and_edge(
        &self,
        prop_id: PropId,
        rel_id: RelId,
        source: DataRelationshipSource,
        relationships: &mut FnvIndexMap<PropId, DataRelationshipInfo>,
        str_ctx: &mut StringCtx<'m>,
    ) {
        let meta = rel_repr_meta(rel_id, &self.rel_ctx, &self.defs, &self.repr_ctx);

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

        let (data_relationship_kind, target) =
            match (meta.relationship.edge_projection, target_repr_kind) {
                (Some(edge_projection), ReprKind::Union(members, _bound)) => {
                    let kinds_and_targets = members
                        .iter()
                        .map(|(member_def_id, _)| {
                            self.data_relationship_kind_and_target(
                                rel_id,
                                &meta.relationship,
                                Some(edge_projection),
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
                (None, ReprKind::Union(..)) => {
                    let (kind, _) = self.data_relationship_kind_and_target(
                        rel_id,
                        &meta.relationship,
                        meta.relationship.edge_projection,
                        source_def_id,
                        target_def_id,
                    );

                    (kind, DataRelationshipTarget::Union(target_def_id))
                }
                _ => self.data_relationship_kind_and_target(
                    rel_id,
                    &meta.relationship,
                    meta.relationship.edge_projection,
                    source_def_id,
                    target_def_id,
                ),
            };

        // collect relationship
        relationships.insert(
            prop_id,
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
        rel_id: RelId,
        relationship: &Relationship,
        edge_projection: Option<EdgeCardinalProjection>,
        source_def_id: DefId,
        target_def_id: DefId,
    ) -> (DataRelationshipKind, DataRelationshipTarget) {
        let target_properties = self.prop_ctx.properties_by_def_id(target_def_id);

        if let Some(identifies) = target_properties.and_then(|p| p.identifies) {
            let meta = rel_def_meta(identifies, &self.rel_ctx, &self.defs);
            if relationship.can_identify() && meta.relationship.object.0 == source_def_id {
                (
                    DataRelationshipKind::Id,
                    DataRelationshipTarget::Unambiguous(target_def_id),
                )
            } else if self
                .prop_ctx
                .properties_by_def_id(source_def_id)
                // It can be an edge (for now) only the the source (subject) is an entity.
                // i.e. there is no support for "indirect" edge properties
                .map(|props| props.identified_by.is_some())
                .unwrap_or(false)
            {
                (
                    edge_projection
                        .map(DataRelationshipKind::Edge)
                        .unwrap_or(DataRelationshipKind::Tree),
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
                edge_projection
                    .map(DataRelationshipKind::Edge)
                    .unwrap_or(DataRelationshipKind::Tree),
                DataRelationshipTarget::Unambiguous(target_def_id),
            )
        } else {
            let source_properties = self.prop_ctx.properties_by_def_id(source_def_id);
            let is_entity_id = relationship.can_identify()
                && source_properties
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

    fn namespaced_to_ontology_def_kind(
        &self,
        def_id: DefId,
        def_kind: &DefKind,
        ident: TextConstant,
        serde_gen: &mut SerdeGenerator,
        data_relationships: &FnvIndexMap<PropId, DataRelationshipInfo>,
    ) -> domain::DefKind {
        match def_kind {
            DefKind::Arc(_) => {
                let edge = self.edge_ctx.symbolic_edges.get(&EdgeId(def_id)).unwrap();

                let mut edge_info = EdgeInfo {
                    ident,
                    cardinals: Vec::with_capacity(edge.cardinals.len()),
                    store_key: None,
                };

                for cardinal in edge.cardinals.values() {
                    let output_cardinal = match &cardinal.kind {
                        CardinalKind::Vertex { members } => {
                            let entity_count = members
                                .keys()
                                .filter(|def_id| self.entity_ctx.entities.contains_key(def_id))
                                .count();

                            let mut flags = EdgeCardinalFlags::empty();

                            if entity_count == members.len() {
                                flags.insert(EdgeCardinalFlags::ENTITY);

                                if cardinal.unique_count > 0 {
                                    flags.insert(EdgeCardinalFlags::UNIQUE);
                                }
                                if cardinal.pinned_count > 0 {
                                    flags.insert(EdgeCardinalFlags::PINNED_DEF);
                                }
                            } else if entity_count > 0 {
                                panic!("FIXME: mix of entity/non-entity");
                            };

                            EdgeCardinal {
                                target: members.keys().copied().collect(),
                                flags,
                            }
                        }
                        CardinalKind::Parameter { def_id } => EdgeCardinal {
                            target: DefIdSet::from_iter([*def_id]),
                            flags: EdgeCardinalFlags::empty(),
                        },
                    };

                    edge_info.cardinals.push(output_cardinal);
                }

                domain::DefKind::Edge(edge_info)
            }
            _ => {
                let Some(properties) = self.prop_ctx.properties_by_def_id(def_id) else {
                    return self
                        .fallback_namespaced_ontology_def_kind(def_id, def_kind, ident, serde_gen);
                };
                let Some(id_relationship_id) = properties.identified_by else {
                    return self
                        .fallback_namespaced_ontology_def_kind(def_id, def_kind, ident, serde_gen);
                };

                let identifies_meta = rel_def_meta(id_relationship_id, &self.rel_ctx, &self.defs);

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

                let inherent_primary_id_meta = self.find_inherent_primary_id(def_id, properties);

                let id_value_generator =
                    if let Some((_, inherent_primary_id_meta)) = &inherent_primary_id_meta {
                        self.misc_ctx
                            .value_generators
                            .get(&inherent_primary_id_meta.rel_id)
                            .cloned()
                    } else {
                        None
                    };

                domain::DefKind::Entity(Entity {
                    ident,
                    // The entity is self-identifying if it has an inherent primary_id and that is its only inherent property.
                    // TODO: Is the entity still self-identifying if it has only an external primary id (i.e. it is a unit type)?
                    is_self_identifying: inherent_primary_id_meta.is_some()
                        && inherent_property_count <= 1,
                    id_prop: match inherent_primary_id_meta {
                        Some((prop_id, _)) => prop_id,
                        None => todo!(),
                    },
                    id_value_def_id: identifies_meta.relationship.subject.0,
                    id_value_generator,
                    id_operator_addr: serde_gen
                        .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                            identifies_meta.relationship.subject.0,
                            SerdeModifier::NONE,
                        )))
                        .unwrap(),
                })
            }
        }
    }

    fn fallback_namespaced_ontology_def_kind(
        &self,
        def_id: DefId,
        def_kind: &DefKind,
        ident: TextConstant,
        serde_gen: &mut SerdeGenerator,
    ) -> domain::DefKind {
        def_kind.as_ontology_type_kind(BasicDef {
            ident: Some(ident),
            repr: self
                .repr_ctx
                .get_repr_kind(&def_id)
                .map(|repr_kind| make_def_repr(repr_kind, serde_gen))
                .unwrap_or(DefRepr::Unknown),
        })
    }

    fn find_inherent_primary_id(
        &self,
        _entity_id: DefId,
        properties: &Properties,
    ) -> Option<(PropId, RelDefMeta<'_, 'm>)> {
        let id_relationship_id = properties.identified_by?;
        let inherent_prop_id = self
            .misc_ctx
            .inherent_id_map
            .get(&id_relationship_id)
            .cloned()?;
        let map = properties.table.as_ref()?;
        let property = map.get(&inherent_prop_id)?;

        Some((
            inherent_prop_id,
            rel_def_meta(property.rel_id, &self.rel_ctx, &self.defs),
        ))
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

fn make_def_repr(kind: &ReprKind, serde_gen: &mut SerdeGenerator) -> DefRepr {
    match kind {
        ReprKind::Unit => DefRepr::Unit,
        ReprKind::Scalar(_, kind, _) => make_scalar_def_repr(kind, serde_gen),
        ReprKind::FmtStruct(opt_attr) => DefRepr::FmtStruct(*opt_attr),
        ReprKind::Seq => DefRepr::Seq,
        ReprKind::Struct => DefRepr::Struct,
        ReprKind::StructIntersection(_) => DefRepr::Struct,
        ReprKind::Intersection(defs) => {
            DefRepr::Intersection(defs.iter().map(|(def_id, _)| *def_id).collect())
        }
        ReprKind::Union(defs, bound) => DefRepr::Union(
            defs.iter().map(|(def_id, _)| *def_id).collect(),
            match bound {
                UnionBound::Any => DefReprUnionBound::Any,
                UnionBound::Scalar(scalar_kind) => DefReprUnionBound::Scalar(Box::new(
                    make_scalar_def_repr(scalar_kind, serde_gen),
                )),
                UnionBound::Struct => DefReprUnionBound::Struct,
                UnionBound::Fmt => DefReprUnionBound::Fmt,
            },
        ),
        ReprKind::Macro => DefRepr::Macro,
        ReprKind::Extern => DefRepr::Unknown,
    }
}

fn make_scalar_def_repr(kind: &ReprScalarKind, serde_gen: &mut SerdeGenerator) -> DefRepr {
    match kind {
        &ReprScalarKind::I64(_) => DefRepr::I64,
        ReprScalarKind::F64(_) => DefRepr::F64,
        ReprScalarKind::Serial => DefRepr::Serial,
        ReprScalarKind::Boolean => DefRepr::Boolean,
        ReprScalarKind::Text => DefRepr::Text,
        ReprScalarKind::TextConstant(def_id) => {
            let literal = serde_gen.defs.get_string_representation(*def_id);
            let constant = serde_gen.str_ctx.intern_constant(literal);
            DefRepr::TextConstant(constant)
        }
        ReprScalarKind::Octets(_) => DefRepr::Octets,
        ReprScalarKind::DateTime => DefRepr::DateTime,
        ReprScalarKind::Other => DefRepr::Unknown,
    }
}
