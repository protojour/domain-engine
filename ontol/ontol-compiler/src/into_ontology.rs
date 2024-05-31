use fnv::FnvHashMap;
use indoc::indoc;
use ontol_runtime::{
    interface::{
        serde::{SerdeDef, SerdeModifier},
        DomainInterface,
    },
    ontology::{
        domain::{
            BasicTypeInfo, DataRelationshipInfo, DataRelationshipKind, DataRelationshipSource,
            DataRelationshipTarget, Domain, EntityInfo, TypeInfo, TypeKind,
        },
        map::MapMeta,
        ontol::{OntolDomainMeta, TextConstant, TextLikeType},
        Ontology,
    },
    property::PropertyId,
    rustdoc::RustDoc,
    DefId, PackageId,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    def::{BuiltinRelationKind, DefKind, LookupRelationshipMeta, RelationshipMeta, TypeDef},
    def::{RelParams, TypeDefFlags},
    interface::{
        graphql::generate_schema::generate_graphql_schema,
        serde::{serde_generator::SerdeGenerator, SerdeKey, EDGE_PROPERTY},
    },
    namespace::Space,
    package::ONTOL_PKG,
    primitive::PrimitiveKind,
    relation::{Properties, UnionMemberCache},
    repr::repr_model::ReprKind,
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

        let mut strings = self.str_ctx.detach();
        let mut serde_gen = self.serde_generator(&mut strings, &union_member_cache);
        let mut builder = Ontology::builder();
        let mut ontology_union_variants: FnvHashMap<DefId, Box<[DefId]>> = Default::default();

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

            for (type_name, type_def_id) in type_namespace {
                let type_name_constant = serde_gen.str_ctx.intern_constant(type_name);
                let data_relationships = self.find_data_relationships(
                    type_def_id,
                    &union_member_cache,
                    serde_gen.str_ctx,
                );
                let def_kind = self.defs.def_kind(type_def_id);

                if let Some(ReprKind::StructUnion(members)) =
                    self.repr_ctx.get_repr_kind(&type_def_id)
                {
                    ontology_union_variants.insert(
                        type_def_id,
                        members.iter().map(|(member, _)| *member).collect(),
                    );
                }

                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: match def_kind {
                        DefKind::Type(TypeDef { flags, .. }) => {
                            flags.contains(TypeDefFlags::PUBLIC)
                        }
                        _ => true,
                    },
                    kind: match self.entity_info(
                        type_def_id,
                        type_name_constant,
                        &mut serde_gen,
                        &data_relationships,
                    ) {
                        Some(entity_info) => TypeKind::Entity(entity_info),
                        None => def_kind.as_ontology_type_kind(BasicTypeInfo {
                            name: Some(type_name_constant),
                        }),
                    },
                    operator_addr: serde_gen.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    store_key: Some(
                        self.rel_ctx
                            .store_keys
                            .get(&type_def_id)
                            .copied()
                            .unwrap_or(type_name_constant),
                    ),
                    data_relationships,
                });
            }

            for type_def_id in namespace.anonymous {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: false,
                    kind: self
                        .defs
                        .def_kind(type_def_id)
                        .as_ontology_type_kind(BasicTypeInfo { name: None }),
                    operator_addr: serde_gen.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    store_key: self.rel_ctx.store_keys.get(&type_def_id).copied(),
                    data_relationships: self.find_data_relationships(
                        type_def_id,
                        &union_member_cache,
                        serde_gen.str_ctx,
                    ),
                });
            }

            if package_id == ONTOL_PKG {
                for def_id in self.defs.text_literals.values().copied() {
                    domain.add_type(TypeInfo {
                        def_id,
                        public: false,
                        kind: TypeKind::Data(BasicTypeInfo { name: None }),
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
            .map(|(def_id, docs)| (def_id, strings.intern_constant(&docs)))
            .collect();

        builder
            .text_constants(strings.into_arcstr_vec())
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
            .union_variants(ontology_union_variants)
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

    fn find_data_relationships(
        &self,
        type_def_id: DefId,
        union_member_cache: &UnionMemberCache,
        strings: &mut StringCtx<'m>,
    ) -> FnvHashMap<PropertyId, DataRelationshipInfo> {
        let mut data_relationships = FnvHashMap::default();
        self.add_inherent_data_relationships(type_def_id, &mut data_relationships, strings);

        if let Some(ReprKind::StructIntersection(members)) =
            self.repr_ctx.get_repr_kind(&type_def_id)
        {
            for (member_def_id, _) in members {
                self.add_inherent_data_relationships(
                    *member_def_id,
                    &mut data_relationships,
                    strings,
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
                    if let Some(data_relationship) = self.generate_data_relationship_info(
                        *property_id,
                        DataRelationshipSource::ByUnionProxy,
                        strings,
                    ) {
                        data_relationships.insert(*property_id, data_relationship);
                    }
                }
            }
        }

        data_relationships
    }

    fn add_inherent_data_relationships(
        &self,
        type_def_id: DefId,
        output: &mut FnvHashMap<PropertyId, DataRelationshipInfo>,
        strings: &mut StringCtx<'m>,
    ) {
        let Some(properties) = self.rel_ctx.properties_by_def_id(type_def_id) else {
            return;
        };
        if let Some(table) = &properties.table {
            for property_id in table.keys() {
                if let Some(data_relationship) = self.generate_data_relationship_info(
                    *property_id,
                    DataRelationshipSource::Inherent,
                    strings,
                ) {
                    output.insert(*property_id, data_relationship);
                }
            }
        }
    }

    fn generate_data_relationship_info(
        &self,
        property_id: PropertyId,
        source: DataRelationshipSource,
        strings: &mut StringCtx<'m>,
    ) -> Option<DataRelationshipInfo> {
        let meta = self.defs.relationship_meta(property_id.relationship_id);

        let (source_def_id, _, _) = meta.relationship.by(property_id.role);
        let (target_def_id, _, _) = meta.relationship.by(property_id.role.opposite());

        let subject_name = match meta.relation_def_kind.value {
            DefKind::TextLiteral(subject_name) => strings.intern_constant(subject_name),
            // FIXME: This doesn't _really_ have a subject "name". It represents a flattened structure:
            DefKind::Type(_) => strings.intern_constant(""),
            _ => return None,
        };
        let target_properties = self.rel_ctx.properties_by_def_id(target_def_id)?;
        let repr_kind = self.repr_ctx.get_repr_kind(&target_def_id)?;

        let graph_rel_params = match meta.relationship.rel_params {
            RelParams::Type(def_id) => Some(def_id),
            RelParams::Unit | RelParams::IndexRange(_) => None,
        };

        let (data_relationship_kind, target) = match repr_kind {
            ReprKind::StructUnion(members) => {
                let target = DataRelationshipTarget::Union(target_def_id);

                if members.iter().all(|(member_def_id, _)| {
                    self.rel_ctx
                        .properties_by_def_id(*member_def_id)
                        .map(|properties| properties.identified_by.is_some())
                        .unwrap_or(false)
                }) {
                    (
                        DataRelationshipKind::EntityGraph {
                            rel_params: graph_rel_params,
                        },
                        target,
                    )
                } else {
                    (DataRelationshipKind::Tree, target)
                }
            }
            _ => {
                let target = DataRelationshipTarget::Unambiguous(target_def_id);
                if target_properties.identified_by.is_some() {
                    (
                        DataRelationshipKind::EntityGraph {
                            rel_params: graph_rel_params,
                        },
                        target,
                    )
                } else {
                    let source_properties = self.rel_ctx.properties_by_def_id(source_def_id);
                    let is_entity_id = source_properties
                        .map(|properties| {
                            properties.identified_by == Some(property_id.relationship_id)
                        })
                        .unwrap_or(false);

                    (
                        if is_entity_id {
                            DataRelationshipKind::Id
                        } else {
                            DataRelationshipKind::Tree
                        },
                        target,
                    )
                }
            }
        };

        Some(DataRelationshipInfo {
            kind: data_relationship_kind,
            subject_cardinality: meta.relationship.subject_cardinality,
            object_cardinality: meta.relationship.object_cardinality,
            subject_name,
            object_name: meta
                .relationship
                .object_prop
                .map(|prop| strings.intern_constant(prop)),
            store_key: self
                .rel_ctx
                .store_keys
                .get(&property_id.relationship_id.0)
                .copied(),
            source,
            target,
        })
    }

    fn entity_info(
        &self,
        type_def_id: DefId,
        name: TextConstant,
        serde_generator: &mut SerdeGenerator,
        data_relationships: &FnvHashMap<PropertyId, DataRelationshipInfo>,
    ) -> Option<EntityInfo> {
        let properties = self.rel_ctx.properties_by_def_id(type_def_id)?;
        let id_relationship_id = properties.identified_by?;

        let identifies_meta = self.defs.relationship_meta(id_relationship_id);

        // inherent properties are the properties that are _not_ entity relationships:
        let mut inherent_property_count = 0;

        for data_relationship in data_relationships.values() {
            if matches!(data_relationship.kind, DataRelationshipKind::Tree) {
                inherent_property_count += 1;
            }
        }

        let inherent_primary_id_meta = self.find_inherent_primary_id(type_def_id, properties);

        let id_value_generator = if let Some(inherent_primary_id_meta) = &inherent_primary_id_meta {
            self.rel_ctx
                .value_generators
                .get(&inherent_primary_id_meta.relationship_id)
                .cloned()
        } else {
            None
        };

        Some(EntityInfo {
            name,
            // The entity is self-identifying if it has an inherent primary_id and that is its only inherent property.
            // TODO: Is the entity still self-identifying if it has only an external primary id (i.e. it is a unit type)?
            is_self_identifying: inherent_primary_id_meta.is_some() && inherent_property_count <= 1,
            id_relationship_id: match inherent_primary_id_meta {
                Some(inherent_meta) => inherent_meta.relationship_id,
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

    fn find_inherent_primary_id(
        &self,
        _entity_id: DefId,
        properties: &Properties,
    ) -> Option<RelationshipMeta<'_, 'm>> {
        let id_relationship_id = properties.identified_by?;
        let inherent_id = self
            .rel_ctx
            .inherent_id_map
            .get(&id_relationship_id)
            .cloned()?;
        let map = properties.table.as_ref()?;
        let _property = map.get(&PropertyId::subject(inherent_id))?;

        Some(self.defs.relationship_meta(inherent_id))
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
