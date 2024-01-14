#![forbid(unsafe_code)]

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use codegen::task::{execute_codegen_tasks, CodegenTasks};
use def::{DefKind, DefVisibility, Defs, LookupRelationshipMeta, RelationshipMeta, TypeDef};

pub use error::*;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use interface::{
    graphql::generate_schema::generate_graphql_schema, serde::serde_generator::SerdeGenerator,
};
use lowering::Lowering;
use mem::Mem;
use namespace::{Namespaces, Space};
use ontol_runtime::{
    config::PackageConfig,
    interface::{
        serde::{SerdeDef, SerdeKey, SerdeModifier},
        DomainInterface,
    },
    ontology::{
        DataRelationshipInfo, DataRelationshipKind, DataRelationshipSource, DataRelationshipTarget,
        Domain, EntityInfo, MapLossiness, MapMeta, OntolDomainMeta, Ontology, TypeInfo,
    },
    value::PropertyId,
    DefId, PackageId,
};
use ontology_graph::OntologyGraph;
use package::{PackageTopology, Packages, ParsedPackage, ONTOL_PKG};
use pattern::Patterns;
use primitive::Primitives;
use relation::{Properties, Relations, UnionMemberCache};
pub use source::*;
use strings::Strings;
use text_patterns::{compile_all_text_patterns, TextPatterns};
use tracing::debug;
use type_check::seal::SealCtx;
use types::{DefTypes, Types};

use crate::{def::RelParams, type_check::repr::repr_model::ReprKind};

pub mod error;
pub mod hir_unify;
pub mod mem;
pub mod ontology_graph;
pub mod package;
pub mod source;

mod codegen;
mod compiler_queries;
mod def;
mod interface;
mod lowering;
mod map;
mod map_arm_def_inference;
mod namespace;
mod ontol_domain;
mod pattern;
mod primitive;
mod regex_util;
mod relation;
mod sequence;
mod strings;
mod text_patterns;
mod type_check;
mod typed_hir;
mod types;

/// Temporary configuration: Use the new flat unifier
const USE_FLAT_UNIFIER: bool = true;

/// Temporary configuration: Fall back to the classic unifier if the flat unifier fails
const CLASSIC_UNIFIER_FALLBACK: bool = true;

/// Temporary configuration: Handle seq/DeclSeq in the flat unifier
const USE_FLAT_SEQ_HANDLING: bool = true;

#[derive(Debug)]
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: Packages,
    pub(crate) package_names: Vec<(PackageId, Arc<String>)>,

    pub(crate) namespaces: Namespaces<'m>,
    pub(crate) defs: Defs<'m>,
    pub(crate) package_config_table: FnvHashMap<PackageId, PackageConfig>,
    pub(crate) primitives: Primitives,
    pub(crate) patterns: Patterns,

    pub(crate) strings: Strings<'m>,
    pub(crate) types: Types<'m>,
    pub(crate) def_types: DefTypes<'m>,
    pub(crate) relations: Relations,
    pub(crate) seal_ctx: SealCtx,
    pub(crate) text_patterns: TextPatterns,

    pub(crate) codegen_tasks: CodegenTasks<'m>,

    pub(crate) errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    pub fn new(mem: &'m Mem, sources: Sources) -> Self {
        let mut defs = Defs::default();
        let primitives = Primitives::new(&mut defs);

        Self {
            sources,
            packages: Default::default(),
            package_names: Default::default(),
            namespaces: Default::default(),
            defs,
            package_config_table: Default::default(),
            primitives,
            patterns: Default::default(),
            strings: Strings::new(mem),
            types: Types::new(mem),
            def_types: Default::default(),
            relations: Relations::default(),
            seal_ctx: Default::default(),
            text_patterns: TextPatterns::default(),
            codegen_tasks: Default::default(),
            errors: Default::default(),
        }
    }

    /// Entry point of all compilation: Compiles the full package topology
    pub fn compile_package_topology(
        &mut self,
        topology: PackageTopology,
    ) -> Result<(), UnifiedCompileError> {
        // There could be errors in the ontol domain, this is useful for development:
        self.check_error()?;

        for parsed_package in topology.packages {
            debug!(
                "lower {:?}: {:?}",
                parsed_package.package_id, parsed_package.reference
            );
            let source_id = self
                .sources
                .source_id_for_package(parsed_package.package_id)
                .expect("no source id available for package");
            let src = self
                .sources
                .get_source(source_id)
                .expect("no compiled source available");

            self.lower_and_check_next_domain(parsed_package, src)?;
        }

        execute_codegen_tasks(self);
        compile_all_text_patterns(self);
        self.check_error()
    }

    /// Lower statements from the next domain,
    /// perform type check against its dependent domains,
    /// and seal the types at the end.
    fn lower_and_check_next_domain(
        &mut self,
        package: ParsedPackage,
        src: Src,
    ) -> Result<(), UnifiedCompileError> {
        for error in package.parser_errors {
            self.push_error(match error {
                ontol_parser::Error::Lex(lex_error) => {
                    let span = lex_error.span();
                    CompileError::Lex(LexError::new(lex_error)).spanned(&src.span(&span))
                }
                ontol_parser::Error::Parse(parse_error) => {
                    let span = parse_error.span();
                    CompileError::Parse(ParseError::new(parse_error)).spanned(&src.span(&span))
                }
            });
        }

        let package_def_id = self.defs.alloc_def_id(package.package_id);
        self.define_package(package_def_id);

        self.packages
            .loaded_packages
            .insert(package.reference, package_def_id);

        self.package_config_table
            .insert(package.package_id, package.config);

        let lowered = Lowering::new(self, &src)
            .lower_statements(package.statements)
            .finish();

        for def_id in lowered.root_defs {
            if lowered.map_defs.contains(&def_id) {
                if let Some(inference_info) = self.check_map_arm_def_inference(def_id) {
                    self.type_check().check_def_sealed(inference_info.source.1);
                    let new_defs = self
                        .map_arm_def_inferencer(def_id)
                        .infer_map_arm_type(inference_info);

                    for def_id in new_defs {
                        self.type_check().check_def_shallow(def_id);
                    }

                    self.type_check().check_def_sealed(inference_info.target.1);
                }
            }

            self.type_check().check_def_shallow(def_id);
        }

        self.seal_domain(package.package_id);

        self.package_names
            .push((package.package_id, src.name.clone()));

        self.check_error()
    }

    /// Get the current ontology graph (which is serde-serializable)
    pub fn ontology_graph(&self) -> OntologyGraph<'_, 'm> {
        OntologyGraph::from(self)
    }

    /// Finish compilation, turn into runtime ontology.
    pub fn into_ontology(mut self) -> Ontology {
        let package_ids = self.package_ids();
        let unique_domain_names = self.unique_domain_names();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut package_config_table = std::mem::take(&mut self.package_config_table);
        let docs = std::mem::take(&mut self.namespaces.docs);

        let union_member_cache = {
            let mut cache: FnvHashMap<DefId, BTreeSet<DefId>> = Default::default();

            for package_id in package_ids.iter() {
                let namespace = namespaces.get(package_id).unwrap();

                for (_, union_def_id) in &namespace.types {
                    let Some(ReprKind::StructUnion(variants)) =
                        self.seal_ctx.get_repr_kind(union_def_id)
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

        let mut serde_generator = self.serde_generator(&union_member_cache);
        let mut builder = Ontology::builder();

        let dynamic_sequence_operator_addr = serde_generator.make_dynamic_sequence_addr();

        let map_namespaces: FnvHashMap<_, _> = namespaces
            .iter_mut()
            .map(|(package_id, namespace)| {
                (*package_id, std::mem::take(namespace.space_mut(Space::Map)))
            })
            .collect();

        // For now, create serde operators for every domain
        for package_id in package_ids.iter().cloned() {
            let domain_name = unique_domain_names
                .get(&package_id)
                .expect("Anonymous domain");
            let mut domain = Domain::new(domain_name.into());

            let namespace = namespaces.remove(&package_id).unwrap();
            let type_namespace = namespace.types;

            if let Some(package_config) = package_config_table.remove(&package_id) {
                builder.add_package_config(package_id, package_config);
            }

            for (type_name, type_def_id) in type_namespace {
                let data_relationships =
                    self.find_data_relationships(type_def_id, &union_member_cache);

                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: match self.defs.def_kind(type_def_id) {
                        DefKind::Type(TypeDef { visibility, .. }) => {
                            matches!(visibility, DefVisibility::Public)
                        }
                        _ => true,
                    },
                    name: Some(type_name.into()),
                    entity_info: self.entity_info(
                        type_def_id,
                        &mut serde_generator,
                        &data_relationships,
                    ),
                    operator_addr: serde_generator.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    data_relationships,
                });
            }

            for type_def_id in namespace.anonymous {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: false,
                    name: None,
                    entity_info: None,
                    operator_addr: serde_generator.gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                    data_relationships: self
                        .find_data_relationships(type_def_id, &union_member_cache),
                });
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
                    &self.codegen_tasks,
                    &union_member_cache,
                    &mut serde_generator,
                ) {
                    interfaces
                        .entry(package_id)
                        .or_default()
                        .push(DomainInterface::GraphQL(Arc::new(schema)));
                }
            }
            interfaces
        };

        let (serde_operators, _) = serde_generator.finish();

        let mut property_flows = vec![];

        let map_meta_table = self
            .codegen_tasks
            .result_map_proc_table
            .into_iter()
            .map(|(key, procedure)| {
                let propflow_range = if let Some(current_prop_flows) =
                    self.codegen_tasks.result_propflow_table.remove(&key)
                {
                    let start: u32 = property_flows.len().try_into().unwrap();
                    let len: u32 = current_prop_flows.len().try_into().unwrap();
                    property_flows.extend(current_prop_flows);
                    start..(start + len)
                } else {
                    0..0
                };

                let metadata = self.codegen_tasks.result_metadata_table.remove(&key);

                (
                    key,
                    MapMeta {
                        procedure,
                        propflow_range,
                        lossiness: metadata
                            .map(|metadata| metadata.lossiness)
                            .unwrap_or(MapLossiness::Lossy),
                    },
                )
            })
            .collect();

        builder
            .ontol_domain_meta(OntolDomainMeta {
                bool: self.primitives.bool,
                i64: self.primitives.i64,
                f64: self.primitives.f64,
                text: self.primitives.text,
                open_data_relationship: self.primitives.open_data_relationship,
            })
            .lib(self.codegen_tasks.result_lib)
            .docs(docs)
            .const_procs(self.codegen_tasks.result_const_procs)
            .map_meta_table(map_meta_table)
            .named_forward_maps(self.codegen_tasks.result_named_forward_maps)
            .serde_operators(serde_operators)
            .dynamic_sequence_operator_addr(dynamic_sequence_operator_addr)
            .property_flows(property_flows)
            .string_like_types(self.defs.string_like_types)
            .text_patterns(self.text_patterns.text_patterns)
            .value_generators(self.relations.value_generators)
            .domain_interfaces(domain_interfaces)
            .build()
    }

    fn find_data_relationships(
        &self,
        type_def_id: DefId,
        union_member_cache: &UnionMemberCache,
    ) -> IndexMap<PropertyId, DataRelationshipInfo> {
        let Some(properties) = self.relations.properties_by_def_id(type_def_id) else {
            return Default::default();
        };
        let mut data_relationships = IndexMap::default();

        if let Some(table) = &properties.table {
            for property_id in table.keys() {
                if let Some(data_relationship) = self
                    .generate_data_relationship_info(*property_id, DataRelationshipSource::Inherent)
                {
                    data_relationships.insert(*property_id, data_relationship);
                }
            }
        }

        if let Some(union_memberships) = union_member_cache.cache.get(&type_def_id) {
            for union_def_id in union_memberships {
                let Some(properties) = self.relations.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for property_id in table.keys() {
                    if let Some(data_relationship) = self.generate_data_relationship_info(
                        *property_id,
                        DataRelationshipSource::ByUnionProxy,
                    ) {
                        data_relationships.insert(*property_id, data_relationship);
                    }
                }
            }
        }

        data_relationships
    }

    fn generate_data_relationship_info(
        &self,
        property_id: PropertyId,
        source: DataRelationshipSource,
    ) -> Option<DataRelationshipInfo> {
        let meta = self.defs.relationship_meta(property_id.relationship_id);

        let (target_def_id, _, _) = meta.relationship.by(property_id.role.opposite());

        let DefKind::TextLiteral(subject_name) = meta.relation_def_kind.value else {
            return None;
        };

        if let Some(target_properties) = self.relations.properties_by_def_id(target_def_id) {
            let graph_rel_params = match meta.relationship.rel_params {
                RelParams::Type(def_id) => Some(def_id),
                RelParams::Unit | RelParams::IndexRange(_) => None,
            };

            if let Some(repr_kind) = self.seal_ctx.get_repr_kind(&target_def_id) {
                let (data_relationship_kind, target) = match repr_kind {
                    ReprKind::StructUnion(members) => {
                        let target = DataRelationshipTarget::Union {
                            union_def_id: target_def_id,
                            variants: members.iter().map(|(def_id, _)| *def_id).collect(),
                        };

                        if members.iter().all(|(member_def_id, _)| {
                            self.relations
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
                            (DataRelationshipKind::Tree, target)
                        }
                    }
                };

                Some(DataRelationshipInfo {
                    kind: data_relationship_kind,
                    subject_cardinality: meta.relationship.subject_cardinality,
                    object_cardinality: meta.relationship.object_cardinality,
                    subject_name: (*subject_name).into(),
                    object_name: meta.relationship.object_prop.map(|prop| prop.into()),
                    source,
                    target,
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn entity_info(
        &self,
        type_def_id: DefId,
        serde_generator: &mut SerdeGenerator,
        data_relationships: &IndexMap<PropertyId, DataRelationshipInfo>,
    ) -> Option<EntityInfo> {
        let properties = self.relations.properties_by_def_id(type_def_id)?;
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
            self.relations
                .value_generators
                .get(&inherent_primary_id_meta.relationship_id)
                .cloned()
        } else {
            None
        };

        Some(EntityInfo {
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
            .relations
            .inherent_id_map
            .get(&id_relationship_id)
            .cloned()?;
        let map = properties.table.as_ref()?;
        let _property = map.get(&PropertyId::subject(inherent_id))?;

        Some(self.defs.relationship_meta(inherent_id))
    }

    fn package_ids(&self) -> Vec<PackageId> {
        self.namespaces.namespaces.keys().copied().collect()
    }

    /// Seal all the types in a single domain.
    fn seal_domain(&mut self, package_id: PackageId) {
        debug!("seal {package_id:?}");

        let iterator = self.defs.iter_package_def_ids(package_id);
        let mut type_check = self.type_check();

        for def_id in iterator {
            type_check.seal_def(def_id);
        }

        self.seal_ctx.mark_domain_sealed(package_id);

        // Various cleanup/normalization
        for def_id in self.defs.iter_package_def_ids(package_id) {
            let Some(def) = self.defs.table.get_mut(&def_id) else {
                // Can happen in error cases
                continue;
            };

            if let DefKind::Relationship(relationship) = &mut def.kind {
                // Reset RelParams::Type back to RelParams::Unit if its representation is ReprKind::Unit.
                // This simplifies later compiler stages, that can trust RelParams::Type is a type with real data in it.
                if let RelParams::Type(rel_def_id) = &relationship.rel_params {
                    if matches!(
                        self.seal_ctx.get_repr_kind(rel_def_id).unwrap(),
                        ReprKind::Unit
                    ) {
                        relationship.rel_params = RelParams::Unit;
                    }
                }
            }
        }
    }

    fn unique_domain_names(&self) -> FnvHashMap<PackageId, String> {
        let mut map: HashMap<String, PackageId> = HashMap::new();
        map.insert("ontol".into(), ONTOL_PKG);

        for (package_id, name) in &self.package_names {
            if map.contains_key(name.as_ref()) {
                todo!("Two distinct domains are called `{name}`. This is not handled yet");
            }

            map.insert(name.as_ref().clone(), *package_id);
        }

        // invert
        map.into_iter()
            .map(|(name, package_id)| (package_id, name))
            .collect()
    }

    /// Check for errors and bail out of the compilation process now, if in error state.
    pub(crate) fn check_error(&mut self) -> Result<(), UnifiedCompileError> {
        if self.errors.errors.is_empty() {
            Ok(())
        } else {
            Err(UnifiedCompileError {
                errors: std::mem::take(&mut self.errors.errors),
            })
        }
    }

    pub(crate) fn push_error(&mut self, error: SpannedCompileError) {
        self.errors.errors.push(error);
    }
}

impl<'m> AsRef<Defs<'m>> for Compiler<'m> {
    fn as_ref(&self) -> &Defs<'m> {
        &self.defs
    }
}

impl<'m> AsRef<DefTypes<'m>> for Compiler<'m> {
    fn as_ref(&self) -> &DefTypes<'m> {
        &self.def_types
    }
}

impl<'m> AsRef<Relations> for Compiler<'m> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}
