use std::sync::Arc;

use codegen::task::{execute_codegen_tasks, CodegenTasks};
use def::{DefKind, Defs, LookupRelationshipMeta, RelationshipMeta, TypeDef};

pub use error::*;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use interface::{
    graphql::generate::generate_graphql_schema, serde::serde_generator::SerdeGenerator,
};
use lowering::Lowering;
use mem::Mem;
use namespace::Namespaces;
use ontol_runtime::{
    config::PackageConfig,
    interface::serde::{SerdeDef, SerdeKey, SerdeModifier},
    ontology::{
        Domain, DomainInterface, EntityInfo, EntityRelationship, MapMeta, Ontology, TypeInfo,
    },
    value::PropertyId,
    DefId, PackageId,
};
use ontology_graph::OntologyGraph;
use package::{PackageTopology, Packages, ParsedPackage, ONTOL_PKG};
use pattern::Patterns;
use primitive::Primitives;
use relation::{Properties, Property, Relations};
pub use source::*;
use strings::Strings;
use text_patterns::{compile_all_text_patterns, TextPatterns};
use tracing::debug;
use type_check::seal::SealCtx;
use types::{DefTypes, Types};

use crate::{def::RelParams, type_check::repr::repr_model::ReprKind};

pub mod error;
pub mod hir_unify;
pub mod interface;
pub mod mem;
pub mod ontology_graph;
pub mod package;
pub mod source;
pub mod typed_hir;

mod codegen;
mod compiler_queries;
mod def;
mod lowering;
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
mod types;

#[derive(Debug)]
pub struct Compiler<'m> {
    pub sources: Sources,

    pub(crate) packages: Packages,

    pub(crate) namespaces: Namespaces,
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

        let root_defs = {
            let mut lowering = Lowering::new(self, &src);

            for stmt in package.statements {
                let _ignored = lowering.lower_statement(stmt);
            }

            lowering.finish()
        };

        let mut type_check = self.type_check();
        for root_def in root_defs {
            type_check.check_def_shallow(root_def);
        }

        self.seal_domain(package.package_id);

        self.check_error()
    }

    /// Get the current ontology graph (which is serde-serializable)
    pub fn ontology_graph(&self) -> OntologyGraph<'_, 'm> {
        OntologyGraph::from(self)
    }

    /// Finish compilation, turn into runtime ontology.
    pub fn into_ontology(mut self) -> Ontology {
        let package_ids = self.package_ids();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut package_config_table = std::mem::take(&mut self.package_config_table);
        let docs = std::mem::take(&mut self.namespaces.docs);
        let mut serde_generator = self.serde_generator();

        let mut builder = Ontology::builder();

        let dynamic_sequence_operator_id = serde_generator.make_dynamic_sequence_operator();

        // For now, create serde operators for every domain
        for package_id in package_ids.iter().cloned() {
            let mut domain = Domain::default();

            let namespace = namespaces.remove(&package_id).unwrap();
            let type_namespace = namespace.types;

            if let Some(package_config) = package_config_table.remove(&package_id) {
                builder.add_package_config(package_id, package_config);
            }

            for (type_name, type_def_id) in type_namespace {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: match self.defs.def_kind(type_def_id) {
                        DefKind::Type(TypeDef { public, .. }) => *public,
                        _ => true,
                    },
                    name: Some(type_name),
                    entity_info: self.entity_info(type_def_id, &mut serde_generator),
                    operator_id: serde_generator.gen_operator_id(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                });
            }

            for type_def_id in namespace.anonymous {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: false,
                    name: None,
                    entity_info: None,
                    operator_id: serde_generator.gen_operator_id(SerdeKey::Def(SerdeDef::new(
                        type_def_id,
                        SerdeModifier::json_default(),
                    ))),
                });
            }

            builder.add_domain(package_id, domain);
        }

        // interface handling
        let domain_interfaces = {
            let mut interfaces: FnvHashMap<PackageId, Vec<DomainInterface>> = Default::default();
            for package_id in package_ids.iter().cloned() {
                if package_id != ONTOL_PKG {
                    let schema = generate_graphql_schema(
                        package_id,
                        builder.partial_ontology(),
                        &mut serde_generator,
                    );
                    interfaces
                        .entry(package_id)
                        .or_default()
                        .push(DomainInterface::GraphQL(Arc::new(schema)));
                }
            }
            interfaces
        };

        let (serde_operators, serde_operators_per_def) = serde_generator.finish();

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

                (
                    key,
                    MapMeta {
                        procedure,
                        propflow_range,
                    },
                )
            })
            .collect();

        builder
            .lib(self.codegen_tasks.result_lib)
            .docs(docs)
            .const_procs(self.codegen_tasks.result_const_procs)
            .map_meta_table(map_meta_table)
            .serde_operators(serde_operators, serde_operators_per_def)
            .dynamic_sequence_operator_id(dynamic_sequence_operator_id)
            .property_flows(property_flows)
            .string_like_types(self.defs.string_like_types)
            .text_patterns(self.text_patterns.text_patterns)
            .value_generators(self.relations.value_generators)
            .domain_interfaces(domain_interfaces)
            .build()
    }

    fn entity_info(
        &self,
        type_def_id: DefId,
        serde_generator: &mut SerdeGenerator,
    ) -> Option<EntityInfo> {
        let properties = self.relations.properties_by_def_id(type_def_id)?;
        let id_relationship_id = properties.identified_by?;

        let identifies_meta = self.defs.relationship_meta(id_relationship_id);

        let mut entity_relationships: IndexMap<PropertyId, EntityRelationship> =
            IndexMap::default();

        // inherent properties are the properties that are _not_ entity relationships:
        let mut inherent_property_count = 0;

        if let Some(table) = &properties.table {
            for (property_id, property) in table {
                if let Some(entity_relationship) =
                    self.get_entity_relationship(*property_id, property)
                {
                    entity_relationships.insert(*property_id, entity_relationship);
                } else {
                    inherent_property_count += 1;
                }
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
            id_operator_id: serde_generator
                .gen_operator_id(SerdeKey::Def(SerdeDef::new(
                    identifies_meta.relationship.subject.0,
                    SerdeModifier::NONE,
                )))
                .unwrap(),
            entity_relationships,
        })
    }

    fn get_entity_relationship(
        &self,
        property_id: PropertyId,
        property: &Property,
    ) -> Option<EntityRelationship> {
        let meta = self.defs.relationship_meta(property_id.relationship_id);

        let (target_def_id, _, _) = meta.relationship.by(property_id.role.opposite());

        if let Some(target_properties) = self.relations.properties_by_def_id(target_def_id) {
            if target_properties.identified_by.is_some() {
                Some(EntityRelationship {
                    cardinality: property.cardinality,
                    target: target_def_id,
                })
            } else {
                None
            }
        } else {
            None
        }
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
