use codegen::task::{execute_codegen_tasks, CodegenTasks};
use def::{DefKind, Defs, LookupRelationshipMeta, RelationshipMeta, TypeDef};
use error::{CompileError, ParseError, UnifiedCompileError};

pub use error::*;
use expr::Expressions;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use lowering::Lowering;
use mem::Mem;
use namespace::Namespaces;
use ontol_runtime::{
    config::PackageConfig,
    ontology::{Domain, EntityInfo, EntityRelationship, MapMeta, Ontology, TypeInfo},
    serde::SerdeKey,
    value::PropertyId,
    DataModifier, DefId, DefVariant, PackageId,
};
use ontology_graph::OntologyGraph;
use package::{PackageTopology, Packages};
use patterns::{compile_all_patterns, Patterns};
use primitive::Primitives;
use relation::{Properties, Property, Relations};
use repr::repr_model::ReprCtx;
use serde_codegen::serde_generator::SerdeGenerator;
pub use source::*;
use strings::Strings;
use tracing::debug;
use types::{DefTypes, Types};

pub mod error;
pub mod hir_unify;
pub mod mem;
pub mod ontology_graph;
pub mod package;
pub mod repr;
pub mod serde_codegen;
pub mod source;
pub mod typed_hir;

mod codegen;
mod compiler_queries;
mod core;
mod def;
mod expr;
mod lowering;
mod namespace;
mod patterns;
mod primitive;
mod regex_util;
mod relation;
mod sequence;
mod strings;
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
    pub(crate) expressions: Expressions,

    pub(crate) strings: Strings<'m>,
    pub(crate) types: Types<'m>,
    pub(crate) def_types: DefTypes<'m>,
    pub(crate) relations: Relations,
    pub(crate) repr: ReprCtx,
    pub(crate) patterns: Patterns,

    pub(crate) codegen_tasks: CodegenTasks<'m>,

    pub(crate) errors: CompileErrors,
}

impl<'m> Compiler<'m> {
    pub fn new(mem: &'m Mem, sources: Sources) -> Self {
        let mut defs = Defs::new(mem);
        let primitives = Primitives::new(&mut defs);

        Self {
            sources,
            packages: Default::default(),
            namespaces: Default::default(),
            defs,
            package_config_table: Default::default(),
            primitives,
            expressions: Default::default(),
            strings: Strings::new(mem),
            types: Types::new(mem),
            def_types: Default::default(),
            relations: Relations::default(),
            repr: Default::default(),
            patterns: Patterns::default(),
            codegen_tasks: Default::default(),
            errors: Default::default(),
        }
    }

    /// Entry point of all compilation: Compiles the full package topology
    pub fn compile_package_topology(
        &mut self,
        topology: PackageTopology,
    ) -> Result<(), UnifiedCompileError> {
        let mut root_defs = vec![];

        for parsed_package in topology.packages {
            debug!(
                "lower package {:?}: {:?}",
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

            for error in parsed_package.parser_errors {
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

            let package_def_id = self.define_package(parsed_package.package_id);
            self.packages
                .loaded_packages
                .insert(parsed_package.reference, package_def_id);

            self.package_config_table
                .insert(parsed_package.package_id, parsed_package.config);

            let mut lowering = Lowering::new(self, &src);

            for stmt in parsed_package.statements {
                let _ignored = lowering.lower_statement(stmt);
            }

            root_defs.append(&mut lowering.finish());
        }

        self.compile_all_packages(root_defs)
    }

    fn compile_all_packages(&mut self, root_defs: Vec<DefId>) -> Result<(), UnifiedCompileError> {
        let mut type_check = self.type_check();
        for root_def in root_defs {
            type_check.check_def(root_def);
        }

        // Call this after all source files have been compiled
        compile_all_patterns(self);

        self.type_check().check_domain_types();
        self.repr_check();
        self.type_check().check_unions();

        self.check_error()?;

        execute_codegen_tasks(self);
        self.check_error()?;

        Ok(())
    }

    /// Get the current ontology graph (which is serde-serializable)
    pub fn ontology_graph(&self) -> OntologyGraph<'_, 'm> {
        OntologyGraph::from(self)
    }

    /// Finish compilation, turn into runtime ontology.
    pub fn into_ontology(mut self) -> Ontology {
        let package_ids = self.package_ids();

        let mut namespaces = std::mem::take(&mut self.namespaces.namespaces);
        let mut package_config_map = std::mem::take(&mut self.package_config_table);
        let docs = std::mem::take(&mut self.namespaces.docs);
        let mut serde_generator = self.serde_generator();

        let mut builder = Ontology::builder();

        let dynamic_sequence_operator_id = serde_generator.make_dynamic_sequence_operator();

        // For now, create serde operators for every domain
        for package_id in package_ids {
            let mut domain = Domain::default();

            let namespace = namespaces.remove(&package_id).unwrap();
            let type_namespace = namespace.types;

            if let Some(package_config) = package_config_map.remove(&package_id) {
                builder.add_package_config(package_id, package_config);
            }

            for (type_name, type_def_id) in type_namespace {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: match self.defs.get_def_kind(type_def_id) {
                        Some(DefKind::Type(TypeDef { public, .. })) => *public,
                        _ => true,
                    },
                    name: Some(type_name),
                    entity_info: self.entity_info(type_def_id, &mut serde_generator),
                    operator_id: serde_generator.gen_operator_id(SerdeKey::Def(DefVariant::new(
                        type_def_id,
                        DataModifier::default(),
                    ))),
                });
            }

            for type_def_id in namespace.anonymous {
                domain.add_type(TypeInfo {
                    def_id: type_def_id,
                    public: false,
                    name: None,
                    entity_info: None,
                    operator_id: serde_generator.gen_operator_id(SerdeKey::Def(DefVariant::new(
                        type_def_id,
                        DataModifier::default(),
                    ))),
                });
            }

            builder.add_domain(package_id, domain);
        }

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
            .string_patterns(self.patterns.string_patterns)
            .value_generators(self.relations.value_generators)
            .build()
    }

    fn entity_info(
        &self,
        type_def_id: DefId,
        serde_generator: &mut SerdeGenerator,
    ) -> Option<EntityInfo> {
        let properties = self.relations.properties_by_def_id(type_def_id)?;
        let id_relationship_id = properties.identified_by?;

        let identifies_meta = self
            .defs
            .lookup_relationship_meta(id_relationship_id)
            .expect("BUG: problem getting property meta");

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
            id_value_def_id: identifies_meta.relationship.subject.0.def_id,
            id_value_generator,
            id_operator_id: serde_generator
                .gen_operator_id(SerdeKey::Def(DefVariant::new(
                    identifies_meta.relationship.subject.0.def_id,
                    DataModifier::NONE,
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
        let meta = self
            .defs
            .lookup_relationship_meta(property_id.relationship_id)
            .unwrap();

        let (target_def_ref, _, _) = meta.relationship.right_side(property_id.role);

        if let Some(target_properties) = self.relations.properties_by_def_id(target_def_ref.def_id)
        {
            if target_properties.identified_by.is_some() {
                Some(EntityRelationship {
                    cardinality: property.cardinality,
                    target: target_def_ref.def_id,
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
    ) -> Option<RelationshipMeta<'m>> {
        let id_relationship_id = properties.identified_by?;
        let inherent_id = self
            .relations
            .inherent_id_map
            .get(&id_relationship_id)
            .cloned()?;
        let map = properties.table.as_ref()?;
        let _property = map.get(&PropertyId::subject(inherent_id))?;

        self.defs.lookup_relationship_meta(inherent_id).ok()
    }

    fn package_ids(&self) -> Vec<PackageId> {
        self.namespaces.namespaces.keys().copied().collect()
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
