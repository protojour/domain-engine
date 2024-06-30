use fnv::FnvHashSet;
use ontol_runtime::{ontology::ontol::TextConstant, DefId, EdgeId, PackageId};
use tracing::{debug, debug_span};

use crate::{
    def::{rel_def_meta, DefKind, RelParams},
    edge::EdgeCtx,
    package::ParsedPackage,
    repr::repr_model::ReprKind,
    thesaurus::{Thesaurus, TypeRelation},
    type_check::MapArmsKind,
    CompileError, Compiler, Session, Src, UnifiedCompileError,
};

impl<'m> Compiler<'m> {
    /// Lower statements from the next domain,
    /// perform type check against its dependent domains,
    /// and seal the types at the end.
    pub(super) fn lower_and_check_next_domain(
        &mut self,
        package: ParsedPackage,
        src: Src,
    ) -> Result<(), UnifiedCompileError> {
        let _entered = debug_span!("pkg", id = ?package.package_id.id()).entered();

        for error in package.parse_errors {
            match error {
                ontol_parser::Error::Lex(lex_error) => {
                    let span = lex_error.span;
                    CompileError::Lex(lex_error.msg).span(src.span(span))
                }
                ontol_parser::Error::Parse(parse_error) => {
                    let span = parse_error.span;
                    CompileError::Parse(parse_error.msg).span(src.span(span))
                }
            }
            .report(self);
        }

        let pkg_def_id = self.defs.alloc_def_id(package.package_id);
        self.define_package(pkg_def_id);

        self.packages
            .loaded_packages
            .insert(package.reference, pkg_def_id);

        self.package_config_table
            .insert(package.package_id, package.config);

        let root_defs = package.syntax.lower(pkg_def_id, src.clone(), Session(self));

        for def_id in root_defs {
            self.type_check().check_def(def_id);
        }

        self.seal_domain(package.package_id);

        self.package_names
            .push((package.package_id, self.str_ctx.intern_constant(&src.name)));

        self.check_error()
    }

    /// Do all the (remaining) checks and generations for the package/domain and seal it
    /// Initial check_def must be done before this
    pub(crate) fn seal_domain(&mut self, package_id: PackageId) {
        debug!("seal {package_id:?}");

        {
            let mut type_check = self.type_check();

            // pre repr checks
            for def_id in type_check.defs.iter_package_def_ids(package_id) {
                if let Some(def) = type_check.defs.table.get(&def_id) {
                    if let DefKind::Type(_) = &def.kind {
                        type_check.check_domain_type_pre_repr(def_id, def);
                    }
                }
            }

            // repr checks
            for def_id in type_check.defs.iter_package_def_ids(package_id) {
                type_check.repr_check(def_id).check_repr_root();
            }

            // domain type checks
            for def_id in type_check.defs.iter_package_def_ids(package_id) {
                if let Some(def) = type_check.defs.table.get(&def_id) {
                    if let DefKind::Type(_) = &def.kind {
                        type_check.check_domain_type_post_repr(def_id, def);
                    }
                }
            }
        }

        // entity check
        // this is not in the TypeCheck context because it may
        // generate new DefIds
        for def_id in self.defs.iter_package_def_ids(package_id) {
            self.check_entity(def_id);
        }

        // check that no types use entities as supertypes
        for (_, is_table) in self.thesaurus.iter() {
            for (is, span) in is_table {
                if matches!(&is.rel, TypeRelation::Super) {
                    let identified_by = self
                        .rel_ctx
                        .properties_by_def_id
                        .get(&is.def_id)
                        .and_then(|properties| properties.identified_by);

                    if identified_by.is_some() {
                        CompileError::EntityCannotBeSupertype
                            .span(*span)
                            .report(&mut self.errors);
                    }
                }
            }
        }

        // symbolic edge check
        for edge_id in self.defs.iter_package_def_ids(package_id) {
            if let Some(DefKind::Edge) = self.defs.def_kind_option(edge_id) {
                if let Some(edge) = self.edge_ctx.symbolic_edges.get_mut(&EdgeId(edge_id)) {
                    for variable in edge.variables.values_mut() {
                        if variable.def_set.is_empty() {
                            CompileError::SymEdgeNoDefinitionForExistentialVar
                                .span(variable.span)
                                .report(&mut self.errors);
                        } else {
                            // refine def set
                            // refinement means that it should consist only of entity defs.

                            let raw_def_set = std::mem::take(&mut variable.def_set);
                            let mut refined_def_set: FnvHashSet<DefId> = Default::default();

                            for def_id in raw_def_set {
                                if let Some(properties) = self.rel_ctx.properties_by_def_id(def_id)
                                {
                                    if let Some(identifies_rel_id) = properties.identifies {
                                        let meta = rel_def_meta(identifies_rel_id, &self.defs);

                                        refined_def_set.insert(meta.relationship.object.0);
                                    } else {
                                        refined_def_set.insert(def_id);
                                    }
                                }
                            }

                            variable.def_set = refined_def_set;
                        }
                    }
                }
            }
        }

        {
            let mut type_check = self.type_check();

            // union and extern checks
            for def_id in type_check.defs.iter_package_def_ids(package_id) {
                match type_check.repr_ctx.get_repr_kind(&def_id) {
                    Some(ReprKind::Union(_) | ReprKind::StructUnion(_)) => {
                        for error in type_check.check_union(def_id) {
                            error.report(&mut type_check);
                        }
                    }
                    Some(ReprKind::Extern) => {
                        type_check.check_extern(def_id, type_check.defs.def_span(def_id));
                    }
                    _ => {}
                }
            }
        }

        // Various cleanup/normalization
        for def_id in self.defs.iter_package_def_ids(package_id) {
            let Some(def) = self.defs.table.get_mut(&def_id) else {
                // Can happen in error cases
                continue;
            };

            if let DefKind::Relationship(relationship) = &mut def.kind {
                // Reset RelParams::Type back to RelParams::Unit if its representation is ReprKind::Unit.
                // This simplifies later compiler stages, that can trust RelParams::Type is a type with real data in it.
                if let RelParams::Type(rel_params_def_id) = &relationship.rel_params {
                    copy_relationship_store_key(
                        *rel_params_def_id,
                        def_id,
                        &mut self.edge_ctx,
                        &self.thesaurus,
                    );

                    if matches!(
                        self.repr_ctx.get_repr_kind(rel_params_def_id).unwrap(),
                        ReprKind::Unit
                    ) {
                        relationship.rel_params = RelParams::Unit;
                    }
                }
            }
        }

        // check map statements
        {
            let mut map_defs: Vec<DefId> = vec![];

            for def_id in self.defs.iter_package_def_ids(package_id) {
                {
                    let Some(def) = self.defs.table.get(&def_id) else {
                        // Can happen in error cases
                        continue;
                    };
                    if matches!(&def.kind, DefKind::Mapping { .. }) {
                        map_defs.push(def_id);
                    } else {
                        continue;
                    }
                }

                // Infer anonymous types at root of named maps
                if let Some(inference_info) = self.check_map_arm_def_inference(def_id) {
                    self.type_check().check_def(inference_info.source.1);
                    let new_defs = self
                        .map_arm_def_inferencer(def_id)
                        .infer_map_arm_type(inference_info);

                    for def_id in new_defs {
                        self.type_check().check_def(def_id);
                    }

                    self.type_check().check_def(inference_info.target.1);
                }
            }

            let mut type_check = self.type_check();

            for def_id in map_defs {
                let def = type_check.defs.table.get(&def_id).unwrap();

                if let DefKind::Mapping {
                    ident: _,
                    arms,
                    var_alloc,
                    extern_def_id,
                    is_abstract,
                } = &def.kind
                {
                    if let Some(extern_def_id) = extern_def_id {
                        type_check.check_map_extern(def, *arms, *extern_def_id);
                    } else {
                        match type_check.check_map(
                            (def.id, def.span),
                            var_alloc,
                            *arms,
                            if *is_abstract {
                                MapArmsKind::Abstract
                            } else {
                                MapArmsKind::Concrete
                            },
                        ) {
                            Ok(_) => {}
                            Err(error) => {
                                debug!("Check map error: {error:?}");
                            }
                        }
                    }
                }
            }
        }

        self.seal_ctx.mark_domain_sealed(package_id);
    }
}

/// Copies a store key registered for a set of rel_params,
/// which may be reset to RelParams::Unit if no named parameters are present,
/// and makes the store key available via a RelationshipId (DefId) instead.
fn copy_relationship_store_key(
    rel_params_def_id: DefId,
    rel_def_id: DefId,
    edge_ctx: &mut EdgeCtx,
    thesaurus: &Thesaurus,
) {
    fn recurse_search(
        def_id: DefId,
        edge_ctx: &mut EdgeCtx,
        thesaurus: &Thesaurus,
        result: &mut Option<TextConstant>,
        visited: &mut FnvHashSet<DefId>,
    ) {
        if !visited.insert(def_id) {
            return;
        }

        if let Some(text_constant) = edge_ctx.store_keys.get(&def_id) {
            *result = Some(*text_constant);
            return;
        }

        for entry in thesaurus.entries_raw(def_id) {
            if matches!(entry.rel, TypeRelation::Super) {
                recurse_search(entry.def_id, edge_ctx, thesaurus, result, visited);
            }
        }
    }

    let mut store_key: Option<TextConstant> = None;

    recurse_search(
        rel_params_def_id,
        edge_ctx,
        thesaurus,
        &mut store_key,
        &mut Default::default(),
    );

    if let Some(store_key) = store_key {
        edge_ctx.store_keys.insert(rel_def_id, store_key);
    }
}
