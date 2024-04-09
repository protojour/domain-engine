use ontol_runtime::{DefId, PackageId};
use tracing::debug;

use crate::{
    def::{DefKind, RelParams},
    lowering::Lowering,
    package::ParsedPackage,
    repr::repr_model::ReprKind,
    thesaurus::TypeRelation,
    CompileError, Compiler, LexError, ParseError, Src, UnifiedCompileError,
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
            self.type_check().check_def(def_id);
        }

        self.seal_domain(package.package_id);

        self.package_names
            .push((package.package_id, self.strings.intern_constant(&src.name)));

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
                        .relations
                        .properties_by_def_id
                        .get(&is.def_id)
                        .and_then(|properties| properties.identified_by);

                    if identified_by.is_some() {
                        self.errors
                            .push(CompileError::EntityCannotBeSupertype.spanned(span));
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
                            type_check.errors.push(error);
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
                if let RelParams::Type(rel_def_id) = &relationship.rel_params {
                    if matches!(
                        self.repr_ctx.get_repr_kind(rel_def_id).unwrap(),
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
                } = &def.kind
                {
                    if let Some(extern_def_id) = extern_def_id {
                        type_check.check_map_extern(def, *arms, *extern_def_id);
                    } else {
                        match type_check.check_map(def, var_alloc, *arms) {
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
