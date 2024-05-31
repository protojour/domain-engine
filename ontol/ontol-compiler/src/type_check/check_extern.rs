use ontol_runtime::{
    ontology::{map::Extern, ontol::TextConstant},
    DefId,
};
use tracing::warn;

use crate::{
    codegen::{
        task::{ExternMap, MapCodegenRequest},
        type_mapper::TypeMapper,
    },
    def::{Def, DefKind, LookupRelationshipMeta},
    mem::Intern,
    pattern::{PatId, PatternKind, TypePath},
    types::{Type, TypeRef},
    CompileError, SourceSpan,
};

use super::TypeCheck;

struct ExternBuilder {
    url: Option<TextConstant>,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_extern(&mut self, def_id: DefId, span: SourceSpan) {
        warn!("check extern");

        let Some(table) = self.rel_ctx.properties_table_by_def_id(def_id) else {
            CompileError::TODO("extern has no properties")
                .span(span)
                .report(self);
            return;
        };

        let mut extern_builder = ExternBuilder { url: None };

        for property_id in table.keys() {
            let meta = self.defs.relationship_meta(property_id.relationship_id);
            let (value_type_def_id, ..) = meta.relationship.by(property_id.role.opposite());

            match meta.relation_def_kind.value {
                DefKind::TextLiteral(prop_name) => match *prop_name {
                    "url" => {
                        if let Some(url) = self.get_string_constant(value_type_def_id) {
                            extern_builder.url = Some(self.str_ctx.intern_constant(&url));
                        }
                    }
                    _ => {
                        CompileError::TODO("unknown property name for extern")
                            .span(*meta.relationship.span)
                            .report(&mut self.errors);
                    }
                },
                _ => {
                    CompileError::TODO("unknown property for extern")
                        .span(*meta.relationship.span)
                        .report(&mut self.errors);
                }
            }
        }

        let Ok(ontology_extern) = self.build_ontology_extern(extern_builder, span) else {
            return;
        };

        self.def_ty_ctx
            .ontology_externs
            .insert(def_id, ontology_extern);
    }

    fn build_ontology_extern(
        &mut self,
        builder: ExternBuilder,
        span: SourceSpan,
    ) -> Result<Extern, ()> {
        let Some(url) = builder.url else {
            CompileError::TODO("extern has no url")
                .span(span)
                .report(self);
            return Err(());
        };

        Ok(Extern::HttpJson { url })
    }

    fn get_string_constant(&self, def_id: DefId) -> Option<String> {
        if let DefKind::TextLiteral(lit) = self.defs.def_kind(def_id) {
            Some((*lit).into())
        } else {
            None
        }
    }

    /// Check a mapping within an extern definition
    pub fn check_map_extern(
        &mut self,
        def: &Def,
        pat_ids: [PatId; 2],
        extern_def_id: DefId,
    ) -> TypeRef<'m> {
        self.check_def(extern_def_id);

        let first = self.check_arm(pat_ids[0]);
        let second = self.check_arm(pat_ids[1]);

        let extern_map = ExternMap {
            extern_def_id,
            map_def_id: def.id,
        };

        if let Some(key_pair) = TypeMapper::new(self.rel_ctx, self.defs, self.repr_ctx)
            .find_map_key_pair([first, second])
        {
            let first_def_id = first.get_single_def_id().unwrap();
            let request = if key_pair.first().def_id == first_def_id {
                MapCodegenRequest::ExternForward(extern_map)
            } else {
                MapCodegenRequest::ExternBackward(extern_map)
            };

            self.code_ctx
                .add_map_task(key_pair, request, self.defs, self.errors);
        }

        self.types.intern(Type::Tautology)
    }

    fn check_arm(&mut self, pat_id: PatId) -> TypeRef<'m> {
        let pattern = self.patterns.table.remove(&pat_id).unwrap();

        match pattern.kind {
            PatternKind::Compound {
                type_path: TypePath::Specified { def_id, .. },
                attributes,
                ..
            } => {
                if !attributes.is_empty() {
                    CompileError::TODO("external mapping cannot have attributes")
                        .span(pattern.span)
                        .report(self);
                }

                self.check_def(def_id)
            }
            _ => CompileError::TODO("must specify a named pattern")
                .span(pattern.span)
                .report_ty(self),
        }
    }
}
