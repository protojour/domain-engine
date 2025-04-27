use std::collections::BTreeSet;

use either::Either;
use ontol_log::{
    log_model::{TypeRef, TypeRefOrUnionOrPattern},
    tag::Tag,
    with_span::WithSpan,
};
use ontol_parser::source::SourceSpan;
use ontol_runtime::{DefId, DomainIndex, OntolDefTag, OntolDefTagExt};

use crate::{
    CompileError,
    def::{DefKind, TypeDef, TypeDefFlags},
    lowering::context::{LoweringOutcome, MapVarTable},
};

use super::{LogLowering, TypeContext};

impl LogLowering<'_, '_, '_> {
    pub(super) fn resolve_type_ref(
        &mut self,
        ty: &Either<WithSpan<TypeRef>, BTreeSet<WithSpan<TypeRef>>>,
        type_ctx: &mut TypeContext,
    ) -> Option<(DefId, SourceSpan)> {
        match ty {
            Either::Left(ty) => self.resolve_unit_type_ref(ty, type_ctx),
            Either::Right(union) => {
                todo!("resolve {union:?}")
            }
        }
    }

    pub(super) fn resolve_type_ref_or_pattern(
        &mut self,
        typ: &TypeRefOrUnionOrPattern,
        type_ctx: &mut TypeContext,
    ) -> Option<(DefId, SourceSpan)> {
        match typ {
            TypeRefOrUnionOrPattern::Type(ty) => self.resolve_unit_type_ref(ty, type_ctx),
            TypeRefOrUnionOrPattern::Union(_set) => todo!(),
            TypeRefOrUnionOrPattern::Pattern(pattern) => {
                let mut var_table = MapVarTable::default();
                let lowered = self.lower_pattern(pattern, &mut var_table);
                let pat_id = self.compiler.patterns.alloc_pat_id();
                self.compiler.patterns.table.insert(pat_id, lowered);
                let def_id = self.define_anonymous(DefKind::Constant(pat_id), pattern.span());

                Some((def_id, self.source_id.span(pattern.span())))
            }
        }
    }

    pub(super) fn resolve_unit_type_ref(
        &mut self,
        ty: &WithSpan<TypeRef>,
        type_ctx: &mut TypeContext,
    ) -> Option<(DefId, SourceSpan)> {
        let def_id = match &ty.0 {
            TypeRef::Text(text) => self.literal_text_def_id(text),
            TypeRef::Number(str) => {
                let lit = self.compiler.str_ctx.intern(str);
                let span = self.source_id.span(ty.span());
                self.compiler.defs.add_transient_def(
                    DefKind::NumberLiteral(lit),
                    DomainIndex::ontol(),
                    span,
                )
            }
            TypeRef::Regex(_src) => todo!(),
            TypeRef::NumberRange(_start, _end) => {
                self.compiler.errors.push(
                    CompileError::TODO("number range is not a proper type")
                        .span(self.source_id.span(ty.span())),
                );
                return None;
            }
            TypeRef::Path(path_ref) => match path_ref {
                ontol_log::log_model::PathRef::Local(tag) => {
                    self.mk_persistent_def_id(self.domain_index, *tag)
                }
                ontol_log::log_model::PathRef::Foreign(_use_tag, _foreign_tag) => todo!(),
                ontol_log::log_model::PathRef::ParentRel => {
                    if let TypeContext::RelParams {
                        rel_id, rel_def_id, ..
                    } = type_ctx
                    {
                        if rel_def_id.is_none() {
                            // Define anonymous DefId for the relation
                            let new_rel_def_id = self.define_anonymous(
                                DefKind::Type(TypeDef {
                                    ident: None,
                                    rel_type_for: Some(*rel_id),
                                    flags: TypeDefFlags::CONCRETE,
                                }),
                                ty.span(),
                            );

                            self.compiler
                                .namespaces
                                .add_anonymous(self.subdomain_def_id, new_rel_def_id);
                            self.outcome.root_defs.push(new_rel_def_id);

                            *rel_def_id = Some(new_rel_def_id);
                        }

                        rel_def_id.unwrap()
                    } else {
                        todo!("report error");
                    }
                }
                ontol_log::log_model::PathRef::LocalArc(_tag, _coord) => todo!(),
                ontol_log::log_model::PathRef::ForeignArc(_use_tag, _tag, _coord) => todo!(),
                ontol_log::log_model::PathRef::Ontol(ontol_def_tag) => {
                    DefId::new_persistent(DomainIndex::ontol(), *ontol_def_tag as u16)
                }
            },
            TypeRef::Anonymous(_tree) => todo!("anonymous type"),
        };

        Some((def_id, self.source_id.span(ty.span())))
    }

    fn literal_text_def_id(&mut self, text: &str) -> DefId {
        match text {
            "" => OntolDefTag::EmptyText.def_id(),
            other => self
                .compiler
                .defs
                .def_text_literal(other, &mut self.compiler.str_ctx),
        }
    }

    pub fn into_outcome(self) -> LoweringOutcome {
        self.outcome
    }

    pub fn mk_persistent_def_id(&mut self, domain_index: DomainIndex, tag: Tag) -> DefId {
        match u16::try_from(tag.0) {
            Ok(tag) => self
                .compiler
                .defs
                .register_persistent_def_id(domain_index, tag),
            Err(_) => {
                panic!("FIXME: DefId tags exceeeded");
            }
        }
    }

    pub fn resolve_foreign_domain_index(&mut self, local_use_tag: Tag) -> Option<DomainIndex> {
        let local_model = self.global_model.get_model(self.local_log)?;
        let _use_data = local_model.uses.get(&local_use_tag)?;

        // self.compiler;

        todo!()
    }
}
