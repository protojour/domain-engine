//! Checks whether types have a valid and _concrete_ representation.
//! It is not an error (in itself) for a type to be abstract,
//! but an abstract type does not form a complete domain in itself.
//!
//! So the responsibility of this code is just to record the facts,
//! and those facts are used in later compilation stages.

use std::collections::{HashMap, hash_map::Entry};

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_core::tag::DomainIndex;
use ontol_parser::source::{NATIVE_SOURCE, NO_SPAN, SourceId, SourceSpan};
use ontol_runtime::{DefId, OntolDefTag, OntolDefTagExt, ontology::ontol::TextLikeType};
use ordered_float::NotNan;
use tracing::{debug, debug_span, trace};

use crate::{
    CompileErrors, Note, SpannedNote,
    def::{Def, DefKind, Defs, TypeDefFlags},
    error::CompileError,
    misc::MiscCtx,
    primitive::PrimitiveKind,
    properties::{Constructor, PropCtx, Properties},
    relation::{RelCtx, RelId, RelParams, rel_def_meta},
    repr::repr_model::UnionBound,
    thesaurus::{Is, Thesaurus, TypeRelation},
    types::{DefTypeCtx, Type},
};

use super::{
    repr_ctx::ReprCtx,
    repr_model::{NumberResolution, Repr, ReprBuilder, ReprFormat, ReprKind, ReprScalarKind},
};

/// If there are repr problems in the ONTOL domain, turn this on.
/// Otherwise, it's too verbose.
const TRACE_BUILTIN: bool = false;

pub struct ReprCheck<'c, 'm> {
    pub root_def_id: DefId,
    pub defs: &'c Defs<'m>,
    pub def_types: &'c DefTypeCtx<'m>,
    pub rel_ctx: &'c RelCtx,
    pub prop_ctx: &'c PropCtx,
    pub misc_ctx: &'c MiscCtx,
    pub thesaurus: &'c Thesaurus,
    pub repr_ctx: &'c mut ReprCtx,

    pub errors: &'c mut CompileErrors,

    pub state: State,
}

impl AsMut<CompileErrors> for ReprCheck<'_, '_> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        self.errors
    }
}

#[derive(Default)]
pub struct State {
    /// The repr check is coinductive,
    /// i.e. cycles mean that the repr is OK.
    /// Cycles appear in recursive types (tree structures, for example).
    pub visited: FnvHashSet<DefId>,

    pub span_stack: Vec<SpanNode>,

    /// If this is non-empty, the type is not representable
    pub abstract_notes: Vec<SpannedNote>,

    circular_spans: Vec<SourceSpan>,

    duplicate_type_params: IndexMap<DefId, Vec<SourceSpan>>,

    /// Diagnostics will be emitted if the root type
    /// has to be concrete
    do_emit_diagnostics: bool,

    do_trace: bool,
}

pub struct SpanNode {
    span: SourceSpan,
    kind: SpanKind,
}

enum SpanKind {
    Type(DefId),
    Field,
}

impl ReprCheck<'_, '_> {
    /// Check the representation of a type
    pub fn check_repr_root(&mut self) {
        let Some(def) = self.defs.table.get(&self.root_def_id) else {
            // This can happen in case of errors
            return;
        };

        let _entered = debug_span!("repr", id = ?self.root_def_id).entered();

        self.state.do_trace =
            TRACE_BUILTIN || self.root_def_id.domain_index() != DomainIndex::ontol();

        self.check_def_repr(
            self.root_def_id,
            def,
            self.prop_ctx.properties_by_def_id(self.root_def_id),
        );

        if self.state.do_emit_diagnostics && !self.state.abstract_notes.is_empty() {
            CompileError::TypeNotRepresentable
                .span(def.span)
                .with_notes(std::mem::take(&mut self.state.abstract_notes))
                .report(self.errors);
        }

        trace!(
            "result repr: {:?}",
            self.repr_ctx.get_repr_kind(&self.root_def_id)
        );
    }

    fn check_def_repr(&mut self, def_id: DefId, def: &Def, properties: Option<&Properties>) {
        if !self.state.visited.insert(def_id) {
            return;
        }

        self.state.span_stack.push(SpanNode {
            span: def.span,
            kind: SpanKind::Type(def_id),
        });

        let repr_result = self.compute_repr_cached_recursive(def_id, properties);

        if repr_result.is_err() && self.state.do_emit_diagnostics {
            for span_node in self.state.span_stack.iter().rev() {
                if span_node.span.source_id == NATIVE_SOURCE {
                    continue;
                }

                match span_node.kind {
                    SpanKind::Type(span_def_id) => {
                        if span_def_id != self.root_def_id {
                            self.state
                                .abstract_notes
                                .push(Note::TypeIsAbstract.span(span_node.span));
                        }
                    }
                    SpanKind::Field => {
                        self.state
                            .abstract_notes
                            .push(Note::FieldTypeIsAbstract.span(span_node.span));
                    }
                }
            }
        }

        self.state.span_stack.pop();
    }

    fn compute_repr_cached_recursive(
        &mut self,
        def_id: DefId,
        properties: Option<&Properties>,
    ) -> Result<(), ()> {
        if self.repr_ctx.repr_table.contains_key(&def_id) {
            return Ok(());
        }

        let repr = self.compute_repr(def_id);

        if def_id == self.root_def_id
            && matches!(
                repr.as_ref().map(|repr| &repr.kind),
                Some(ReprKind::Struct | ReprKind::StructIntersection(_) | ReprKind::Seq)
            )
        {
            // Failure to repr check any member type
            // will result in error diagnostics for the root type:
            self.state.do_emit_diagnostics = true;
        }

        // traverse members (i.e. properties)
        if let Some(properties) = properties {
            if let Some(table) = &properties.table {
                for property in table.values() {
                    self.traverse_property(property.rel_id);
                }
            }

            if let Constructor::Sequence(seq) = &properties.constructor {
                for (_, relationship_id) in seq.elements() {
                    if let Some(relationship_id) = relationship_id {
                        self.traverse_property(relationship_id);
                    }
                }
            }
        }

        if let Some(repr) = repr {
            if self.state.do_trace {
                trace!(
                    " => {def_id:?}({:?}) result repr: {repr:?}",
                    self.defs.def_kind(def_id).opt_identifier()
                );
            }

            let old = self.repr_ctx.repr_table.insert(def_id, repr);
            if old.is_some() {
                panic!("already contained {def_id:?}");
            }
            Ok(())
        } else {
            Err(())
        }
    }

    fn traverse_property(&mut self, rel_id: RelId) {
        let meta = rel_def_meta(rel_id, self.rel_ctx, self.defs);

        let (value_def_id, ..) = meta.relationship.object();
        let value_def = self.defs.table.get(&value_def_id).unwrap();

        if let Some(Type::Error) = self.def_types.def_table.get(&value_def_id) {
            // Avoid reporting repr errors when there are type errors on the field,
            // the type errors take precedence.
            return;
        }

        self.state.span_stack.push(SpanNode {
            span: meta.relationship.object.1,
            kind: SpanKind::Field,
        });
        self.check_def_repr(
            value_def_id,
            value_def,
            self.prop_ctx.properties_by_def_id(value_def_id),
        );
        self.state.span_stack.pop();

        if let RelParams::Def(def_id) = &meta.relationship.rel_params {
            let rel_def = self.defs.table.get(def_id).unwrap();

            self.state.span_stack.push(SpanNode {
                span: rel_def.span,
                kind: SpanKind::Field,
            });
            self.check_def_repr(
                *def_id,
                value_def,
                self.prop_ctx.properties_by_def_id(*def_id),
            );
            self.state.span_stack.pop();
        }
    }

    fn compute_repr(&mut self, leaf_def_id: DefId) -> Option<Repr> {
        let mesh = self.collect_thesaurus(leaf_def_id);

        if mesh.len() > 1 && self.state.do_trace {
            trace!("    mesh for {leaf_def_id:?}: {:?}", mesh);
        }

        let mut builder = ReprBuilder {
            kind: None,
            number_resolutions: Default::default(),
            type_params: Default::default(),
            formats: Default::default(),
        };

        for (def_id, data) in &mesh {
            let def_id = *def_id;

            match self.defs.def_kind(def_id) {
                DefKind::Primitive(kind, _ident) => {
                    if matches!(data.rel, IsRelation::Sub) {
                        // union-like containing a primitive variant.
                        // This only occurs in user domains.
                        self.merge_repr(
                            &mut builder,
                            leaf_def_id,
                            ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                            def_id,
                            data,
                        );
                    } else {
                        match kind {
                            PrimitiveKind::Unit => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Unit,
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Boolean | PrimitiveKind::False | PrimitiveKind::True => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Scalar(
                                        def_id,
                                        ReprScalarKind::Boolean,
                                        data.rel_span,
                                    ),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Text => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Scalar(def_id, ReprScalarKind::Text, data.rel_span),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Number => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Serial => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Scalar(def_id, ReprScalarKind::Serial, data.rel_span),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Octets => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Scalar(
                                        def_id,
                                        ReprScalarKind::Octets(ReprFormat::Unspecified),
                                        data.rel_span,
                                    ),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Vertex => {
                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Scalar(def_id, ReprScalarKind::Vertex, data.rel_span),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Integer => {
                                builder
                                    .number_resolutions
                                    .entry(NumberResolution::Integer)
                                    .or_insert(data.rel_span);
                            }
                            PrimitiveKind::Float => {
                                builder
                                    .number_resolutions
                                    .entry(NumberResolution::Float)
                                    .or_insert(data.rel_span);
                            }
                            PrimitiveKind::F32 => {
                                builder
                                    .number_resolutions
                                    .entry(NumberResolution::F32)
                                    .or_insert(data.rel_span);
                            }
                            PrimitiveKind::F64 => {
                                builder
                                    .number_resolutions
                                    .entry(NumberResolution::F64)
                                    .or_insert(data.rel_span);
                            }
                            PrimitiveKind::Format => {
                                let repr_format = if def_id == OntolDefTag::FormatHex.def_id() {
                                    Some(ReprFormat::Hex)
                                } else if def_id == OntolDefTag::FormatBase64.def_id() {
                                    Some(ReprFormat::Base64)
                                } else {
                                    debug!("unrecognized repr format");
                                    None
                                };

                                if let Some(repr_format) = repr_format {
                                    builder.formats.entry(repr_format).or_insert(data.rel_span);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                DefKind::TextLiteral(_) => {
                    self.merge_repr(
                        &mut builder,
                        leaf_def_id,
                        ReprKind::Scalar(
                            def_id,
                            ReprScalarKind::TextConstant(def_id),
                            data.rel_span,
                        ),
                        def_id,
                        data,
                    );
                }
                DefKind::NumberLiteral(lit) => {
                    self.merge_repr(
                        &mut builder,
                        leaf_def_id,
                        ReprKind::Scalar(
                            def_id,
                            if lit.contains('.') {
                                ReprScalarKind::F64(
                                    NotNan::new(f64::MIN).unwrap()..=NotNan::new(f64::MAX).unwrap(),
                                )
                            } else {
                                ReprScalarKind::I64(i64::MIN..=i64::MAX)
                            },
                            data.rel_span,
                        ),
                        def_id,
                        data,
                    );
                }
                DefKind::Type(type_def) => {
                    // Some `ontol` types are "domain types" but still scalars
                    if let Some(text_like) = self.defs.text_like_types.get(&def_id) {
                        let scalar_kind = match text_like {
                            TextLikeType::Ulid | TextLikeType::Uuid => {
                                ReprScalarKind::Octets(ReprFormat::TextLike)
                            }
                            TextLikeType::DateTime => ReprScalarKind::DateTime,
                        };

                        self.merge_repr(
                            &mut builder,
                            leaf_def_id,
                            ReprKind::Scalar(def_id, scalar_kind, data.rel_span),
                            def_id,
                            data,
                        )
                    } else if type_def.flags.contains(TypeDefFlags::CONCRETE) {
                        if let Some(properties) = self.prop_ctx.properties_by_def_id(def_id) {
                            let mut has_table = false;
                            if properties.table.is_some() {
                                if self.state.do_trace {
                                    trace!(
                                        "    table({def_id:?}): {:?} {data:?}",
                                        properties.table
                                    );
                                }

                                self.merge_repr(
                                    &mut builder,
                                    leaf_def_id,
                                    ReprKind::Struct,
                                    def_id,
                                    data,
                                );
                                has_table = true;
                            }

                            match &properties.constructor {
                                Constructor::Transparent => {
                                    // The type can be represented as a Unit
                                    // if there is an _empty type_ (a leaf type) somewhere in the mesh
                                    if !has_table && data.is_leaf {
                                        self.merge_repr(
                                            &mut builder,
                                            leaf_def_id,
                                            ReprKind::Unit,
                                            def_id,
                                            data,
                                        );
                                    }
                                }
                                Constructor::TextFmt(segment) => {
                                    assert!(!has_table);
                                    let mut attributes = Default::default();
                                    segment.collect_attributes(&mut attributes);
                                    let kind = if attributes.len() == 1 {
                                        ReprKind::FmtStruct(Some(
                                            attributes.into_iter().next().unwrap(),
                                        ))
                                    } else {
                                        ReprKind::FmtStruct(None)
                                    };

                                    self.merge_repr(&mut builder, leaf_def_id, kind, def_id, data);
                                }
                                Constructor::Sequence(_) => {
                                    assert!(!has_table);
                                    self.merge_repr(
                                        &mut builder,
                                        leaf_def_id,
                                        ReprKind::Seq,
                                        def_id,
                                        data,
                                    );
                                }
                            }
                        } else {
                            self.merge_repr(
                                &mut builder,
                                leaf_def_id,
                                ReprKind::Unit,
                                def_id,
                                data,
                            );
                        }
                    }
                }
                DefKind::Regex(_) => {
                    self.merge_repr(
                        &mut builder,
                        leaf_def_id,
                        ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                        def_id,
                        data,
                    );
                }
                DefKind::Extern(_) => {
                    self.merge_repr(&mut builder, leaf_def_id, ReprKind::Extern, def_id, data);
                }
                DefKind::Macro(_) => {
                    self.merge_repr(&mut builder, leaf_def_id, ReprKind::Macro, def_id, data);
                }
                _ => {}
            }

            if matches!(data.rel, IsRelation::Origin | IsRelation::Super) {
                if let Some(type_params) = self.misc_ctx.type_params.get(&def_id) {
                    for (relation_def_id, type_param) in type_params {
                        match builder.type_params.entry(*relation_def_id) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(type_param.clone());
                            }
                            Entry::Occupied(mut occupied) => {
                                let old = occupied.get();
                                if type_param.definition_site != self.root_def_id.domain_index() {
                                    // For now: Type parameters from the same package takes precedence
                                } else if old.definition_site == self.root_def_id.domain_index() {
                                    self.state
                                        .duplicate_type_params
                                        .entry(*relation_def_id)
                                        .or_insert_with(|| vec![old.span])
                                        .push(type_param.span);
                                } else {
                                    // Type params defined in the current package may override those in dependent packages
                                    occupied.insert(type_param.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        for (relation_def_id, spans) in std::mem::take(&mut self.state.duplicate_type_params) {
            CompileError::DuplicateTypeParam(
                self.defs
                    .def_kind(relation_def_id)
                    .opt_identifier()
                    .unwrap()
                    .into(),
            )
            .span(self.defs.def_span(self.root_def_id))
            .with_notes(
                spans
                    .into_iter()
                    .map(|span| SpannedNote::new(Note::DefinedHere, span)),
            )
            .report(self.errors);
        }

        self.check_soundness(builder, &mesh)
    }

    fn merge_repr(
        &mut self,
        builder: &mut ReprBuilder,
        repr_def_id: DefId,
        next: ReprKind,
        next_def_id: DefId,
        data: &IsData,
    ) {
        if self.state.do_trace {
            trace!(
                "    {:?} merge repr {:?}=>{:?} {next:?}",
                self.root_def_id, data.rel, next_def_id,
            );
        }

        use IsRelation::*;
        match (data.rel, &mut builder.kind, next) {
            (Origin, _, next) => {
                builder.kind = Some(next);
            }
            // Handle supertypes - results in intersections
            (Super, None, ReprKind::Unit) => {
                builder.kind = Some(ReprKind::Unit);
            }
            (Super, Some(_), ReprKind::Unit) => {
                // Unit does not add additional information to an existing repr
            }
            (Super, Some(ReprKind::Unit), next) => {
                builder.kind = Some(next);
            }
            (Super, None | Some(ReprKind::Struct), ReprKind::Struct) => {
                if let Some(table) = self.prop_ctx.properties_table_by_def_id(repr_def_id) {
                    if !table.is_empty() {
                        CompileError::TODO("macro must be used here")
                            .span(data.rel_span)
                            .report(self);
                        return;
                    }
                }

                builder.kind = Some(ReprKind::StructIntersection(
                    [(next_def_id, data.rel_span)].into(),
                ));
            }
            (Super, None, next) => {
                builder.kind = Some(next);
            }
            (Super, Some(ReprKind::StructIntersection(members)), ReprKind::Struct) => {
                members.push((next_def_id, data.rel_span));
            }
            (Super, Some(repr), kind) if *repr != kind => match repr {
                ReprKind::Scalar(def0, _, span0) => {
                    builder.kind = Some(ReprKind::Intersection(vec![
                        (*def0, *span0),
                        (next_def_id, data.rel_span),
                    ]));
                }
                ReprKind::Intersection(items) => {
                    builder.kind = Some(ReprKind::Intersection(items.to_vec()));
                }
                _ => {
                    todo!("{repr:?}");
                }
            },
            // Handle subtypes - results in unions
            (Sub, Some(ReprKind::Unit), ReprKind::Unit) => {
                if data.is_leaf {
                    builder.kind = Some(ReprKind::Union(
                        vec![(next_def_id, data.rel_span)],
                        UnionBound::Any,
                    ));
                }
            }
            (Sub, Some(ReprKind::Unit), _) => {
                builder.kind = Some(ReprKind::Union(
                    vec![(next_def_id, data.rel_span)],
                    UnionBound::Any,
                ));
            }
            (Sub, Some(ReprKind::Struct), ReprKind::Struct | ReprKind::Unit) => {
                builder.kind = Some(ReprKind::Union(
                    [(next_def_id, data.rel_span)].into(),
                    UnionBound::Struct,
                ));
            }
            (
                Sub,
                Some(ReprKind::Union(variants, UnionBound::Scalar(kind))),
                ReprKind::Scalar(_, new_kind, _),
            ) => {
                *kind = kind.combine(&new_kind);
                variants.push((next_def_id, data.rel_span));
            }
            (Sub, Some(ReprKind::Union(variants, UnionBound::Struct)), ReprKind::Struct) => {
                variants.push((next_def_id, data.rel_span));
            }
            (Sub, Some(ReprKind::Union(variants, UnionBound::Fmt)), ReprKind::FmtStruct(_)) => {
                variants.push((next_def_id, data.rel_span));
            }
            (Sub, Some(ReprKind::Union(_, UnionBound::Struct)), _) => {
                CompileError::TypeNotRepresentable
                    .span(self.defs.def_span(repr_def_id))
                    .with_note(
                        Note::CannotBePartOfStructUnion.span(self.defs.def_span(next_def_id)),
                    )
                    .report(self);
            }
            (Sub, Some(ReprKind::Union(variants, _)), ReprKind::Struct) => {
                let mut variants = std::mem::take(variants);
                variants.push((next_def_id, data.rel_span));
                builder.kind = Some(ReprKind::Union(variants, UnionBound::Struct));
            }
            (Sub, Some(ReprKind::Union(variants, _)), ReprKind::Unit) => {
                if data.is_leaf {
                    variants.push((next_def_id, data.rel_span));
                }
            }
            (Sub, None, ReprKind::Scalar(_, kind, _)) => {
                builder.kind = Some(ReprKind::Union(
                    vec![(next_def_id, data.rel_span)],
                    UnionBound::Scalar(kind.saturate()),
                ));
            }
            (Sub, None, ReprKind::Struct) => {
                builder.kind = Some(ReprKind::Union(
                    vec![(next_def_id, data.rel_span)],
                    UnionBound::Struct,
                ));
            }
            (Sub, None, ReprKind::FmtStruct(_)) => {
                builder.kind = Some(ReprKind::Union(
                    vec![(next_def_id, data.rel_span)],
                    UnionBound::Fmt,
                ));
            }
            (Sub, None, _) => {
                builder.kind = Some(ReprKind::Union(
                    vec![(next_def_id, data.rel_span)],
                    UnionBound::Any,
                ));
            }
            (
                Sub,
                Some(ReprKind::Scalar(scalar1, kind1, span1)),
                ReprKind::Scalar(scalar2, kind2, span2),
            ) => {
                builder.kind = Some(ReprKind::Union(
                    vec![(*scalar1, *span1), (scalar2, span2)],
                    UnionBound::Scalar(kind1.combine(&kind2)),
                ));
            }
            (
                Sub,
                Some(ReprKind::Union(variants, UnionBound::Any)),
                ReprKind::Scalar(def_id, _, span),
            ) => {
                variants.push((def_id, span));
            }
            (Super | Sub, Some(ReprKind::Seq), _) => {
                CompileError::InvalidMixOfRelationshipTypeForSubject
                    .span(data.rel_span)
                    .report(self);
            }
            (is_relation, old, new) => {
                panic!("Invalid repr transition: {old:?} =({is_relation:?})> {new:?}")
            }
        }

        if self.state.do_trace {
            trace!("    tmp repr: {:?}", builder.kind);
        }
    }

    fn collect_thesaurus(&mut self, def_id: DefId) -> IndexMap<DefId, IsData> {
        let mut output = IndexMap::default();

        let mut is_path = IsPath::default();

        self.traverse_thesaurus(
            def_id,
            IsRelation::Origin,
            0,
            NO_SPAN,
            &mut is_path,
            &mut output,
        );

        if !self.state.circular_spans.is_empty() {
            let spans = std::mem::take(&mut self.state.circular_spans);
            let initial_span = spans.into_iter().next().unwrap();

            CompileError::CircularSubtypingRelation
                .span(initial_span)
                .report(self.errors);
        }

        if !is_path.invalid_super_set.is_empty() {
            CompileError::AnonymousUnionAbstraction
                .span(self.defs.def_span(def_id))
                .with_notes(
                    is_path
                        .invalid_super_set
                        .values()
                        .map(|span| Note::UseDomainSpecificUnitType.span(*span)),
                )
                .report(self.errors)
        }

        output
    }

    fn traverse_thesaurus(
        &mut self,
        def_id: DefId,
        is_relation: IsRelation,
        level: u16,
        span: SourceSpan,
        is_path: &mut IsPath,
        output: &mut IndexMap<DefId, IsData>,
    ) {
        if let Some(data) = output.get_mut(&def_id) {
            data.level = u16::min(level, data.level);

            if data.rel != is_relation {
                self.state.circular_spans.push(span);
            }
        } else {
            output.insert(
                def_id,
                IsData {
                    rel: is_relation,
                    is_leaf: true,
                    rel_span: span,
                    level,
                },
            );

            let mut was_leaf = true;

            for (is, next_span) in self.thesaurus.entries(def_id, self.defs) {
                let next_relation = match (is_relation, is.rel) {
                    (_, TypeRelation::ImplicitSuper | TypeRelation::Subset) => {
                        continue;
                    }
                    (IsRelation::Origin | IsRelation::Super, TypeRelation::Super) => {
                        IsRelation::Super
                    }
                    (IsRelation::Sub, TypeRelation::Super) => {
                        // If traversing _down_ once, don't traverse _up_ again.
                        continue;
                    }
                    (_, TypeRelation::SubVariant) => {
                        if let Some(super_segment) = is_path
                            .segments
                            .iter()
                            .find(|segment| matches!(segment.is.rel, TypeRelation::Super))
                            .copied()
                        {
                            is_path
                                .invalid_super_set
                                .insert(super_segment.is.def_id, super_segment.span);
                        }

                        IsRelation::Sub
                    }
                };

                // Don't traverse built-in spans:
                let next_span = if next_span.source_id == SourceId(0) {
                    span
                } else {
                    *next_span
                };

                is_path.segments.push(IsPathSegment {
                    is: *is,
                    span: next_span,
                });

                self.traverse_thesaurus(
                    is.def_id,
                    next_relation,
                    level + 1,
                    next_span,
                    is_path,
                    output,
                );
                is_path.segments.pop();
                was_leaf = false;
            }

            output.get_mut(&def_id).unwrap().is_leaf = was_leaf;
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct IsData {
    pub rel: IsRelation,
    pub is_leaf: bool,
    pub rel_span: SourceSpan,
    pub level: u16,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(super) enum IsRelation {
    Origin,
    Super,
    Sub,
}

#[derive(Default)]
struct IsPath {
    segments: Vec<IsPathSegment>,
    invalid_super_set: HashMap<DefId, SourceSpan>,
}

#[derive(Clone, Copy)]
struct IsPathSegment {
    is: Is,
    span: SourceSpan,
}
