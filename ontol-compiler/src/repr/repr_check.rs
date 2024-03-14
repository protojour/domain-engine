//! Checks whether types have a valid and _concrete_ representation.
//! It is not an error (in itself) for a type to be abstract,
//! but an abstract type does not form a complete domain in itself.
//!
//! So the responsibility of this code is just to record the facts,
//! and those facts are used in later compilation stages.

use std::collections::hash_map::Entry;

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{value::PropertyId, DefId};
use tracing::{debug_span, trace};

use crate::{
    def::{Def, DefKind, Defs, LookupRelationshipMeta, RelParams, TypeDefFlags},
    error::CompileError,
    package::ONTOL_PKG,
    primitive::{PrimitiveKind, Primitives},
    relation::{Constructor, Properties, Relations},
    thesaurus::Thesaurus,
    thesaurus::TypeRelation,
    type_check::seal::SealCtx,
    types::{DefTypes, Type},
    CompileErrors, Note, SourceId, SourceSpan, SpannedCompileError, SpannedNote, NATIVE_SOURCE,
    NO_SPAN,
};

use super::repr_model::{NumberResolution, Repr, ReprBuilder, ReprKind, ReprScalarKind};

/// If there are repr problems in the ONTOL domain, turn this on.
/// Otherwise, it's too verbose.
const TRACE_BUILTIN: bool = false;

pub struct ReprCheck<'c, 'm> {
    pub root_def_id: DefId,
    pub defs: &'c Defs<'m>,
    pub def_types: &'c DefTypes<'m>,
    pub relations: &'c Relations,
    pub thesaurus: &'c Thesaurus,
    pub seal_ctx: &'c mut SealCtx,
    pub primitives: &'c Primitives,

    #[allow(unused)]
    pub errors: &'c mut CompileErrors,

    pub state: State,
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

impl<'c, 'm> ReprCheck<'c, 'm> {
    /// Check the representation of a type
    pub fn check_repr_root(&mut self) {
        let Some(def) = self.defs.table.get(&self.root_def_id) else {
            // This can happen in case of errors
            return;
        };

        let _entered = debug_span!("repr", id = ?self.root_def_id).entered();

        self.state.do_trace = TRACE_BUILTIN || self.root_def_id.package_id() != ONTOL_PKG;

        self.check_def_repr(
            self.root_def_id,
            def,
            self.relations.properties_by_def_id(self.root_def_id),
        );

        if self.state.do_emit_diagnostics && !self.state.abstract_notes.is_empty() {
            self.errors.error_with_notes(
                CompileError::TypeNotRepresentable,
                &def.span,
                std::mem::take(&mut self.state.abstract_notes),
            );
        }
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
                            self.state.abstract_notes.push(SpannedNote {
                                note: Note::TypeIsAbstract,
                                span: span_node.span,
                            });
                        }
                    }
                    SpanKind::Field => {
                        self.state.abstract_notes.push(SpannedNote {
                            note: Note::FieldTypeIsAbstract,
                            span: span_node.span,
                        });
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
        if self.seal_ctx.repr_table.contains_key(&def_id) {
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
                for (property_id, _property) in table {
                    self.traverse_property(*property_id);
                }
            }

            if let Constructor::Sequence(seq) = &properties.constructor {
                for (_, relationship_id) in seq.elements() {
                    if let Some(relationship_id) = relationship_id {
                        self.traverse_property(PropertyId::subject(relationship_id));
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

            self.seal_ctx.repr_table.insert(def_id, repr);
            Ok(())
        } else {
            Err(())
        }
    }

    fn traverse_property(&mut self, property_id: PropertyId) {
        let meta = self.defs.relationship_meta(property_id.relationship_id);

        let (value_def_id, ..) = meta.relationship.by(property_id.role.opposite());
        let value_def = self.defs.table.get(&value_def_id).unwrap();

        if let Some(Type::Error) = self.def_types.table.get(&value_def_id) {
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
            self.relations.properties_by_def_id(value_def_id),
        );
        self.state.span_stack.pop();

        if let RelParams::Type(def_id) = &meta.relationship.rel_params {
            let rel_def = self.defs.table.get(def_id).unwrap();

            self.state.span_stack.push(SpanNode {
                span: rel_def.span,
                kind: SpanKind::Field,
            });
            self.check_def_repr(
                *def_id,
                value_def,
                self.relations.properties_by_def_id(*def_id),
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
                            ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                            def_id,
                            data,
                        );
                    } else {
                        match kind {
                            PrimitiveKind::Unit => {
                                self.merge_repr(&mut builder, ReprKind::Unit, def_id, data);
                            }
                            PrimitiveKind::Boolean
                            | PrimitiveKind::False
                            | PrimitiveKind::True
                            | PrimitiveKind::Text
                            | PrimitiveKind::Number
                            | PrimitiveKind::Serial => {
                                self.merge_repr(
                                    &mut builder,
                                    ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                                    def_id,
                                    data,
                                );
                            }
                            PrimitiveKind::Integer => {
                                self.merge_number_resolution(
                                    &mut builder,
                                    NumberResolution::Integer,
                                    data.rel_span,
                                );
                            }
                            PrimitiveKind::Float => {
                                self.merge_number_resolution(
                                    &mut builder,
                                    NumberResolution::Float,
                                    data.rel_span,
                                );
                            }
                            PrimitiveKind::F32 => {
                                self.merge_number_resolution(
                                    &mut builder,
                                    NumberResolution::F32,
                                    data.rel_span,
                                );
                            }
                            PrimitiveKind::F64 => {
                                self.merge_number_resolution(
                                    &mut builder,
                                    NumberResolution::F64,
                                    data.rel_span,
                                );
                            }
                            _ => {}
                        }
                    }
                }
                DefKind::TextLiteral(_) | DefKind::NumberLiteral(_) => {
                    self.merge_repr(
                        &mut builder,
                        ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                        def_id,
                        data,
                    );
                }
                DefKind::Type(type_def) => {
                    // Some `ontol` types are "domain types" but still scalars
                    if self.defs.string_like_types.get(&def_id).is_some() {
                        self.merge_repr(
                            &mut builder,
                            ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                            def_id,
                            data,
                        )
                    } else if type_def.flags.contains(TypeDefFlags::CONCRETE) {
                        if let Some(properties) = self.relations.properties_by_def_id(def_id) {
                            let mut has_table = false;
                            if properties.table.is_some() {
                                if self.state.do_trace {
                                    trace!(
                                        "    table({def_id:?}): {:?} {data:?}",
                                        properties.table
                                    );
                                }

                                self.merge_repr(&mut builder, ReprKind::Struct, def_id, data);
                                has_table = true;
                            }

                            match &properties.constructor {
                                Constructor::Transparent => {
                                    // The type can be represented as a Unit
                                    // if there is an _empty type_ (a leaf type) somewhere in the mesh
                                    if !has_table && data.is_leaf {
                                        self.merge_repr(&mut builder, ReprKind::Unit, def_id, data);
                                    }
                                }
                                Constructor::TextFmt(_) => {
                                    assert!(!has_table);
                                    self.merge_repr(
                                        &mut builder,
                                        ReprKind::Scalar(
                                            def_id,
                                            ReprScalarKind::Other,
                                            data.rel_span,
                                        ),
                                        def_id,
                                        data,
                                    );
                                }
                                Constructor::Sequence(_) => {
                                    assert!(!has_table);
                                    self.merge_repr(&mut builder, ReprKind::Seq, def_id, data);
                                }
                            }
                        } else {
                            self.merge_repr(&mut builder, ReprKind::Unit, def_id, data);
                        }
                    }
                }
                DefKind::Regex(_) => {
                    self.merge_repr(
                        &mut builder,
                        ReprKind::Scalar(def_id, ReprScalarKind::Other, data.rel_span),
                        def_id,
                        data,
                    );
                }
                DefKind::Extern(_) => {
                    self.merge_repr(&mut builder, ReprKind::Extern, def_id, data);
                }
                _ => {}
            }

            if matches!(data.rel, IsRelation::Origin | IsRelation::Super) {
                if let Some(type_params) = self.relations.type_params.get(&def_id) {
                    for (relation_def_id, type_param) in type_params {
                        match builder.type_params.entry(*relation_def_id) {
                            Entry::Vacant(vacant) => {
                                vacant.insert(type_param.clone());
                            }
                            Entry::Occupied(mut occupied) => {
                                let old = occupied.get();
                                if type_param.definition_site != self.root_def_id.package_id() {
                                    // For now: Type parameters from the same package takes precedence
                                } else if old.definition_site == self.root_def_id.package_id() {
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
            self.errors.error_with_notes(
                CompileError::DuplicateTypeParam(
                    self.defs
                        .def_kind(relation_def_id)
                        .opt_identifier()
                        .unwrap()
                        .into(),
                ),
                &self.defs.def_span(self.root_def_id),
                spans
                    .into_iter()
                    .map(|span| SpannedNote::new(Note::DefinedHere, span))
                    .collect(),
            );
        }

        self.check_soundness(builder, &mesh)
    }

    fn merge_repr(
        &mut self,
        builder: &mut ReprBuilder,
        next: ReprKind,
        def_id: DefId,
        data: &IsData,
    ) {
        if self.state.do_trace {
            trace!(
                "    {:?} merge repr {:?}=>{:?} {next:?}",
                self.root_def_id,
                data.rel,
                def_id,
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
                builder.kind = Some(ReprKind::StructIntersection(
                    [(def_id, data.rel_span)].into(),
                ));
            }
            (Super, None, next) => {
                builder.kind = Some(next);
            }
            (Super, Some(ReprKind::StructIntersection(members)), ReprKind::Struct) => {
                members.push((def_id, data.rel_span));
            }
            (Super, Some(repr), kind) if *repr != kind => match repr {
                ReprKind::Scalar(def0, _, span0) => {
                    builder.kind = Some(ReprKind::Intersection(vec![
                        (*def0, *span0),
                        (def_id, data.rel_span),
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
                    builder.kind = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
                }
            }
            (Sub, Some(ReprKind::Unit), _) => {
                builder.kind = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
            }
            (Sub, Some(ReprKind::Struct), ReprKind::Struct) => {
                builder.kind = Some(ReprKind::StructUnion([(def_id, data.rel_span)].into()));
            }
            (Sub, Some(ReprKind::StructUnion(variants)), ReprKind::Struct) => {
                variants.push((def_id, data.rel_span));
            }
            (Sub, Some(ReprKind::Union(variants)), ReprKind::Struct) => {
                let mut variants = std::mem::take(variants);
                variants.push((def_id, data.rel_span));
                builder.kind = Some(ReprKind::StructUnion(variants));
            }
            (Sub, Some(ReprKind::Union(variants)), ReprKind::Unit) => {
                if data.is_leaf {
                    variants.push((def_id, data.rel_span));
                }
            }
            (Sub, None, ReprKind::Struct) => {
                builder.kind = Some(ReprKind::StructUnion(vec![(def_id, data.rel_span)]));
            }
            (Sub, None, _) => {
                builder.kind = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
            }
            (
                Sub,
                Some(ReprKind::Scalar(scalar1, _, span1)),
                ReprKind::Scalar(scalar2, _, span2),
            ) => {
                builder.kind = Some(ReprKind::Union(vec![(*scalar1, *span1), (scalar2, span2)]));
            }
            (Sub, Some(ReprKind::Union(variants)), ReprKind::Scalar(def_id, _, span)) => {
                variants.push((def_id, span));
            }
            (Super | Sub, Some(ReprKind::Seq), _) => {
                self.errors.error(
                    CompileError::InvalidMixOfRelationshipTypeForSubject,
                    &data.rel_span,
                );
            }
            (is_relation, old, new) => {
                panic!("Invalid repr transition: {old:?} =({is_relation:?})> {new:?}")
            }
        }

        if self.state.do_trace {
            trace!("    tmp repr: {:?}", builder.kind);
        }
    }

    fn merge_number_resolution(
        &mut self,
        builder: &mut ReprBuilder,
        resolution: NumberResolution,
        span: SourceSpan,
    ) {
        builder.number_resolutions.entry(resolution).or_insert(span);
    }

    fn collect_thesaurus(&mut self, def_id: DefId) -> IndexMap<DefId, IsData> {
        let mut output = IndexMap::default();

        self.traverse_thesaurus(def_id, IsRelation::Origin, 0, NO_SPAN, &mut output);

        if !self.state.circular_spans.is_empty() {
            let spans = std::mem::take(&mut self.state.circular_spans);
            let initial_span = spans.into_iter().next().unwrap();

            self.errors.push(SpannedCompileError {
                error: CompileError::CircularSubtypingRelation,
                span: initial_span,
                notes: vec![],
            })
        }

        output
    }

    fn traverse_thesaurus(
        &mut self,
        def_id: DefId,
        is_relation: IsRelation,
        level: u16,
        span: SourceSpan,
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
                    _ => IsRelation::Sub,
                };

                // Don't traverse built-in spans:
                let next_span = if next_span.source_id == SourceId(0) {
                    span
                } else {
                    *next_span
                };

                self.traverse_thesaurus(is.def_id, next_relation, level + 1, next_span, output);
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
