//! Checks whether types have a valid and _concrete_ representation.
//! It is not an error (in itself) for a type to be abstract,
//! but an abstract type does not form a complete domain in itself.
//!
//! So the responsibility of this code is just to record the facts,
//! and those facts are used in later compilation stages.

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{smart_format, DefId};
use tracing::trace;

use crate::{
    def::{Def, DefKind, Defs, LookupRelationshipMeta, RelParams},
    error::CompileError,
    package::ONTOL_PKG,
    primitive::Primitives,
    relation::{Constructor, Properties, Relations, TypeRelation},
    type_check::seal::SealCtx,
    types::DefTypes,
    CompileErrors, Note, SourceSpan, SpannedCompileError, SpannedNote, NATIVE_SOURCE, NO_SPAN,
};

use super::repr_model::ReprKind;

pub struct ReprCheck<'c, 'm> {
    pub root_def_id: DefId,
    pub is_entity_root: bool,
    pub defs: &'c Defs<'m>,
    pub def_types: &'c DefTypes<'m>,
    pub relations: &'c Relations,
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
        let def = match self.defs.table.get(&self.root_def_id) {
            Some(def) => def,
            None => {
                // This can happen in case of errors
                return;
            }
        };

        self.is_entity_root = self
            .relations
            .properties_by_def_id(self.root_def_id)
            .map(|properties| properties.identified_by.is_some())
            .unwrap_or(false);
        self.state.do_trace = self.root_def_id.package_id() != ONTOL_PKG;

        self.check_def_repr(
            self.root_def_id,
            def,
            self.relations.properties_by_def_id(self.root_def_id),
        );

        let abstract_notes = std::mem::take(&mut self.state.abstract_notes);
        if !abstract_notes.is_empty() {
            self.errors.push(SpannedCompileError {
                error: CompileError::EntityNotRepresentable,
                span: def.span,
                notes: abstract_notes,
            });
        }
    }

    fn check_def_repr(&mut self, def_id: DefId, def: &Def, properties: Option<&Properties>) {
        if self.state.visited.contains(&def_id) {
            return;
        }

        self.state.visited.insert(def_id);
        self.state.span_stack.push(SpanNode {
            span: def.span,
            kind: SpanKind::Type(def_id),
        });

        let repr_result = self.compute_repr_cached(def_id, properties);

        if repr_result.is_err() && self.is_entity_root {
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

    fn compute_repr_cached(
        &mut self,
        def_id: DefId,
        properties: Option<&Properties>,
    ) -> Result<(), ()> {
        if self.seal_ctx.repr_table.contains_key(&def_id) {
            return Ok(());
        }

        let repr = self.compute_repr(def_id);

        // traverse fields
        if let Some(table) = &properties.and_then(|properties| properties.table.as_ref()) {
            for (property_id, _property) in *table {
                let meta = self
                    .defs
                    .lookup_relationship_meta(property_id.relationship_id)
                    .expect("Problem getting relationship meta");
                let object_def_id = meta.relationship.object.0.def_id;
                let object_def = self.defs.table.get(&object_def_id).unwrap();

                self.state.span_stack.push(SpanNode {
                    span: meta.relationship.object.1,
                    kind: SpanKind::Field,
                });
                self.check_def_repr(
                    object_def_id,
                    object_def,
                    self.relations.properties_by_def_id(object_def_id),
                );
                self.state.span_stack.pop();

                if let RelParams::Type(def_ref) = &meta.relationship.rel_params {
                    let rel_def = self.defs.table.get(&def_ref.def_id).unwrap();

                    self.state.span_stack.push(SpanNode {
                        span: rel_def.span,
                        kind: SpanKind::Field,
                    });
                    self.check_def_repr(
                        def_ref.def_id,
                        object_def,
                        self.relations.properties_by_def_id(def_ref.def_id),
                    );
                    self.state.span_stack.pop();
                }
            }
        }

        if let Some(repr) = repr {
            if self.state.do_trace {
                trace!(" => {def_id:?} result repr: {repr:?}");
            }

            self.seal_ctx.repr_table.insert(def_id, repr);
            Ok(())
        } else {
            Err(())
        }
    }

    fn compute_repr(&mut self, leaf_def_id: DefId) -> Option<ReprKind> {
        let ontology_mesh = self.collect_ontology_mesh(leaf_def_id);

        if ontology_mesh.len() > 1 && self.state.do_trace {
            trace!("    mesh for {leaf_def_id:?}: {:?}", ontology_mesh);
        }

        let mut rec = ReprRecord { repr: None };

        for (def_id, data) in &ontology_mesh {
            let def_id = *def_id;

            match self.defs.get_def_kind(def_id).unwrap() {
                DefKind::Primitive(kind) => {
                    if kind.is_concrete() {
                        self.merge_repr(
                            &mut rec,
                            ReprKind::Scalar(def_id, data.rel_span),
                            def_id,
                            data,
                        );
                    }
                }
                DefKind::StringLiteral(_) | DefKind::NumberLiteral(_) => {
                    self.merge_repr(
                        &mut rec,
                        ReprKind::Scalar(def_id, data.rel_span),
                        def_id,
                        data,
                    );
                }
                DefKind::Type(type_def) => {
                    // Some `ontol` types are "domain types" but still scalars
                    if self.defs.string_like_types.get(&def_id).is_some() {
                        self.merge_repr(
                            &mut rec,
                            ReprKind::Scalar(def_id, data.rel_span),
                            def_id,
                            data,
                        )
                    } else if type_def.concrete {
                        if let Some(properties) = self.relations.properties_by_def_id(def_id) {
                            let mut has_table = false;
                            if properties.table.is_some() {
                                if self.state.do_trace {
                                    trace!(
                                        "    table({def_id:?}): {:?} {data:?}",
                                        properties.table
                                    );
                                }

                                self.merge_repr(&mut rec, ReprKind::Struct, def_id, data);
                                has_table = true;
                            }

                            match &properties.constructor {
                                Constructor::Transparent => {
                                    // The type can be represented as a Unit
                                    // if there is an _empty type_ (a leaf type) somewhere in the mesh
                                    if !has_table && data.is_leaf {
                                        self.merge_repr(&mut rec, ReprKind::Unit, def_id, data);
                                    }
                                }
                                Constructor::StringFmt(_) => {
                                    assert!(!has_table);
                                    self.merge_repr(
                                        &mut rec,
                                        ReprKind::Scalar(def_id, data.rel_span),
                                        def_id,
                                        data,
                                    );
                                }
                                Constructor::Sequence(_) => {
                                    assert!(!has_table);
                                    self.merge_repr(&mut rec, ReprKind::Seq, def_id, data);
                                }
                            }
                        } else {
                            self.merge_repr(&mut rec, ReprKind::Struct, def_id, data);
                        }
                    }
                }
                _ => {}
            }
        }

        if let Some(repr) = &mut rec.repr {
            self.check_soundness(repr, &ontology_mesh);
        }

        rec.repr
    }

    fn merge_repr(&mut self, rec: &mut ReprRecord, next: ReprKind, def_id: DefId, data: &IsData) {
        if self.state.do_trace {
            trace!(
                "{:?} merge repr {:?}=>{:?} {next:?}",
                self.root_def_id,
                data.rel,
                def_id,
            );
        }

        use IsRelation::*;
        match (data.rel, &mut rec.repr, next) {
            (Origin, _, next) => {
                rec.repr = Some(next);
            }
            // Handle supertypes - results in intersections
            (Super, None, ReprKind::Unit) => {
                rec.repr = Some(ReprKind::Unit);
            }
            (Super, Some(_), ReprKind::Unit) => {
                // Unit does not add additional information to an existing repr
            }
            (Super, Some(ReprKind::Unit), next) => {
                rec.repr = Some(next);
            }
            (Super, None | Some(ReprKind::Struct), ReprKind::Struct) => {
                rec.repr = Some(ReprKind::StructIntersection(
                    [(def_id, data.rel_span)].into(),
                ));
            }
            (Super, None, next) => {
                rec.repr = Some(next);
            }
            (Super, Some(ReprKind::StructIntersection(members)), ReprKind::Struct) => {
                members.push((def_id, data.rel_span));
            }
            (Super, Some(repr), kind) if *repr != kind => match repr {
                ReprKind::Scalar(def0, span0) => {
                    rec.repr = Some(ReprKind::Intersection(vec![
                        (*def0, *span0),
                        (def_id, data.rel_span),
                    ]));
                }
                _ => {
                    todo!("{repr:?}");
                }
            },
            // Handle subtypes - results in unions
            (Sub, Some(ReprKind::Unit), ReprKind::Unit) => {
                if data.is_leaf {
                    rec.repr = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
                }
            }
            (Sub, Some(ReprKind::Unit), _) => {
                rec.repr = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
            }
            (Sub, Some(ReprKind::Struct), ReprKind::Struct) => {
                rec.repr = Some(ReprKind::StructUnion([(def_id, data.rel_span)].into()));
            }
            (Sub, Some(ReprKind::StructUnion(variants)), ReprKind::Struct) => {
                variants.push((def_id, data.rel_span));
            }
            (Sub, Some(ReprKind::Union(variants)), ReprKind::Struct) => {
                let mut variants = std::mem::take(variants);
                variants.push((def_id, data.rel_span));
                rec.repr = Some(ReprKind::StructUnion(variants));
            }
            (Sub, Some(ReprKind::Union(variants)), ReprKind::Unit) => {
                if data.is_leaf {
                    variants.push((def_id, data.rel_span));
                }
            }
            (Sub, None, ReprKind::Struct) => {
                rec.repr = Some(ReprKind::StructUnion(vec![(def_id, data.rel_span)]));
            }
            (Sub, None, _) => {
                rec.repr = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
            }
            (Sub, Some(ReprKind::Scalar(scalar1, span1)), ReprKind::Scalar(scalar2, span2)) => {
                rec.repr = Some(ReprKind::Union(vec![(*scalar1, *span1), (scalar2, span2)]));
            }
            (Sub, Some(ReprKind::Union(variants)), ReprKind::Scalar(def_id, span)) => {
                variants.push((def_id, span));
            }
            (Super | Sub, Some(ReprKind::Seq), _) => {
                self.errors.push(SpannedCompileError {
                    error: CompileError::InvalidMixOfRelationshipTypeForSubject,
                    span: data.rel_span,
                    notes: vec![],
                });
            }
            (is_relation, old, new) => {
                panic!("Invalid repr transition: {old:?} =({is_relation:?})> {new:?}")
            }
        }

        if self.state.do_trace {
            trace!("    tmp repr: {:?}", rec.repr);
        }
    }

    fn collect_ontology_mesh(&mut self, def_id: DefId) -> IndexMap<DefId, IsData> {
        let mut output = IndexMap::default();

        self.traverse_ontology_mesh(def_id, IsRelation::Origin, 0, NO_SPAN, &mut output);

        output
    }

    fn traverse_ontology_mesh(
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
                self.errors.push(SpannedCompileError {
                    error: CompileError::TODO(smart_format!(
                        "Conflicting optionality for is relation"
                    )),
                    span,
                    notes: vec![],
                });
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

            if let Some(entries) = self.relations.ontology_mesh.get(&def_id) {
                for (is, span) in entries {
                    if is.is_ontol_alias {
                        continue;
                    }

                    let next_relation = match (is_relation, is.rel) {
                        (IsRelation::Origin | IsRelation::Super, TypeRelation::Super) => {
                            IsRelation::Super
                        }
                        _ => IsRelation::Sub,
                    };

                    self.traverse_ontology_mesh(is.def_id, next_relation, level + 1, *span, output);
                    was_leaf = false;
                }
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

pub(super) struct ReprRecord {
    repr: Option<ReprKind>,
}
