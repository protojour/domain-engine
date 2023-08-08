//! Checks whether types have a valid and _concrete_ representation.
//! It is not an error (in itself) for a type to be abstract,
//! but an abstract type does not form a complete domain in itself.
//!
//! So the responsibility of this code is just to record the facts,
//! and those facts are used in later compilation stages.

use fnv::FnvHashSet;
use indexmap::IndexMap;
use ontol_runtime::{ontology::PropertyCardinality, smart_format, DefId};
use tracing::trace;

use crate::{
    def::{Def, Defs, LookupRelationshipMeta, RelParams},
    error::CompileError,
    package::ONTOL_PKG,
    relation::{Constructor, Properties, Relations},
    type_check::seal::SealedDefs,
    types::{DefTypes, Type},
    CompileErrors, Note, SourceSpan, SpannedCompileError, SpannedNote, NATIVE_SOURCE, NO_SPAN,
};

use super::repr_model::ReprKind;

pub struct ReprCheck<'c, 'm> {
    pub root_def_id: DefId,
    pub is_entity_root: bool,
    pub defs: &'c Defs<'m>,
    pub def_types: &'c DefTypes<'m>,
    pub relations: &'c Relations,
    pub sealed_defs: &'c mut SealedDefs,

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
        let def = self.defs.table.get(&self.root_def_id).unwrap();

        self.is_entity_root = self
            .relations
            .properties_by_def_id(self.root_def_id)
            .map(|properties| properties.identified_by.is_some())
            .unwrap_or(false);

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
        if self.sealed_defs.repr_table.contains_key(&def_id) {
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
            trace!("def {def_id:?} result repr: {repr:?}");
            self.sealed_defs.repr_table.insert(def_id, repr);
            Ok(())
        } else {
            Err(())
        }
    }

    fn compute_repr(&mut self, leaf_def_id: DefId) -> Option<ReprKind> {
        let ontology_mesh = self.collect_ontology_mesh(leaf_def_id);

        if leaf_def_id.package_id() != ONTOL_PKG {
            trace!("    mesh for {leaf_def_id:?}: {:?}", ontology_mesh.keys());
        }

        let mut rec = ReprRecord { repr: None };

        for (def_id, data) in &ontology_mesh {
            let def_id = *def_id;

            if leaf_def_id.package_id() != ONTOL_PKG {
                trace!(
                    "    next repr {leaf_def_id:?}=>{def_id:?} prev={:?}",
                    rec.repr
                );
            }

            match self.def_types.table.get(&def_id) {
                Some(Type::Int(_) | Type::IntConstant(_)) => {
                    self.merge_repr(&mut rec, ReprKind::I64, def_id, data);
                }
                Some(Type::Bool(_)) => {
                    self.merge_repr(&mut rec, ReprKind::Bool, def_id, data);
                }
                Some(Type::String(_) | Type::StringConstant(_)) => {
                    self.merge_repr(&mut rec, ReprKind::String, def_id, data)
                }
                Some(Type::StringLike(ontol_def_id, string_like)) => self.merge_repr(
                    &mut rec,
                    ReprKind::StringLike(*ontol_def_id, string_like.clone()),
                    def_id,
                    data,
                ),
                Some(Type::Domain(_) | Type::Anonymous(_)) => {
                    if let Some(properties) = self.relations.properties_by_def_id(def_id) {
                        let mut has_table = false;
                        if properties.table.is_some() {
                            trace!("    table: {:?} {data:?}", properties.table);
                            self.merge_repr(
                                &mut rec,
                                ReprKind::StructIntersection([(def_id, data.rel_span)].into()),
                                def_id,
                                data,
                            );
                            has_table = true;
                        }

                        match &properties.constructor {
                            Constructor::StringFmt(_) => {
                                assert!(!has_table);
                                self.merge_repr(&mut rec, ReprKind::String, def_id, data);
                            }
                            Constructor::Struct => {
                                if !has_table {
                                    self.merge_repr(
                                        &mut rec,
                                        ReprKind::StructIntersection(Default::default()),
                                        def_id,
                                        data,
                                    );
                                }
                            }
                            Constructor::Sequence(_) => {
                                assert!(!has_table);
                                self.merge_repr(&mut rec, ReprKind::Seq, def_id, data);
                            }
                            _ => {}
                        }
                    } else {
                        self.merge_repr(&mut rec, ReprKind::EmptyDict, def_id, data);
                    }
                }
                _ => {}
            }
        }

        rec.repr
    }

    fn merge_repr(&mut self, rec: &mut ReprRecord, next: ReprKind, def_id: DefId, data: &IsData) {
        trace!("    merge repr {next:?}");

        match (&mut rec.repr, data.is_relation, next) {
            (
                None,
                IsRelation::Origin | IsRelation::Mandatory,
                ReprKind::StructIntersection(members),
            ) => {
                rec.repr = Some(ReprKind::StructIntersection(members));
            }
            (
                Some(ReprKind::StructIntersection(members)),
                IsRelation::Origin | IsRelation::Mandatory,
                ReprKind::StructIntersection(new_members),
            ) => {
                members.extend(new_members);
            }
            (None, IsRelation::Origin | IsRelation::Mandatory, kind) => {
                rec.repr = Some(kind);
            }
            (Some(repr), IsRelation::Mandatory, kind) if *repr != kind => {
                rec.repr = Some(ReprKind::Intersection(vec![(def_id, data.rel_span)]));
            }
            (Some(_), IsRelation::Mandatory, _) => {}
            (Some(repr), IsRelation::Origin, _) => {
                unreachable!("in origin relation, repr was {repr:?}");
            }
            (None, IsRelation::Optional, _) => {
                rec.repr = Some(ReprKind::Union(vec![(def_id, data.rel_span)]));
            }
            (Some(_), IsRelation::Optional, _) => {
                rec.repr = Some(ReprKind::Union(vec![]));
            }
        }
    }

    fn collect_ontology_mesh(&mut self, def_id: DefId) -> IndexMap<DefId, IsData> {
        let mut output = IndexMap::default();

        self.traverse_ontology_mesh(def_id, IsRelation::Origin, NO_SPAN, &mut output);

        output
    }

    fn traverse_ontology_mesh(
        &mut self,
        def_id: DefId,
        is_relation: IsRelation,
        span: SourceSpan,
        output: &mut IndexMap<DefId, IsData>,
    ) {
        if let Some(data) = output.get(&def_id) {
            if data.is_relation != is_relation {
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
                    is_relation,
                    rel_span: span,
                },
            );

            if let Some(entries) = self.relations.ontology_mesh.get(&def_id) {
                for (is, span) in entries {
                    let next_relation = match (is_relation, is.cardinality) {
                        (
                            IsRelation::Origin | IsRelation::Mandatory,
                            PropertyCardinality::Mandatory,
                        ) => IsRelation::Mandatory,
                        _ => IsRelation::Optional,
                    };

                    self.traverse_ontology_mesh(is.def_id, next_relation, *span, output);
                }
            }
        }
    }
}

#[derive(Debug)]
struct IsData {
    is_relation: IsRelation,
    rel_span: SourceSpan,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum IsRelation {
    Origin,
    Mandatory,
    Optional,
}

struct ReprRecord {
    repr: Option<ReprKind>,
}
