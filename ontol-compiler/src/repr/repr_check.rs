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
    package::CORE_PKG,
    relation::{Constructor, Properties, Relations},
    types::{DefTypes, Type},
    CompileErrors, Compiler, Note, SourceSpan, SpannedCompileError, SpannedNote, NATIVE_SOURCE,
    NO_SPAN,
};

use super::repr_model::{ReprCtx, ReprKind};

impl<'m> Compiler<'m> {
    pub fn repr_check(&mut self) {
        for (def_id, def) in &self.defs.table {
            let properties = self.relations.properties_by_def_id(*def_id);

            let mut repr_check = ReprCheck {
                root_def_id: *def_id,
                is_entity_root: properties
                    .map(|properties| properties.identified_by.is_some())
                    .unwrap_or(false),
                defs: &self.defs,
                def_types: &self.def_types,
                relations: &self.relations,
                repr: &mut self.repr,
                errors: &mut self.errors,
                visited: Default::default(),
                span_stack: vec![],
                abstract_notes: vec![],
            };

            repr_check.check_def_repr(*def_id, def, properties);

            let abstract_notes = std::mem::take(&mut repr_check.abstract_notes);
            if !abstract_notes.is_empty() {
                self.errors.push(SpannedCompileError {
                    error: CompileError::EntityNotRepresentable,
                    span: def.span,
                    notes: abstract_notes,
                });
            }
        }
    }
}

struct ReprCheck<'c, 'm> {
    root_def_id: DefId,
    is_entity_root: bool,
    defs: &'c Defs<'m>,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,
    repr: &'c mut ReprCtx,

    #[allow(unused)]
    errors: &'c mut CompileErrors,

    /// The repr check is coinductive,
    /// i.e. cycles mean that the repr is OK.
    /// Cycles appear in recursive types (tree structures, for example).
    visited: FnvHashSet<DefId>,

    span_stack: Vec<SpanNode>,

    /// If this is non-empty, the type is not representable
    abstract_notes: Vec<SpannedNote>,
}

struct SpanNode {
    span: SourceSpan,
    kind: SpanKind,
}

enum SpanKind {
    Type(DefId),
    Field,
}

impl<'c, 'm> ReprCheck<'c, 'm> {
    fn check_def_repr(&mut self, def_id: DefId, def: &Def, properties: Option<&Properties>) {
        if self.visited.contains(&def_id) {
            return;
        }

        self.visited.insert(def_id);
        self.span_stack.push(SpanNode {
            span: def.span,
            kind: SpanKind::Type(def_id),
        });

        let repr_result = self.compute_repr_cached(def_id, properties);

        if repr_result.is_err() && self.is_entity_root {
            for span_node in self.span_stack.iter().rev() {
                if span_node.span.source_id == NATIVE_SOURCE {
                    continue;
                }

                match span_node.kind {
                    SpanKind::Type(span_def_id) => {
                        if span_def_id != self.root_def_id {
                            self.abstract_notes.push(SpannedNote {
                                note: Note::TypeIsAbstract,
                                span: span_node.span,
                            });
                        }
                    }
                    SpanKind::Field => {
                        self.abstract_notes.push(SpannedNote {
                            note: Note::FieldTypeIsAbstract,
                            span: span_node.span,
                        });
                    }
                }
            }
        }

        self.span_stack.pop();
    }

    fn compute_repr_cached(
        &mut self,
        def_id: DefId,
        properties: Option<&Properties>,
    ) -> Result<(), ()> {
        if self.repr.table.contains_key(&def_id) {
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

                self.span_stack.push(SpanNode {
                    span: meta.relationship.object.1,
                    kind: SpanKind::Field,
                });
                self.check_def_repr(
                    object_def_id,
                    object_def,
                    self.relations.properties_by_def_id(object_def_id),
                );
                self.span_stack.pop();

                if let RelParams::Type(def_ref) = &meta.relationship.rel_params {
                    let rel_def = self.defs.table.get(&def_ref.def_id).unwrap();

                    self.span_stack.push(SpanNode {
                        span: rel_def.span,
                        kind: SpanKind::Field,
                    });
                    self.check_def_repr(
                        def_ref.def_id,
                        object_def,
                        self.relations.properties_by_def_id(def_ref.def_id),
                    );
                    self.span_stack.pop();
                }
            }
        }

        if let Some(repr) = repr {
            trace!("def {def_id:?} result repr: {repr:?}");
            self.repr.table.insert(def_id, repr);
            Ok(())
        } else {
            Err(())
        }
    }

    fn compute_repr(&mut self, leaf_def_id: DefId) -> Option<ReprKind> {
        let ontology_mesh = self.collect_ontology_mesh(leaf_def_id);

        if leaf_def_id.package_id() != CORE_PKG {
            trace!("mesh for {leaf_def_id:?}: {:?}", ontology_mesh.keys());
        }

        let mut rec = ReprRecord { repr: None };

        for (def_id, data) in &ontology_mesh {
            let def_id = *def_id;

            if leaf_def_id.package_id() != CORE_PKG {
                trace!("next repr {leaf_def_id:?}=>{def_id:?} prev={:?}", rec.repr);
            }

            match self.def_types.table.get(&def_id) {
                Some(Type::Int(_) | Type::IntConstant(_)) => {
                    self.merge_repr(&mut rec, ReprKind::I64, def_id, data);
                }
                Some(Type::Bool(_)) => {
                    self.merge_repr(&mut rec, ReprKind::Bool, def_id, data);
                }
                Some(Type::String(_) | Type::StringConstant(_) | Type::StringLike(..)) => {
                    self.merge_repr(&mut rec, ReprKind::String, def_id, data)
                }
                Some(Type::Domain(_) | Type::Anonymous(_)) => {
                    if let Some(properties) = self.relations.properties_by_def_id(def_id) {
                        let mut has_table = false;
                        if properties.table.is_some() {
                            trace!("table: {:?} {data:?}", properties.table);
                            self.merge_repr(&mut rec, ReprKind::Struct, def_id, data);
                            has_table = true;
                        }

                        match &properties.constructor {
                            Constructor::StringFmt(_) => {
                                assert!(!has_table);
                                self.merge_repr(&mut rec, ReprKind::String, def_id, data);
                            }
                            Constructor::Struct => {
                                if !has_table {
                                    self.merge_repr(&mut rec, ReprKind::EmptyDict, def_id, data);
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

    fn merge_repr(&mut self, rec: &mut ReprRecord, kind: ReprKind, def_id: DefId, data: &IsData) {
        match (&mut rec.repr, data.is_relation, kind) {
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
