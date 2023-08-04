//! Checks whether types have a valid and _concrete_ representation.
//! It is not an error (in itself) for a type to be abstract,
//! but an abstract type does not form a complete domain in itself.
//!
//! So the responsibility of this code is just to record the facts,
//! and those facts are used in later compilation stages.

use std::collections::BTreeSet;

use fnv::FnvHashSet;
use ontol_runtime::DefId;
use tracing::{trace, warn};

use crate::{
    def::{Def, Defs, LookupRelationshipMeta, RelParams},
    error::CompileError,
    relation::{Constructor, Properties, Relations},
    types::{DefTypes, Type},
    CompileErrors, Compiler, Note, SourceSpan, SpannedCompileError, SpannedNote, NATIVE_SOURCE,
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
            trace!("def {def_id:?} repr: {repr:?}");
            self.repr.table.insert(def_id, repr);
            Ok(())
        } else {
            Err(())
        }
    }

    fn compute_repr(&mut self, leaf_def_id: DefId) -> Option<ReprKind> {
        let ontology_mesh = self.collect_ontology_mesh(leaf_def_id);

        let mut repr: Option<ReprKind> = None;

        for def_id in &ontology_mesh {
            match self.def_types.table.get(def_id) {
                Some(Type::Int(_) | Type::IntConstant(_)) => {
                    self.merge_repr(&mut repr, ReprKind::I64);
                }
                Some(Type::Bool(_)) => {
                    self.merge_repr(&mut repr, ReprKind::Bool);
                }
                Some(Type::String(_) | Type::StringConstant(_) | Type::StringLike(..)) => {
                    self.merge_repr(&mut repr, ReprKind::String)
                }
                Some(Type::Domain(_) | Type::Anonymous(_)) => {
                    if let Some(properties) = self.relations.properties_by_def_id(*def_id) {
                        if properties.table.is_some() {
                            self.merge_repr(&mut repr, ReprKind::Struct);
                        }

                        match &properties.constructor {
                            Constructor::StringFmt(_) => {
                                self.merge_repr(&mut repr, ReprKind::String);
                            }
                            Constructor::Struct => {
                                self.merge_repr(&mut repr, ReprKind::Struct);
                            }
                            Constructor::Sequence(_) => {
                                self.merge_repr(&mut repr, ReprKind::Seq);
                            }
                            _ => {}
                        }
                    } else {
                        self.merge_repr(&mut repr, ReprKind::Struct);
                    }
                }
                _ => {}
            }
        }

        repr
    }

    fn merge_repr(&mut self, repr: &mut Option<ReprKind>, kind: ReprKind) {
        match repr {
            None => *repr = Some(kind),
            Some(repr) => {
                if *repr != kind {
                    warn!("Conflicting repr: {repr:?} and {kind:?}");
                }
            }
        }
    }

    fn collect_ontology_mesh(&self, def_id: DefId) -> BTreeSet<DefId> {
        let mut output = BTreeSet::default();

        fn traverse(def_id: DefId, output: &mut BTreeSet<DefId>, check: &ReprCheck) {
            if output.contains(&def_id) {
                return;
            }

            output.insert(def_id);

            let mesh = match check.relations.ontology_mesh.get(&def_id) {
                Some(mesh) => mesh,
                None => return,
            };

            for is_def_id in mesh {
                traverse(*is_def_id, output, check);
            }
        }

        traverse(def_id, &mut output, self);

        output
    }
}
