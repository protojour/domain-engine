//! Checks whether types have a valid and _concrete_ representation.
//! It is not an error (in itself) for a type to be abstract,
//! but an abstract type does not form a complete domain in itself.
//!
//! So the responsibility of this code is just to record the facts,
//! and those facts are used in later compilation stages.

use std::collections::BTreeSet;

use fnv::FnvHashSet;
use ontol_runtime::DefId;

use crate::{
    def::{Def, Defs, LookupRelationshipMeta},
    error::CompileError,
    relation::{Constructor, Properties, Relations},
    types::{DefTypes, Type},
    Compiler, Note, SourceSpan, SpannedCompileError, SpannedNote, NATIVE_SOURCE,
};

impl<'m> Compiler<'m> {
    pub fn repr_check(&mut self) {
        for (def_id, def) in &self.defs.table {
            if let Some(properties) = self.relations.properties_by_def_id(*def_id) {
                // The roots for repr check is the entities:
                if properties.identified_by.is_none() {
                    continue;
                }

                let mut repr_check = ReprCheck::new(*def_id, self);
                repr_check.check_entity_repr(*def_id, def, properties);

                if !repr_check.abstract_notes.is_empty() {
                    self.errors.push(SpannedCompileError {
                        error: CompileError::EntityNotRepresentable,
                        span: def.span,
                        notes: repr_check.abstract_notes,
                    });
                }
            }
        }
    }
}

struct ReprCheck<'c, 'm> {
    entity_def_id: DefId,
    defs: &'c Defs<'m>,
    def_types: &'c DefTypes<'m>,
    relations: &'c Relations,

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
    fn new(entity_def_id: DefId, compiler: &'c Compiler<'m>) -> Self {
        Self {
            entity_def_id,
            defs: &compiler.defs,
            def_types: &compiler.def_types,
            relations: &compiler.relations,
            visited: Default::default(),
            span_stack: vec![],
            abstract_notes: vec![],
        }
    }

    fn check_entity_repr(&mut self, def_id: DefId, def: &Def, properties: &Properties) {
        self.check_type_repr(def_id, def, properties);
    }

    fn check_type_repr(&mut self, def_id: DefId, def: &Def, properties: &Properties) {
        if self.visited.contains(&def_id) {
            return;
        }

        self.visited.insert(def_id);
        self.span_stack.push(SpanNode {
            span: def.span,
            kind: SpanKind::Type(def_id),
        });

        let ontology_mesh = self.collect_ontology_mesh(def_id);
        let mut has_repr = false;

        for mesh_def_id in &ontology_mesh {
            match self.def_types.table.get(mesh_def_id).unwrap() {
                Type::Int(_)
                | Type::Bool(_)
                | Type::String(_)
                | Type::StringConstant(_)
                | Type::StringLike(..)
                | Type::IntConstant(_) => has_repr = true,
                Type::Domain(_) | Type::Anonymous(_) => {
                    if let Some(mesh_properties) = self.relations.properties_by_def_id(*mesh_def_id)
                    {
                        if mesh_properties.table.is_some() {
                            has_repr = true;
                        }

                        match &mesh_properties.constructor {
                            Constructor::StringFmt(_) => {
                                has_repr = true;
                            }
                            Constructor::Struct => {
                                has_repr = true;
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        if let Some(table) = &properties.table {
            for (property_id, _property) in table {
                let meta = self
                    .defs
                    .lookup_relationship_meta(property_id.relationship_id)
                    .expect("Problem getting relationship meta");
                let object_def_id = meta.relationship.object.0.def_id;
                let object_def = self.defs.table.get(&object_def_id).unwrap();
                if let Some(object_properties) = self.relations.properties_by_def_id(object_def_id)
                {
                    self.span_stack.push(SpanNode {
                        span: meta.relationship.object.1,
                        kind: SpanKind::Field,
                    });
                    self.check_type_repr(object_def_id, object_def, object_properties);
                    self.span_stack.pop();
                }
            }
        }

        if !has_repr {
            for span_node in self.span_stack.iter().rev() {
                if span_node.span.source_id == NATIVE_SOURCE {
                    continue;
                }

                match span_node.kind {
                    SpanKind::Type(span_def_id) => {
                        if span_def_id != self.entity_def_id {
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
