use std::ops::Range;

use ontol_parser::cst::{
    inspect::{self as insp},
    view::NodeView,
};
use ontol_runtime::{
    property::{PropertyCardinality, ValueCardinality},
    DefId, RelationshipId,
};

use crate::{
    def::{DefKind, RelParams, Relationship, TypeDef, TypeDefFlags},
    CompileError,
};

use super::context::{BlockContext, CstLowering, MapVarTable, RelationKey, RootDefs};

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub(super) fn lower_rel_statement(
        &mut self,
        stmt: insp::RelStatement<V>,
        block: BlockContext,
    ) -> Option<RootDefs> {
        let rel_subject = stmt.subject()?;
        let fwd_set = stmt.fwd_set()?;
        let rel_object = stmt.object()?;

        let mut root_defs = RootDefs::default();

        let subject_def_id = self.resolve_type_reference(
            rel_subject.type_mod()?.type_ref()?,
            &block,
            Some(&mut root_defs),
        )?;
        let object_def_id = match rel_object.type_mod_or_pattern()? {
            insp::TypeModOrPattern::TypeMod(type_mod) => {
                self.resolve_type_reference(type_mod.type_ref()?, &block, Some(&mut root_defs))?
            }
            insp::TypeModOrPattern::Pattern(pattern) => {
                let mut var_table = MapVarTable::default();
                let lowered = self.lower_pattern(pattern.clone(), &mut var_table);
                let pat_id = self.ctx.compiler.patterns.alloc_pat_id();
                self.ctx.compiler.patterns.table.insert(pat_id, lowered);

                self.ctx
                    .define_anonymous(DefKind::Constant(pat_id), pattern.view().span())
            }
        };

        for (index, relation) in fwd_set.relations().enumerate() {
            if let Some(mut defs) = self.lower_relationship(
                subject_def_id,
                rel_subject.clone(),
                relation,
                object_def_id,
                rel_object.clone(),
                if index == 0 {
                    stmt.clone().backwd_set()
                } else {
                    None
                },
                stmt.clone(),
            ) {
                root_defs.append(&mut defs);
            }
        }

        Some(root_defs)
    }

    #[allow(clippy::too_many_arguments)]
    fn lower_relationship(
        &mut self,
        subject_def: DefId,
        rel_subject: insp::RelSubject<V>,
        relation: insp::Relation<V>,
        object_def: DefId,
        rel_object: insp::RelObject<V>,
        backward_relation: Option<insp::RelBackwdSet<V>>,
        rel_stmt: insp::RelStatement<V>,
    ) -> Option<RootDefs> {
        let mut root_defs = RootDefs::new();

        let (key, ident_span, index_range_rel_params): (_, _, Option<Range<Option<u16>>>) = {
            let type_mod = relation.relation_type()?;
            match type_mod.type_ref()? {
                insp::TypeRef::NumberRange(range) => (
                    RelationKey::Indexed,
                    range.0.span(),
                    Some(self.lower_u16_range(range)),
                ),
                type_ref => {
                    let def_id = self.resolve_type_reference(
                        type_ref,
                        &BlockContext::NoContext,
                        Some(&mut root_defs),
                    )?;
                    let span = type_mod.view().span();

                    match self.ctx.compiler.defs.def_kind(def_id) {
                        DefKind::TextLiteral(_) | DefKind::Type(_) => {
                            (RelationKey::Named(def_id), span, None)
                        }
                        DefKind::BuiltinRelType(..) => (RelationKey::Builtin(def_id), span, None),
                        DefKind::NumberLiteral(lit) => match lit.parse::<u16>() {
                            Ok(int) => (RelationKey::Indexed, span, Some(Some(int)..Some(int + 1))),
                            Err(_) => {
                                CompileError::NumberParse("not an integer".to_string())
                                    .span_report(span, &mut self.ctx);
                                return None;
                            }
                        },
                        _ => {
                            CompileError::InvalidRelationType.span_report(span, &mut self.ctx);
                            return None;
                        }
                    }
                }
            }
        };

        let has_object_prop = backward_relation.is_some();

        // This syntax just defines the relation the first time it's used
        let relation_def_id = self.ctx.define_relation_if_undefined(key, ident_span);

        let relationship_id = self.ctx.compiler.defs.alloc_def_id(self.ctx.package_id);
        self.append_documentation(relationship_id, rel_stmt.0.clone());

        let rel_params = if let Some(index_range_rel_params) = index_range_rel_params {
            if let Some(rp) = relation.rel_params() {
                CompileError::CannotMixIndexedRelationIdentsAndEdgeTypes
                    .span_report(rp.0.span(), &mut self.ctx);
                return None;
            }

            RelParams::IndexRange(index_range_rel_params)
        } else if let Some(rp) = relation.rel_params() {
            let rel_def_id = self.ctx.define_anonymous_type(
                TypeDef {
                    ident: None,
                    rel_type_for: Some(RelationshipId(relationship_id)),
                    flags: TypeDefFlags::CONCRETE,
                },
                rp.0.span(),
            );
            let context_fn = || rel_def_id;

            root_defs.push(rel_def_id);

            // This type needs to be part of the anonymous part of the namespace
            self.ctx
                .compiler
                .namespaces
                .add_anonymous(self.ctx.package_id, rel_def_id);

            for statement in rp.statements() {
                if let Some(mut defs) =
                    self.lower_statement(statement, BlockContext::Context(&context_fn))
                {
                    root_defs.append(&mut defs);
                }
            }

            RelParams::Type(rel_def_id)
        } else {
            RelParams::Unit
        };

        let mut relationship = {
            let object_prop = backward_relation
                .clone()
                .and_then(|rel| rel.name())
                .and_then(|name| name.text())
                .and_then(|result| self.ctx.unescape(result))
                .map(|prop| self.ctx.compiler.str_ctx.intern(&prop));

            let subject_cardinality = (
                property_cardinality(relation.prop_cardinality())
                    .unwrap_or(PropertyCardinality::Mandatory),
                value_cardinality(match rel_object.type_mod_or_pattern() {
                    Some(insp::TypeModOrPattern::TypeMod(type_mod)) => Some(type_mod),
                    _ => None,
                })
                .unwrap_or(ValueCardinality::Unit),
            );
            let object_cardinality = {
                let default = if has_object_prop {
                    // i.e. no syntax sugar: The object prop is explicit,
                    // therefore the object cardinality is explicit.
                    (PropertyCardinality::Mandatory, ValueCardinality::Unit)
                } else {
                    // The syntactic sugar case, which is the default behaviour:
                    // Many incoming edges to the same object:
                    (PropertyCardinality::Optional, ValueCardinality::IndexSet)
                };

                (
                    backward_relation
                        .and_then(|rel| property_cardinality(rel.prop_cardinality()))
                        .unwrap_or(default.0),
                    value_cardinality(rel_subject.type_mod()).unwrap_or(default.1),
                )
            };

            Relationship {
                relation_def_id,
                relation_span: self.ctx.source_span(ident_span),
                subject: (subject_def, self.ctx.source_span(rel_subject.0.span())),
                subject_cardinality,
                object: (object_def, self.ctx.source_span(rel_object.0.span())),
                object_cardinality,
                object_prop,
                rel_params,
            }
        };

        // HACK(for now): invert relationship
        if relation_def_id == self.ctx.compiler.primitives.relations.id {
            relationship = Relationship {
                relation_def_id: self.ctx.compiler.primitives.relations.identifies,
                relation_span: relationship.relation_span,
                subject: relationship.object,
                subject_cardinality: relationship.object_cardinality,
                object: relationship.subject,
                object_cardinality: relationship.subject_cardinality,
                object_prop: None,
                rel_params: relationship.rel_params,
            };
        }

        self.ctx.set_def_kind(
            relationship_id,
            DefKind::Relationship(relationship),
            rel_stmt.0.span(),
        );
        root_defs.push(relationship_id);

        Some(root_defs)
    }
}

fn value_cardinality<V: NodeView>(type_mod: Option<insp::TypeMod<V>>) -> Option<ValueCardinality> {
    Some(match type_mod? {
        insp::TypeMod::TypeModUnit(_) => ValueCardinality::Unit,
        insp::TypeMod::TypeModSet(_) => ValueCardinality::IndexSet,
        insp::TypeMod::TypeModList(_) => ValueCardinality::List,
    })
}

fn property_cardinality<V: NodeView>(
    prop_cardinality: Option<insp::PropCardinality<V>>,
) -> Option<PropertyCardinality> {
    Some(if prop_cardinality?.question().is_some() {
        PropertyCardinality::Optional
    } else {
        PropertyCardinality::Mandatory
    })
}
