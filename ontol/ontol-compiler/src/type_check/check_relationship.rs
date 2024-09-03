use ontol_runtime::{property::PropertyCardinality, tuple::CardinalIdx, DefId, PropId};
use tracing::debug;

use crate::{
    def::{BuiltinRelationKind, DefKind},
    edge::{CardinalKind, EdgeId, SymbolicEdge},
    error::CompileError,
    mem::Intern,
    misc::{MacroExpand, TypeParam},
    package::ONTOL_PKG,
    properties::{Constructor, Property},
    relation::{RelId, Relationship},
    sequence::Sequence,
    thesaurus::TypeRelation,
    types::{FormatType, Type, TypeRef, ERROR_TYPE},
    Note, SourceSpan,
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_rel(&mut self, rel_id: RelId, macro_expand: Option<&mut Option<MacroExpand>>) {
        let spanned = self.rel_ctx.spanned_relationship_by_id(rel_id);
        self.check_relationship(rel_id, spanned.value, spanned.span, macro_expand, None);
    }

    fn check_relationship(
        &mut self,
        rel_id: RelId,
        relationship: &Relationship,
        span: &SourceSpan,
        macro_expand: Option<&mut Option<MacroExpand>>,
        parent_relationship: Option<&Relationship>,
    ) -> TypeRef<'m> {
        let relation_def_kind = &self.defs.def_kind(relationship.relation_def_id);

        match relation_def_kind {
            DefKind::TextLiteral(_) => {
                self.check_named_relation(rel_id, None, relationship);
            }
            DefKind::Type(_) => {
                let edge_id = self
                    .edge_ctx
                    .edge_id_by_symbol(relationship.relation_def_id);

                self.check_named_relation(rel_id, edge_id, relationship);
            }
            DefKind::BuiltinRelType(kind, _) => {
                self.check_builtin_relation(
                    rel_id,
                    relationship,
                    kind,
                    span,
                    macro_expand,
                    parent_relationship,
                );
            }
            _ => {
                panic!()
            }
        }

        for (modifier_relationship, span) in &relationship.modifiers {
            self.check_relationship(
                rel_id,
                modifier_relationship,
                span,
                None,
                Some(relationship),
            );
        }

        self.type_ctx.intern(Type::Tautology)
    }

    /// This defines a property on a compound type.
    /// The relation can be:
    /// 1. a text literal, which defines a named property
    /// 2. a domain-defined unit type, which defines a "flattened" property
    fn check_named_relation(
        &mut self,
        rel_id: RelId,
        edge_id: Option<EdgeId>,
        relationship: &Relationship,
    ) -> TypeRef<'m> {
        let subject = &relationship.subject;
        let object = &relationship.object;

        let subject_ty = self.check_def(subject.0);
        let object_ty = self.check_def(object.0);

        if subject.0.package_id() == ONTOL_PKG {
            CompileError::SubjectMustBeDomainType
                .span(subject.1)
                .report(self);
            return object_ty;
        }

        self.check_subject_data_type(subject_ty, &subject.1);
        self.check_object_data_type(object_ty, &object.1);

        let prop_id = self.prop_ctx.append_prop(
            subject.0,
            Property {
                rel_id,
                cardinality: relationship.subject_cardinality,
                is_entity_id: false,
                is_edge_partial: false,
            },
        );

        // Ensure properties in object
        self.prop_ctx.properties_by_def_id_mut(object.0);

        if let Some(edge_id) = edge_id {
            let edge = self.edge_ctx.symbolic_edges.get_mut(&edge_id).unwrap();
            let slot = edge.symbols.get(&relationship.relation_def_id).unwrap();

            let left = slot.left;
            let right = slot.right;

            fn update_edge_prop(
                edge: &mut SymbolicEdge,
                idx: CardinalIdx,
                def_id: DefId,
                prop_id: PropId,
            ) -> bool {
                let cardinal = edge.cardinals.get_mut(&idx).unwrap();
                if let CardinalKind::Vertex { members } = &mut cardinal.kind {
                    members.entry(def_id).or_default().insert(prop_id);
                    true
                } else {
                    false
                }
            }

            if !update_edge_prop(edge, left, subject.0, prop_id)
                || !update_edge_prop(edge, right, object.0, prop_id)
            {
                CompileError::TODO("must use an edge symbol that has variables on both sides")
                    .span(relationship.relation_span)
                    .with_note(
                        Note::ThisSymbol.span(self.defs.def_span(relationship.relation_def_id)),
                    )
                    .report(self);
            }
        }

        object_ty
    }

    fn check_builtin_relation(
        &mut self,
        rel_id: RelId,
        relationship: &Relationship,
        relation: &BuiltinRelationKind,
        span: &SourceSpan,
        macro_expand: Option<&mut Option<MacroExpand>>,
        parent_relationship: Option<&Relationship>,
    ) -> TypeRef<'m> {
        let subject = &relationship.subject;
        let object = &relationship.object;

        // TODO: Check the object property is undefined

        match relation {
            BuiltinRelationKind::Is => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                if subject_ty.is_macro_def() && !object_ty.is_macro_def() {
                    return CompileError::TODO("macro def can only use other macro defs")
                        .span(*span)
                        .report_ty(self);
                }

                if let Type::MacroDef(macro_def_id) = object_ty {
                    self.check_subject_data_type(subject_ty, &subject.1);
                    debug!("is macro {rel_id:?}");

                    if let Some(macro_expand) = macro_expand {
                        *macro_expand = Some(MacroExpand {
                            subject: relationship.subject.0,
                            macro_def_id: *macro_def_id,
                        });
                    } else {
                        return CompileError::TODO("cannot use a macro here")
                            .span(*span)
                            .report_ty(self);
                    }
                } else {
                    self.check_subject_data_type(subject_ty, &subject.1);
                    self.check_object_data_type(object_ty, &object.1);

                    // Ensure properties
                    self.prop_ctx.properties_by_def_id_mut(subject.0);

                    if !self.thesaurus.insert_domain_is(
                        subject.0,
                        match relationship.subject_cardinality.0 {
                            PropertyCardinality::Mandatory => TypeRelation::Super,
                            PropertyCardinality::Optional => TypeRelation::SubVariant,
                        },
                        object.0,
                        *span,
                    ) {
                        CompileError::DuplicateAnonymousRelationship
                            .span(*span)
                            .report(self);
                    }
                }

                object_ty
            }
            BuiltinRelationKind::Identifies => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.prop_ctx.properties_by_def_id_mut(subject.0);

                if properties.identifies.is_some() {
                    return CompileError::AlreadyIdentifiesAType
                        .span(*span)
                        .report_ty(self);
                }
                if subject.0.package_id() != object.0.package_id() {
                    return CompileError::MustIdentifyWithinDomain
                        .span(*span)
                        .report_ty(self);
                }

                properties.identifies = Some(rel_id);
                let object_properties = self.prop_ctx.properties_by_def_id_mut(object.0);
                match object_properties.identified_by {
                    Some(id) => {
                        debug!(
                            "Object is identified by {id:?}, this relation is {:?}",
                            relationship.relation_def_id
                        );
                        return CompileError::AlreadyIdentified.span(*span).report_ty(self);
                    }
                    None => object_properties.identified_by = Some(rel_id),
                }

                object_ty
            }
            BuiltinRelationKind::Id => {
                panic!("This should not have been lowered");
            }
            BuiltinRelationKind::StoreKey => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);

                let Type::TextConstant(def_id) = object_ty else {
                    return CompileError::TypeMismatch {
                        actual: format!(
                            "{}",
                            FormatType::new(object_ty, self.defs, self.primitives)
                        ),
                        expected: "text constant".to_string(),
                    }
                    .span(*span)
                    .report_ty(self);
                };

                let DefKind::TextLiteral(text_literal) = self.defs.def_kind(*def_id) else {
                    panic!("text literal expected to be registered not found!");
                };
                let text_constant = self.str_ctx.intern_constant(text_literal);
                self.edge_ctx.store_keys.insert(subject.0, text_constant);

                object_ty
            }
            BuiltinRelationKind::Indexed => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);
                let properties = self.prop_ctx.properties_by_def_id_mut(subject.0);
                match (&properties.table, &mut properties.constructor) {
                    (None, Constructor::Transparent) => {
                        let mut sequence = Sequence::default();

                        if let Err(error) =
                            sequence.define_relationship(&relationship.rel_params, rel_id)
                        {
                            return error.span(*span).report_ty(self);
                        }

                        properties.constructor = Constructor::Sequence(sequence);
                    }
                    (None, Constructor::Sequence(sequence)) => {
                        if let Err(error) =
                            sequence.define_relationship(&relationship.rel_params, rel_id)
                        {
                            return error.span(*span).report_ty(self);
                        }
                    }
                    _ => {
                        return CompileError::InvalidMixOfRelationshipTypeForSubject
                            .span(*span)
                            .report_ty(self);
                    }
                }

                object_ty
            }
            BuiltinRelationKind::Default => {
                let Some(parent_relationship) = parent_relationship else {
                    return &ERROR_TYPE;
                };

                let _subject_ty = self.check_def(subject.0);

                let Relationship {
                    object: outer_object,
                    ..
                } = parent_relationship;

                let Some(object_ty) = self.def_ty_ctx.def_table.get(&outer_object.0).cloned()
                else {
                    return CompileError::TODO(
                        "the type of the default relation has not been checked",
                    )
                    .span(*span)
                    .report_ty(self);
                };

                // just copy the type, type check done later
                self.expected_constant_types.insert(object.0, object_ty);

                let _object_ty = self.check_def(object.0);

                self.misc_ctx.default_const_objects.insert(rel_id, object.0);

                object_ty
            }
            BuiltinRelationKind::Gen => {
                let Some(parent_relationship) = parent_relationship else {
                    return &ERROR_TYPE;
                };

                let _subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                let Type::ValueGenerator(value_generator_def_id) = object_ty else {
                    return CompileError::TODO("Not a value generator")
                        .span(object.1)
                        .report_ty(self);
                };
                let Relationship {
                    object: outer_object,
                    ..
                } = parent_relationship;

                let Some(_) = self.def_ty_ctx.def_table.get(&outer_object.0) else {
                    return CompileError::TODO("the type of the gen relation has not been checked")
                        .span(*span)
                        .report_ty(self);
                };

                self.misc_ctx
                    .value_generators_unchecked
                    .insert(rel_id, (*value_generator_def_id, *span));
                object_ty
            }
            BuiltinRelationKind::Min | BuiltinRelationKind::Max | BuiltinRelationKind::Example => {
                let subject_ty = self.check_def(subject.0);
                let _ = self.check_def(object.0);

                self.misc_ctx
                    .type_params
                    .entry(subject.0)
                    .or_default()
                    .insert(
                        relationship.relation_def_id,
                        TypeParam {
                            definition_site: rel_id.0.package_id(),
                            object: object.0,
                            span: *span,
                        },
                    );

                subject_ty
            }
            BuiltinRelationKind::Order => {
                let subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                self.check_subject_data_type(subject_ty, &subject.1);
                self.check_object_data_type(object_ty, &object.1);

                // This will be checked post-seal in check_entity.rs
                self.misc_ctx
                    .order_relationships
                    .entry(subject.0)
                    .or_default()
                    .push(rel_id);

                object_ty
            }
            BuiltinRelationKind::Direction => {
                let _subject_ty = self.check_def(subject.0);
                let object_ty = self.check_def(object.0);

                if self
                    .misc_ctx
                    .direction_relationships
                    .insert(subject.0, (rel_id, object.0))
                    .is_some()
                {
                    return CompileError::TODO("duplicate `direction` relationship")
                        .span(*span)
                        .report_ty(self);
                }

                object_ty
            }
            BuiltinRelationKind::FmtTransition => &Type::Tautology,
        }
    }

    fn check_subject_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        let Some(def_id) = ty.get_single_def_id() else {
            CompileError::SubjectMustBeDomainType
                .span(*span)
                .report(self);
            return;
        };

        match self.defs.def_kind(def_id) {
            DefKind::Primitive(..) | DefKind::Type(_) | DefKind::Extern(_) | DefKind::Macro(_) => {
                self.check_not_sealed(ty, span);
            }
            _ => {
                CompileError::SubjectMustBeDomainType
                    .span(*span)
                    .report(self);
            }
        }
    }

    fn check_object_data_type(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        match ty {
            Type::Tautology
            | Type::BuiltinRelation
            | Type::Function(_)
            | Type::Package
            | Type::Infer(_)
            | Type::ValueGenerator(_)
            | Type::MacroDef(_)
            | Type::Error => {
                CompileError::ObjectMustBeDataType.span(*span).report(self);
            }
            _ => {}
        }
    }

    fn check_not_sealed(&mut self, ty: TypeRef<'m>, span: &SourceSpan) {
        if let Some(def_id) = ty.get_single_def_id() {
            if self.seal_ctx.is_sealed(def_id) {
                CompileError::MutationOfSealedDef.span(*span).report(self);
            }
        }
    }
}
