use std::ops::Deref;

use arcstr::ArcStr;
use fnv::FnvHashSet;
use ontol_runtime::{
    ontology::{domain::DomainId, ontol::TextConstant},
    property::{PropertyCardinality, ValueCardinality},
    DefId, DomainIndex,
};
use tracing::{debug, debug_span, info};
use ulid::Ulid;

use crate::{
    def::{BuiltinRelationKind, DefKind},
    edge::{EdgeCtx, EdgeId},
    entity::entity_ctx::def_implies_entity,
    lowering::context::LoweringOutcome,
    misc::{MacroExpand, MacroItem, MiscCtx},
    namespace::DocId,
    relation::{rel_def_meta, RelId, RelParams, Relationship},
    repr::repr_model::ReprKind,
    thesaurus::{Thesaurus, TypeRelation},
    topology::ParsedDomain,
    type_check::MapArmsKind,
    types::Type,
    CompileError, Compiler, Session, SourceSpan, Src, UnifiedCompileError,
};

impl<'m> Compiler<'m> {
    /// Lower statements from the next domain,
    /// perform type check against its dependent domains,
    /// and seal the types at the end.
    pub(super) fn lower_and_check_next_domain(
        &mut self,
        parsed: ParsedDomain,
        src: Src,
    ) -> Result<(), UnifiedCompileError> {
        let _entered = debug_span!("domain", idx = ?parsed.domain_index.index()).entered();

        for error in parsed.parse_errors {
            match error {
                ontol_parser::Error::Lex(lex_error) => {
                    let span = lex_error.span;
                    CompileError::Lex(lex_error.msg).span(src.span(span))
                }
                ontol_parser::Error::Parse(parse_error) => {
                    let span = parse_error.span;
                    CompileError::Parse(parse_error.msg).span(src.span(span))
                }
            }
            .report(self);
        }

        let domain_def_id = self.defs.alloc_def_id(parsed.domain_index);
        self.define_domain(domain_def_id);

        self.loaded
            .by_reference
            .insert(parsed.reference, domain_def_id);

        self.domain_config_table
            .insert(parsed.domain_index, parsed.config);

        let outcome = parsed
            .syntax
            .lower(domain_def_id, src.clone(), Session(self));

        self.domain_ids
            .entry(parsed.domain_index)
            .or_insert_with(|| {
                let domain_id = Ulid::new();
                info!("autogenerating unstable domain id `{domain_id}`");
                DomainId {
                    ulid: domain_id,
                    stable: false,
                }
            });

        self.handle_lowering_outcome(outcome);
        self.seal_domain(parsed.domain_index);

        self.domain_names
            .push((parsed.domain_index, self.str_ctx.intern_constant(&src.name)));

        self.check_error()
    }

    fn handle_lowering_outcome(&mut self, mut outcome: LoweringOutcome) {
        let mut macro_defs = vec![];

        for def_id in outcome.root_defs {
            if let Type::MacroDef(macro_def_id) = self.type_check().check_def(def_id) {
                macro_defs.push(macro_def_id);
            }
        }

        {
            for (def_id, chain) in outcome.fmt_chains {
                self.type_check().check_def(def_id);

                self.check_fmt_chain(def_id, chain);
            }
        }

        // handle relationships in macros first, they have to be ready before other relationships
        for macro_def_id in macro_defs {
            if let Some(pkg_rels) = outcome.rels.get_mut(&macro_def_id.0) {
                if let Some(macro_rels) = pkg_rels.remove(&macro_def_id.1) {
                    let mut macro_items: Vec<MacroItem> = vec![];

                    for (rel_tag, relationship, span, docs) in macro_rels {
                        let rel_id = RelId(*macro_def_id, rel_tag);
                        let relation_def_kind = self.defs.def_kind(relationship.relation_def_id);
                        let subject_cardinality = relationship.subject_cardinality;

                        if let DefKind::BuiltinRelType(BuiltinRelationKind::Is, _) =
                            relation_def_kind
                        {
                            if matches!(
                                subject_cardinality,
                                (PropertyCardinality::Mandatory, ValueCardinality::Unit)
                            ) {
                                let object_def_kind = self.defs.def_kind(relationship.object.0);
                                if let DefKind::Macro(_macro_ident) = object_def_kind {
                                    macro_items.push(MacroItem::UseMacro(relationship.object.0));
                                } else {
                                    CompileError::TODO("must be a macro")
                                        .span(span)
                                        .report(self);
                                }
                            } else {
                                macro_items.push(MacroItem::Relationship {
                                    rel_id,
                                    relationship,
                                    span,
                                    docs,
                                });
                            }
                        } else {
                            macro_items.push(MacroItem::Relationship {
                                rel_id,
                                relationship,
                                span,
                                docs,
                            });
                        }
                    }

                    self.misc_ctx
                        .def_macro_items
                        .insert(*macro_def_id, macro_items);
                }
            }
        }

        // commit relationships from outcome
        {
            let mut rel_ids = vec![];

            for (domain_index, rel_map) in outcome.rels {
                for (def_tag, rels) in rel_map {
                    for (rel_tag, relationship, span, docs) in rels {
                        let rel_id = RelId(DefId(domain_index, def_tag), rel_tag);
                        self.rel_ctx.commit_rel(rel_id, relationship, span);

                        if let Some(docs) = docs {
                            self.namespaces.docs.insert(DocId::Rel(rel_id), docs);
                        }

                        rel_ids.push(rel_id);
                    }
                }
            }

            for rel_id in rel_ids {
                let mut macro_expand: Option<MacroExpand> = None;
                self.type_check().check_rel(rel_id, Some(&mut macro_expand));

                if let Some(macro_expand) = macro_expand {
                    for (relationship, span, docs) in
                        self.expand_macro_rels(rel_id, macro_expand.macro_def_id)
                    {
                        let rel_id = self.rel_ctx.alloc_rel_id(macro_expand.subject);
                        self.rel_ctx.commit_rel(rel_id, relationship, span);

                        if let Some(docs) = docs {
                            self.namespaces.docs.insert(DocId::Rel(rel_id), docs);
                        }

                        self.type_check().check_rel(rel_id, None);
                    }
                }
            }
        }
    }

    /// Do all the (remaining) checks and generations for the package/domain and seal it
    /// Initial check_def must be done before this
    pub(crate) fn seal_domain(&mut self, domain_index: DomainIndex) {
        debug!("seal {domain_index:?}");

        self.domain_type_repr_check(domain_index);

        // entity check
        // this is not in the TypeCheck context because it may
        // generate new DefIds
        for def_id in self.defs.iter_domain_def_ids(domain_index) {
            self.check_entity(def_id);
        }

        self.domain_no_entity_supertype_check(domain_index);
        self.domain_union_and_extern_check(domain_index);
        self.domain_rel_normalization(domain_index);
        self.domain_entity_rel_force_edge_check(domain_index);
        self.domain_map_check(domain_index);

        self.seal_ctx.mark_domain_sealed(domain_index);
    }

    /// Check repr for all types in the domain
    fn domain_type_repr_check(&mut self, domain_index: DomainIndex) {
        let mut type_check = self.type_check();

        // pre repr checks
        for def_id in type_check.defs.iter_domain_def_ids(domain_index) {
            if let Some(def) = type_check.defs.table.get(&def_id) {
                if let DefKind::Type(_) = &def.kind {
                    type_check.check_domain_type_pre_repr(def_id, def);
                }
            }
        }

        // repr checks
        for def_id in type_check.defs.iter_domain_def_ids(domain_index) {
            type_check.repr_check(def_id).check_repr_root();
        }

        // domain type checks
        for def_id in type_check.defs.iter_domain_def_ids(domain_index) {
            if let Some(def) = type_check.defs.table.get(&def_id) {
                if let DefKind::Type(_) = &def.kind {
                    type_check.check_domain_type_post_repr(def_id, def);
                }
            }
        }
    }

    /// check that no types use entities as supertypes
    fn domain_no_entity_supertype_check(&mut self, domain_index: DomainIndex) {
        for (def_id, is_table) in self.thesaurus.iter() {
            if def_id.domain_index() != domain_index {
                continue;
            }

            for (is, span) in is_table {
                if matches!(&is.rel, TypeRelation::Super) {
                    let identified_by = self
                        .prop_ctx
                        .properties_by_def_id
                        .get(&is.def_id)
                        .and_then(|properties| properties.identified_by);

                    if identified_by.is_some() {
                        CompileError::EntityCannotBeSupertype
                            .span(*span)
                            .report(&mut self.errors);
                    }
                }
            }
        }
    }

    fn domain_union_and_extern_check(&mut self, domain_index: DomainIndex) {
        let mut type_check = self.type_check();

        // union and extern checks
        for def_id in type_check.defs.iter_domain_def_ids(domain_index) {
            match type_check.repr_ctx.get_repr_kind(&def_id) {
                Some(ReprKind::Union(..)) => {
                    for error in type_check.check_union(def_id) {
                        error.report(&mut type_check);
                    }
                }
                Some(ReprKind::Extern) => {
                    type_check.check_extern(def_id, type_check.defs.def_span(def_id));
                }
                _ => {}
            }
        }
    }

    /// Various cleanup/normalization
    fn domain_rel_normalization(&mut self, domain_index: DomainIndex) {
        for def_id in self.defs.iter_domain_def_ids(domain_index) {
            for rel_id in self.rel_ctx.iter_rel_ids(def_id) {
                let Some(relationship) = self.rel_ctx.relationship_by_id_mut(rel_id) else {
                    // can happen in error cases
                    continue;
                };

                // Reset RelParams::Type back to RelParams::Unit if its representation is ReprKind::Unit.
                // This simplifies later compiler stages, that can trust RelParams::Type is a type with real data in it.
                if let RelParams::Def(rel_params_def_id) = &relationship.rel_params {
                    if let Some(projection) = &relationship.edge_projection {
                        copy_relationship_store_key(
                            EdgeId(projection.edge_id),
                            *rel_params_def_id,
                            &mut self.edge_ctx,
                            &self.thesaurus,
                        );
                    }

                    if matches!(
                        self.repr_ctx.get_repr_kind(rel_params_def_id).unwrap(),
                        ReprKind::Unit
                    ) {
                        relationship.rel_params = RelParams::Unit;
                    }
                }
            }
        }
    }

    fn domain_entity_rel_force_edge_check(&mut self, domain_index: DomainIndex) {
        for def_id in self.defs.iter_domain_def_ids(domain_index) {
            for rel_id in self.rel_ctx.iter_rel_ids(def_id) {
                if !self.rel_ctx.is_committed(rel_id) {
                    continue;
                }

                let meta = rel_def_meta(rel_id, &self.rel_ctx, &self.defs);

                let subject = meta.relationship.subject;
                let object = meta.relationship.object;

                if matches!(meta.relation_def_kind.deref(), DefKind::BuiltinRelType(..)) {
                    continue;
                }

                if let Some(projection) = meta.relationship.edge_projection {
                    let edge_id = EdgeId(projection.edge_id);
                    let edge = self.edge_ctx.symbolic_edges.get(&edge_id).unwrap();

                    if let Some((_, param_def_id)) = edge.find_parameter_cardinal() {
                        let relationship = self.rel_ctx.relationship_by_id_mut(rel_id).unwrap();

                        relationship.rel_params = RelParams::Def(param_def_id);
                    }
                } else if !matches!(
                    meta.relationship.object_cardinality.1,
                    ValueCardinality::Unit
                ) && def_implies_entity(
                    subject.0,
                    &self.repr_ctx,
                    &self.prop_ctx,
                    &self.entity_ctx,
                ) && def_implies_entity(
                    object.0,
                    &self.repr_ctx,
                    &self.prop_ctx,
                    &self.entity_ctx,
                ) {
                    CompileError::EntityToEntityRelationshipMustUseArc
                        .span(meta.relationship.relation_span)
                        .report(&mut self.errors);
                }
            }
        }
    }

    fn domain_map_check(&mut self, domain_index: DomainIndex) {
        let mut map_defs: Vec<DefId> = vec![];

        for def_id in self.defs.iter_domain_def_ids(domain_index) {
            {
                let Some(def) = self.defs.table.get(&def_id) else {
                    // Can happen in error cases
                    continue;
                };
                if matches!(&def.kind, DefKind::Mapping { .. }) {
                    map_defs.push(def_id);
                } else {
                    continue;
                }
            }

            // Infer anonymous types at root of named maps
            if let Some(inference_info) = self.check_map_arm_def_inference(def_id) {
                self.type_check().check_def(inference_info.source.1);
                let outcome = self
                    .map_arm_def_inferencer(def_id)
                    .infer_map_arm_type(inference_info);

                {
                    let mut type_check = self.type_check();
                    for def_id in outcome.new_defs {
                        type_check.check_def(def_id);
                    }
                    for rel_id in outcome.new_rels {
                        type_check.check_rel(rel_id, None);
                    }
                }

                self.type_check().check_def(inference_info.target.1);
            }
        }

        let mut type_check = self.type_check();

        for def_id in map_defs {
            let def = type_check.defs.table.get(&def_id).unwrap();

            if let DefKind::Mapping {
                ident: _,
                arms,
                var_alloc,
                extern_def_id,
                is_abstract,
            } = &def.kind
            {
                if let Some(extern_def_id) = extern_def_id {
                    type_check.check_map_extern(def, *arms, *extern_def_id);
                } else {
                    match type_check.check_map(
                        (def.id, def.span),
                        var_alloc,
                        *arms,
                        if *is_abstract {
                            MapArmsKind::Abstract
                        } else {
                            MapArmsKind::Concrete
                        },
                    ) {
                        Ok(_) => {}
                        Err(error) => {
                            debug!("Check map error: {error:?}");
                        }
                    }
                }
            }
        }
    }

    fn expand_macro_rels(
        &mut self,
        source_rel_id: RelId,
        macro_def_id: DefId,
    ) -> Vec<(Relationship, SourceSpan, Option<ArcStr>)> {
        struct MacroExpandCtx<'a> {
            used_macros: FnvHashSet<DefId>,
            used_rels: FnvHashSet<RelId>,
            misc_ctx: &'a MiscCtx,
            output: Vec<(Relationship, SourceSpan, Option<ArcStr>)>,
        }

        fn expand_inner(source_rel_id: RelId, macro_def_id: DefId, ctx: &mut MacroExpandCtx) {
            if !ctx.used_macros.insert(macro_def_id) {
                return;
            }

            let macro_items = ctx.misc_ctx.def_macro_items.get(&macro_def_id).unwrap();
            for item in macro_items {
                match item {
                    MacroItem::UseMacro(inner_macro_def_id) => {
                        expand_inner(source_rel_id, *inner_macro_def_id, ctx);
                    }
                    MacroItem::Relationship {
                        rel_id,
                        relationship,
                        span,
                        docs,
                    } => {
                        if !ctx.used_rels.contains(rel_id) {
                            ctx.output.push((
                                Relationship {
                                    relation_def_id: relationship.relation_def_id,
                                    edge_projection: relationship.edge_projection,
                                    relation_span: relationship.relation_span,
                                    subject: (source_rel_id.0, relationship.subject.1),
                                    subject_cardinality: relationship.subject_cardinality,
                                    object: relationship.object,
                                    object_cardinality: relationship.object_cardinality,
                                    rel_params: relationship.rel_params.clone(),
                                    macro_source: Some(*rel_id),
                                    modifiers: relationship.modifiers.clone(),
                                },
                                *span,
                                docs.clone(),
                            ));
                        }
                    }
                }
            }
        }

        let mut ctx = MacroExpandCtx {
            used_macros: Default::default(),
            used_rels: Default::default(),
            misc_ctx: &self.misc_ctx,
            output: vec![],
        };

        expand_inner(source_rel_id, macro_def_id, &mut ctx);

        ctx.output
    }
}

/// Copies a store key registered for a set of rel_params,
/// which may be reset to RelParams::Unit if no named parameters are present,
/// and makes the store key available via a RelationshipId (DefId) instead.
fn copy_relationship_store_key(
    edge_id: EdgeId,
    rel_params_def_id: DefId,
    edge_ctx: &mut EdgeCtx,
    thesaurus: &Thesaurus,
) {
    fn recurse_search(
        def_id: DefId,
        edge_ctx: &mut EdgeCtx,
        thesaurus: &Thesaurus,
        result: &mut Option<TextConstant>,
        visited: &mut FnvHashSet<DefId>,
    ) {
        if !visited.insert(def_id) {
            return;
        }

        if let Some(text_constant) = edge_ctx.store_keys.get(&def_id) {
            *result = Some(*text_constant);
            return;
        }

        for entry in thesaurus.entries_raw(def_id) {
            if matches!(entry.rel, TypeRelation::Super) {
                recurse_search(entry.def_id, edge_ctx, thesaurus, result, visited);
            }
        }
    }

    let mut store_key: Option<TextConstant> = None;

    recurse_search(
        rel_params_def_id,
        edge_ctx,
        thesaurus,
        &mut store_key,
        &mut Default::default(),
    );

    if let Some(store_key) = store_key {
        edge_ctx.store_keys.insert(rel_params_def_id, store_key);
        edge_ctx.edge_store_keys.insert(edge_id, store_key);
    }
}
