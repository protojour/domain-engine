//! context and data types for raw relationships (`rel`)

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::domain::EdgeCardinalProjection,
    property::{Cardinality, ValueCardinality},
    DefId, DefRelTag, RelId,
};
use tracing::trace;

use crate::{
    def::{DefKind, Defs},
    repr::{
        repr_ctx::ReprCtx,
        repr_model::{ReprKind, ReprScalarKind},
    },
    OwnedOrRef, SourceSpan, SpannedBorrow, NO_SPAN,
};

/// Context that tracks relation and relationship information
#[derive(Default)]
pub struct RelCtx {
    allocators: FnvHashMap<DefId, DefRelTag>,
    table: BTreeMap<RelId, (Relationship, SourceSpan)>,
}

impl RelCtx {
    pub fn alloc_rel_id(&mut self, def_id: DefId) -> RelId {
        let tag_mut = self.allocators.entry(def_id).or_insert(DefRelTag(0));
        let tag = *tag_mut;
        tag_mut.0 += 1;

        let rel_id = RelId(def_id, tag);
        trace!("new {rel_id:?}");

        rel_id
    }

    pub fn commit_rel(&mut self, rel_id: RelId, relationship: Relationship, span: SourceSpan) {
        self.table.insert(rel_id, (relationship, span));
    }

    pub fn span(&self, rel_id: RelId) -> SourceSpan {
        self.table.get(&rel_id).unwrap().1
    }

    pub fn spanned_relationship_by_id(&self, rel_id: RelId) -> SpannedBorrow<Relationship> {
        let (value, span) = &self.table.get(&rel_id).unwrap();
        SpannedBorrow { value, span }
    }

    pub fn relationship_by_id_mut(&mut self, rel_id: RelId) -> Option<&mut Relationship> {
        self.table.get_mut(&rel_id).map(|(rel, _)| rel)
    }

    pub fn iter_rel_ids(&self, def_id: DefId) -> impl Iterator<Item = RelId> {
        let max_tag = self
            .allocators
            .get(&def_id)
            .cloned()
            .unwrap_or(DefRelTag(0));

        (0..max_tag.0).map(move |tag| RelId(def_id, DefRelTag(tag)))
    }
}

/// This definition expresses that a relation is a relationship between a subject and an object
#[derive(Clone, Debug)]
pub struct Relationship {
    pub relation_def_id: DefId,
    pub projection: EdgeCardinalProjection,
    pub relation_span: SourceSpan,

    pub subject: (DefId, SourceSpan),
    /// The cardinality of the relationship, i.e. how many objects are related to the subject
    pub subject_cardinality: Cardinality,

    pub object: (DefId, SourceSpan),
    /// How many subjects are related to the object
    pub object_cardinality: Cardinality,

    pub rel_params: RelParams,
    pub modifiers: Vec<(Relationship, SourceSpan)>,
    pub macro_source: Option<RelId>,
}

impl Relationship {
    pub fn subject(&self) -> (DefId, Cardinality, SourceSpan) {
        (self.subject.0, self.subject_cardinality, self.subject.1)
    }

    pub fn object(&self) -> (DefId, Cardinality, SourceSpan) {
        (self.object.0, self.object_cardinality, self.object.1)
    }

    pub fn can_identify(&self) -> bool {
        matches!(self.object_cardinality.1, ValueCardinality::Unit)
    }
}

#[derive(Clone, Debug)]
pub enum RelParams {
    Unit,
    Type(DefId),
    IndexRange(Range<Option<u16>>),
}

#[derive(Clone)]
pub struct RelDefMeta<'d, 'm> {
    pub rel_id: RelId,
    pub relationship: SpannedBorrow<'d, Relationship>,
    pub relation_def_kind: SpannedBorrow<'d, DefKind<'m>>,
}

pub struct RelReprMeta<'a> {
    pub rel_id: RelId,
    pub relationship: SpannedBorrow<'a, Relationship>,
    pub relation_repr_kind: OwnedOrRef<'a, ReprKind>,
}

/// Cache of which DefId is a member of which unions
pub struct UnionMemberCache {
    pub(crate) cache: FnvHashMap<DefId, BTreeSet<DefId>>,
}

pub fn rel_def_meta<'c, 'm>(
    rel_id: RelId,
    rel_ctx: &'c RelCtx,
    defs: &'c Defs<'m>,
) -> RelDefMeta<'c, 'm> {
    let (relationship, span) = rel_ctx.table.get(&rel_id).unwrap();
    let relationship = SpannedBorrow {
        value: relationship,
        span,
    };

    let relation_def_kind = defs
        .get_spanned_def_kind(relationship.relation_def_id)
        .expect("no def for relation id");

    RelDefMeta {
        rel_id,
        relationship,
        relation_def_kind,
    }
}

pub fn rel_repr_meta<'c>(
    rel_id: RelId,
    rel_ctx: &'c RelCtx,
    defs: &'c Defs,
    repr_ctx: &'c ReprCtx,
) -> RelReprMeta<'c> {
    let (relationship, span) = rel_ctx.table.get(&rel_id).unwrap();
    let relationship = SpannedBorrow {
        value: relationship,
        span,
    };
    let relation_def_id = relationship.relation_def_id;

    let relation_repr_kind = repr_ctx
        .get_repr_kind(&relation_def_id)
        .map(OwnedOrRef::Borrowed)
        .unwrap_or_else(|| match defs.def_kind(relation_def_id) {
            DefKind::TextLiteral(_) => OwnedOrRef::Owned(ReprKind::Scalar(
                relation_def_id,
                ReprScalarKind::TextConstant(relation_def_id),
                NO_SPAN,
            )),
            _ => panic!(
                "no repr for {:?}: {:?}",
                relationship.relation_def_id,
                defs.def_kind(relationship.relation_def_id)
            ),
        });

    RelReprMeta {
        rel_id,
        relationship,
        relation_repr_kind,
    }
}
