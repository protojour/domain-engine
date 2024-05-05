//! Track relationships via the `is` relation
//!
use std::collections::HashSet;

use fnv::FnvHashMap;
use indexmap::{map::Entry, IndexMap};
use ontol_runtime::DefId;
use thin_vec::{thin_vec, ThinVec};

use crate::{
    def::{DefKind, Defs},
    primitive::Primitives,
    SourceSpan, NO_SPAN,
};

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Is {
    pub def_id: DefId,
    pub rel: TypeRelation,
}

impl Is {
    pub fn is_super(&self) -> bool {
        matches!(self.rel, TypeRelation::Super | TypeRelation::ImplicitSuper)
    }

    pub fn is_sub(&self) -> bool {
        matches!(self.rel, TypeRelation::Subset | TypeRelation::SubVariant)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum TypeRelation {
    /// Explicit supertype relation
    Super,
    /// Implicit supertype relation, no actual "inheritance" (built-in only)
    ImplicitSuper,
    /// Explicit subvariant relation
    SubVariant,
    /// Implicit subset relation (built-in only)
    Subset,
}

pub struct Entries<'a> {
    builtin_entries: &'a [(Is, SourceSpan)],
    domain_entries: &'a IndexMap<Is, SourceSpan>,
}

impl<'a> Entries<'a> {
    pub fn is_empty(&self) -> bool {
        self.builtin_entries.is_empty() && self.domain_entries.is_empty()
    }

    pub fn iter(&self) -> EntriesIter {
        EntriesIter {
            builtin_entries: self.builtin_entries.iter(),
            domain_entries: self.domain_entries.iter(),
        }
    }
}

impl<'a> IntoIterator for Entries<'a> {
    type IntoIter = EntriesIter<'a>;
    type Item = (&'a Is, &'a SourceSpan);

    fn into_iter(self) -> Self::IntoIter {
        EntriesIter {
            builtin_entries: self.builtin_entries.iter(),
            domain_entries: self.domain_entries.iter(),
        }
    }
}

pub struct EntriesIter<'a> {
    builtin_entries: core::slice::Iter<'a, (Is, SourceSpan)>,
    domain_entries: indexmap::map::Iter<'a, Is, SourceSpan>,
}

impl<'a> Iterator for EntriesIter<'a> {
    type Item = (&'a Is, &'a SourceSpan);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((is, span)) = self.builtin_entries.next() {
            return Some((is, span));
        }

        self.domain_entries.next()
    }
}

pub struct Thesaurus {
    table: FnvHashMap<DefId, IndexMap<Is, SourceSpan>>,
    pub reverse_table: FnvHashMap<DefId, HashSet<DefId>>,

    /// This should always be empty
    no_entries: IndexMap<Is, SourceSpan>,

    text_literal_entries: ThinVec<(Is, SourceSpan)>,
}

impl Thesaurus {
    pub fn new(primitives: &Primitives) -> Self {
        Self {
            table: Default::default(),
            reverse_table: Default::default(),
            no_entries: Default::default(),
            text_literal_entries: thin_vec![(
                Is {
                    def_id: primitives.text,
                    rel: TypeRelation::ImplicitSuper
                },
                NO_SPAN
            )],
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (DefId, &IndexMap<Is, SourceSpan>)> {
        self.table.iter().map(|(def_id, map)| (*def_id, map))
    }

    pub fn entries(&self, def_id: DefId, defs: &Defs) -> Entries<'_> {
        Entries {
            builtin_entries: match defs.def_kind(def_id) {
                DefKind::TextLiteral(_) => self.text_literal_entries.as_slice(),
                _ => &[],
            },
            domain_entries: self.table.get(&def_id).unwrap_or(&self.no_entries),
        }
    }

    pub fn entries_raw(&self, def_id: DefId) -> Vec<Is> {
        self.table
            .get(&def_id)
            .iter()
            .flat_map(|map| map.keys())
            .copied()
            .collect()
    }

    pub fn insert_domain_is(
        &mut self,
        subject: DefId,
        rel: TypeRelation,
        object: DefId,
        span: SourceSpan,
    ) -> bool {
        let prev_entry = self.table.entry(subject).or_default().entry(Is {
            def_id: object,
            rel,
        });

        match prev_entry {
            Entry::Vacant(vacant) => {
                vacant.insert(span);

                self.reverse_table
                    .entry(object)
                    .or_default()
                    .insert(subject);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub fn insert_builtin_is(
        &mut self,
        sub_def_id: DefId,
        (super_rel, sub_rel): (TypeRelation, TypeRelation),
        super_def_id: DefId,
    ) {
        self.table.entry(sub_def_id).or_default().insert(
            Is {
                def_id: super_def_id,
                rel: sub_rel,
            },
            NO_SPAN,
        );
        self.table.entry(super_def_id).or_default().insert(
            Is {
                def_id: sub_def_id,
                rel: super_rel,
            },
            NO_SPAN,
        );
    }
}
