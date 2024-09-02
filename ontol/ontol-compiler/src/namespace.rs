use arcstr::ArcStr;
use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::DefId;

use crate::relation::RelId;

/// Namespace disambiguator
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum Space {
    Def,
    Map,
}

/// The reason that IndexMap is used here is determinism.
/// The keys will then be sorted in the order written in ONTOL, so that
/// each compile behaves similar to the previous compile, easing debugging.
#[derive(Default, Debug)]
pub struct Namespace<'m> {
    pub(crate) types: IndexMap<&'m str, DefId>,
    pub(crate) maps: IndexMap<&'m str, DefId>,
    pub(crate) anonymous: Vec<DefId>,
}

impl<'m> Namespace<'m> {
    pub fn space(&self, space: Space) -> &IndexMap<&'m str, DefId> {
        match space {
            Space::Def => &self.types,
            Space::Map => &self.maps,
        }
    }

    pub fn space_mut(&mut self, space: Space) -> &mut IndexMap<&'m str, DefId> {
        match space {
            Space::Def => &mut self.types,
            Space::Map => &mut self.maps,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum DocId {
    Def(DefId),
    Rel(RelId),
}

#[derive(Default)]
pub struct Namespaces<'m> {
    pub(crate) namespaces: FnvHashMap<DefId, Namespace<'m>>,
    pub(crate) docs: FnvHashMap<DocId, ArcStr>,
}

impl<'m> Namespaces<'m> {
    pub fn lookup(&self, search_path: &[DefId], space: Space, ident: &str) -> Option<DefId> {
        for def_id in search_path {
            let Some(namespace) = self.namespaces.get(def_id) else {
                continue;
            };
            if let Some(def_id) = namespace.space(space).get(ident) {
                return Some(*def_id);
            };
        }

        None
    }

    pub fn get_namespace_mut(
        &mut self,
        parent: DefId,
        space: Space,
    ) -> &mut IndexMap<&'m str, DefId> {
        self.namespaces.entry(parent).or_default().space_mut(space)
    }

    pub fn add_anonymous(&mut self, parent: DefId, def_id: DefId) {
        self.namespaces
            .entry(parent)
            .or_default()
            .anonymous
            .push(def_id);
    }
}
