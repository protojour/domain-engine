use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::DefId;
use smartstring::alias::String;

use crate::PackageId;

/// Namespace disambiguator
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum Space {
    Type,
    Rel,
    Map,
}

/// The reason that IndexMap is used here is determinism.
/// The keys will then be sorted in the order written in ONTOL, so that
/// each compile behaves similar to the previous compile, easing debugging.
#[derive(Default, Debug)]
pub struct Namespace<'m> {
    pub(crate) types: IndexMap<&'m str, DefId>,
    pub(crate) relations: IndexMap<&'m str, DefId>,
    pub(crate) maps: IndexMap<&'m str, DefId>,
    pub(crate) anonymous: Vec<DefId>,
}

impl<'m> Namespace<'m> {
    pub fn space(&self, space: Space) -> &IndexMap<&'m str, DefId> {
        match space {
            Space::Type => &self.types,
            Space::Rel => &self.relations,
            Space::Map => &self.maps,
        }
    }

    pub fn space_mut(&mut self, space: Space) -> &mut IndexMap<&'m str, DefId> {
        match space {
            Space::Type => &mut self.types,
            Space::Rel => &mut self.relations,
            Space::Map => &mut self.maps,
        }
    }
}

#[derive(Default)]
pub struct Namespaces<'m> {
    pub(crate) namespaces: FnvHashMap<PackageId, Namespace<'m>>,
    pub(crate) docs: FnvHashMap<DefId, Vec<String>>,
}

impl<'m> Namespaces<'m> {
    pub fn lookup(&self, search_path: &[PackageId], space: Space, ident: &str) -> Option<DefId> {
        for package in search_path {
            let Some(namespace) = self.namespaces.get(package) else {
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
        package: PackageId,
        space: Space,
    ) -> &mut IndexMap<&'m str, DefId> {
        self.namespaces.entry(package).or_default().space_mut(space)
    }

    pub fn add_anonymous(&mut self, package: PackageId, def_id: DefId) {
        self.namespaces
            .entry(package)
            .or_default()
            .anonymous
            .push(def_id);
    }
}
