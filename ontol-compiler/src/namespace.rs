use std::collections::HashMap;

use smartstring::alias::String;

use crate::{def::DefId, PackageId};

/// Namespace disambiguator
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub enum Space {
    Type,
    Rel,
}

#[derive(Default, Debug)]
pub struct Namespace {
    types: HashMap<String, DefId>,
    relations: HashMap<String, DefId>,
}

impl Namespace {
    pub fn space(&self, space: Space) -> &HashMap<String, DefId> {
        match space {
            Space::Type => &self.types,
            Space::Rel => &self.relations,
        }
    }

    pub fn space_mut(&mut self, space: Space) -> &mut HashMap<String, DefId> {
        match space {
            Space::Type => &mut self.types,
            Space::Rel => &mut self.relations,
        }
    }
}

#[derive(Default, Debug)]
pub struct Namespaces {
    pub(crate) namespaces: HashMap<PackageId, Namespace>,
}

impl Namespaces {
    pub fn lookup(&self, search_path: &[PackageId], space: Space, ident: &str) -> Option<DefId> {
        for package in search_path {
            let Some(namespace) = self.namespaces.get(package) else {
                continue
            };
            if let Some(def_id) = namespace.space(space).get(ident) {
                return Some(*def_id);
            };
        }

        None
    }

    pub fn get_mut(&mut self, package: PackageId, space: Space) -> &mut HashMap<String, DefId> {
        self.namespaces.entry(package).or_default().space_mut(space)
    }
}
