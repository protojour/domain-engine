use std::collections::{HashMap, HashSet};

use crate::def::DefId;

#[derive(Debug)]
pub enum Role {
    Subject,
    Object,
}

#[derive(Default, Debug)]
pub struct Relations {
    pub property_map: HashMap<DefId, Properties>,
}

impl Relations {
    pub fn properties_mut(&mut self, def_id: DefId) -> &mut Properties {
        self.property_map.entry(def_id).or_default()
    }
}

#[derive(Default, Debug)]
pub struct Properties {
    pub subject: SubjectProperties,
    pub object: HashSet<DefId>,
}

#[derive(Default, Debug)]
pub enum SubjectProperties {
    #[default]
    None,
    Anonymous(DefId),
    Named(HashSet<DefId>),
}
