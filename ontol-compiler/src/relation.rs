use std::collections::HashMap;

use indexmap::IndexSet;
use ontol_runtime::{DefId, PropertyId};

#[derive(Debug, Clone, Copy)]
pub enum Role {
    Subject,
    Object,
}

#[derive(Debug)]
pub struct Relations {
    next_property_id: PropertyId,
    pub properties: HashMap<PropertyId, Property>,
    pub properties_by_type: HashMap<DefId, Properties>,
}

impl Default for Relations {
    fn default() -> Self {
        Self {
            next_property_id: PropertyId(0),
            properties: Default::default(),
            properties_by_type: Default::default(),
        }
    }
}

impl Relations {
    pub fn new_property(&mut self, relationship_id: DefId, role: Role) -> PropertyId {
        let property_id = self.next_property_id;
        self.next_property_id.0 += 1;
        self.properties.insert(
            property_id,
            Property {
                relationship_id,
                role,
            },
        );
        property_id
    }

    pub fn properties_by_type(&self, domain_type_id: DefId) -> Option<&Properties> {
        self.properties_by_type.get(&domain_type_id)
    }

    pub fn properties_by_type_mut(&mut self, domain_type_id: DefId) -> &mut Properties {
        self.properties_by_type.entry(domain_type_id).or_default()
    }
}

#[derive(Default, Debug)]
pub struct Properties {
    pub subject: SubjectProperties,
    pub object: IndexSet<PropertyId>,
}

#[derive(Default, Debug)]
pub enum SubjectProperties {
    /// A type with no properties
    #[default]
    Unit,
    Value(PropertyId),
    Map(IndexSet<PropertyId>),
}

#[derive(Debug)]
pub struct Property {
    pub relationship_id: DefId,
    pub role: Role,
}
