//! Environment
//!
//! Not sure yet whether it's a compiler or runtime environment, let's see.
//!

use std::collections::HashMap;

use crate::{
    compile::error::CompileErrors,
    def::{DefId, Defs, Namespaces, Relation, Relationship},
    expr::{Expr, ExprId},
    mem::Mem,
    relation::{self, Property, PropertyId, Relations},
    source::{Package, PackageId, Sources},
    types::{TypeRef, Types},
};

/// Runtime environment
#[derive(Debug)]
pub struct Env<'m> {
    pub sources: Sources,

    pub(crate) packages: HashMap<PackageId, Package>,

    pub(crate) namespaces: Namespaces,
    pub(crate) defs: Defs,
    pub(crate) expressions: HashMap<ExprId, Expr>,

    pub(crate) types: Types<'m>,
    pub(crate) def_types: HashMap<DefId, TypeRef<'m>>,
    pub(crate) relations: Relations,

    pub(crate) errors: CompileErrors,
}

impl<'m> Env<'m> {
    pub fn new(mem: &'m Mem) -> Self {
        Self {
            sources: Default::default(),
            packages: Default::default(),
            types: Types::new(mem),
            namespaces: Default::default(),
            defs: Default::default(),
            expressions: Default::default(),
            def_types: Default::default(),
            relations: Default::default(),
            errors: Default::default(),
        }
    }

    /// Look up the type of a definition.
    /// This assumes that type checking has been performed.
    /// Crashes otherwise.
    pub fn get_def_type(&self, def_id: DefId) -> TypeRef<'m> {
        match self.def_types.get(&def_id) {
            Some(type_ref) => type_ref,
            None => match self.defs.map.get(&def_id) {
                Some(def) => {
                    panic!("BUG: Type not found for {def_id:?}: {def:?}");
                }
                None => {
                    panic!("BUG: No definition exists for {def_id:?}");
                }
            },
        }
    }

    pub fn get_property_meta(
        &self,
        property_id: PropertyId,
    ) -> Result<(&relation::Property, &Relationship, &Relation), ()> {
        let property = self.relations.properties.get(&property_id).ok_or(())?;
        let (relationship, relation) = self.defs.get_relationship_defs(property.relationship_id)?;
        Ok((property, relationship, relation))
    }
}
