use ontol_runtime::{DefId, Role};

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Def, DefKind},
    error::CompileError,
    relation::MapProperties,
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_domain_types(&mut self) {
        for (def_id, def) in &self.defs.map {
            if let DefKind::DomainType(_) | DefKind::DomainEntity(_) = &def.kind {
                self.check_domain_type_properties(*def_id, def);
            }
        }
    }

    fn check_domain_type_properties(&mut self, def_id: DefId, _def: &Def) -> Option<()> {
        let properties = self.relations.properties_by_type(def_id)?;
        let is_entity = properties.id.is_some();

        let mut errors = vec![];

        if let MapProperties::Map(map) = &properties.map {
            for (property_id, _) in map {
                match property_id.role {
                    Role::Subject => {
                        // self.get_subject_property_meta(def_id, property_id.relation_id)
                        //     .unwrap();
                    }
                    Role::Object => {
                        if !is_entity {
                            let (relationship, _) = self
                                .get_object_property_meta(def_id, property_id.relation_id)
                                .unwrap();

                            errors.push((
                                CompileError::NonEntityInReverseRelationship,
                                *relationship.span,
                            ));
                        }
                    }
                }
            }
        }

        for (compile_error, span) in errors {
            self.error(compile_error, &span);
        }

        None
    }
}
