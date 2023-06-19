//! Traits that can be implemented for "variants" of the compiler

use ontol_runtime::{DefId, RelationId};

use crate::{
    def::{Defs, Relation, Relationship},
    relation::{Relations, RelationshipId},
    types::{DefTypes, TypeRef},
    SpannedBorrow,
};

fn get<T: AsRef<U>, U>(owner: &T) -> &U {
    <T as AsRef<U>>::as_ref(owner)
}

pub trait GetDefType<'m> {
    /// Look up the type of a definition.
    fn get_def_type(&self, def_id: DefId) -> Option<TypeRef<'m>>;

    /// Look up the type of a definition.
    /// This assumes that type checking has been performed.
    /// Crashes otherwise.
    fn expect_def_type(&self, def_id: DefId) -> TypeRef<'m>;
}

impl<'m, T> GetDefType<'m> for T
where
    T: AsRef<Defs<'m>> + AsRef<DefTypes<'m>>,
{
    fn get_def_type(&self, def_id: DefId) -> Option<TypeRef<'m>> {
        get::<_, DefTypes>(self).map.get(&def_id).copied()
    }

    fn expect_def_type(&self, def_id: DefId) -> TypeRef<'m> {
        match get::<_, DefTypes>(self).map.get(&def_id) {
            Some(type_ref) => type_ref,
            None => match get::<_, Defs>(self).map.get(&def_id) {
                Some(def) => {
                    panic!("BUG: Type not found for {def_id:?}: {def:?}");
                }
                None => {
                    panic!("BUG: No definition exists for {def_id:?}");
                }
            },
        }
    }
}

pub struct RelationshipMeta<'m> {
    pub relationship_id: RelationshipId,
    pub relationship: SpannedBorrow<'m, Relationship>,
    pub relation: SpannedBorrow<'m, Relation<'m>>,
}

pub trait GetPropertyMeta<'m> {
    fn get_relationship_meta(
        &self,
        relationship_id: RelationshipId,
    ) -> Result<RelationshipMeta<'m>, ()>;

    fn relationship_meta_by_subject(
        &self,
        subject_id: DefId,
        relation_id: RelationId,
    ) -> Result<RelationshipMeta<'m>, ()>;

    fn relationship_meta_by_object(
        &self,
        object_id: DefId,
        relation_id: RelationId,
    ) -> Result<RelationshipMeta<'m>, ()>;
}

impl<'m, T> GetPropertyMeta<'m> for T
where
    T: AsRef<Relations> + AsRef<Defs<'m>>,
{
    fn get_relationship_meta(
        &self,
        relationship_id: RelationshipId,
    ) -> Result<RelationshipMeta<'m>, ()> {
        get::<_, Defs<'m>>(self).lookup_relationship_meta(relationship_id)
    }

    fn relationship_meta_by_subject(
        &self,
        subject_id: DefId,
        relation_id: RelationId,
    ) -> Result<RelationshipMeta<'m>, ()> {
        let relationship_id = get::<_, Relations>(self)
            .relationships_by_subject
            .get(&(subject_id, relation_id))
            .cloned()
            .ok_or(())?;

        get::<_, Defs<'m>>(self).lookup_relationship_meta(relationship_id)
    }

    fn relationship_meta_by_object(
        &self,
        object_id: DefId,
        relation_id: RelationId,
    ) -> Result<RelationshipMeta<'m>, ()> {
        let relationship_id = get::<_, Relations>(self)
            .relationships_by_object
            .get(&(object_id, relation_id))
            .cloned()
            .ok_or(())?;

        get::<_, Defs<'m>>(self).lookup_relationship_meta(relationship_id)
    }
}
