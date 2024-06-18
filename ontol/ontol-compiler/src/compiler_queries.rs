//! Traits that can be implemented for "variants" of the compiler

use ontol_runtime::DefId;

use crate::{
    def::Defs,
    types::{DefTypeCtx, TypeRef},
};

fn get<T: AsRef<U>, U>(owner: &T) -> &U {
    <T as AsRef<U>>::as_ref(owner)
}

pub trait GetDefType<'m> {
    /// Look up the type of a definition.
    fn get_def_type(&self, def_id: DefId) -> Option<TypeRef<'m>>;
}

impl<'m, T> GetDefType<'m> for T
where
    T: AsRef<Defs<'m>> + AsRef<DefTypeCtx<'m>>,
{
    fn get_def_type(&self, def_id: DefId) -> Option<TypeRef<'m>> {
        get::<_, DefTypeCtx>(self).def_table.get(&def_id).copied()
    }
}
