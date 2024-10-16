use domain_engine_core::{domain_error::DomainErrorKind, DomainResult};
use indexmap::IndexMap;
use ontol_runtime::{
    format_utils::format_value,
    ontology::{
        aspects::{DefsAspect, SerdeAspect},
        Ontology,
    },
    value::Value,
    DefId,
};

use crate::core::{DynamicKey, InMemoryStore};

pub(super) enum ConstraintCheck {
    Disabled,
    Deferred(DeferredCheck),
}

struct FormatCtx<'a> {
    defs: &'a DefsAspect,
    serde: &'a SerdeAspect,
}

impl<'a> AsRef<DefsAspect> for FormatCtx<'a> {
    fn as_ref(&self) -> &DefsAspect {
        self.defs
    }
}

impl<'a> AsRef<SerdeAspect> for FormatCtx<'a> {
    fn as_ref(&self) -> &SerdeAspect {
        self.serde
    }
}

impl ConstraintCheck {
    pub fn check_deferred(&self, store: &InMemoryStore, ontology: &Ontology) -> DomainResult<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Deferred(txn) => txn.check_deferred(store, ontology),
        }
    }

    pub fn unresolved_foreign_key(
        &mut self,
        def_id: DefId,
        key: DynamicKey,
        value: Value,
        defs: &DefsAspect,
        serde: &SerdeAspect,
    ) -> DomainResult<()> {
        match self {
            Self::Disabled => {
                let ctx = FormatCtx { defs, serde };
                Err(
                    DomainErrorKind::UnresolvedForeignKey(format_value(&value, &ctx).to_string())
                        .into_error(),
                )
            }
            Self::Deferred(d) => {
                d.deferred_foreign_keys.insert((def_id, key), value);
                Ok(())
            }
        }
    }

    pub fn get_foreign_deferred(&self, def_id: DefId, key: &DynamicKey) -> Option<&Value> {
        match self {
            Self::Disabled => None,
            Self::Deferred(d) => d.deferred_foreign_keys.get(&(def_id, key.clone())),
        }
    }
}

/// It's not really a transaction, but implements deferred integrity check
#[derive(Default)]
pub struct DeferredCheck {
    deferred_foreign_keys: IndexMap<(DefId, DynamicKey), Value>,
}

impl DeferredCheck {
    fn check_deferred(&self, store: &InMemoryStore, ontology: &Ontology) -> DomainResult<()> {
        for ((def_id, key), value) in &self.deferred_foreign_keys {
            if store.look_up_vertex(*def_id, key).is_none() {
                return Err(DomainErrorKind::UnresolvedForeignKey(
                    format_value(value, ontology).to_string(),
                )
                .into_error());
            }
        }

        Ok(())
    }
}
