//! Memory management utilities
//!
//! Domain Engine use Atomic Reference Counting internally.
//! JavaScript cannot "see" the memory it implicitly owns through WASM handles,
//! so the JS runtime might fail to invoke GC at appropriate times.
//! i.e. JS could use very little memory (just a pointer into wasm), while WASM would use a lot.
//!
//! These utilities are for explicitly releasing WASM memory _before_ JS memory is released,
//! when the user is done interacting with the objects.

use std::sync::{Arc, Weak};

use crate::wasm_error::{WasmError, WasmResult};

/// WasmGc can optionally own a reference.
pub enum WasmGc<T> {
    Strong(Arc<T>),
    Weak(WasmWeak<T>),
}

impl<T> WasmGc<T> {
    /// Try to upgrade to a strong reference
    pub fn upgrade(&self) -> WasmResult<Arc<T>> {
        match self {
            Self::Strong(arc) => Ok(arc.clone()),
            Self::Weak(weak) => weak.upgrade(),
        }
    }

    /// Release the reference, turning it into a weak ref.
    pub fn release(&mut self) -> bool {
        if let Self::Strong(arc) = self {
            *self = Self::Weak(WasmWeak::from_arc(arc));
        }

        self.upgrade().is_err()
    }

    /// Make a weak reference, leaving this one intact.
    pub fn as_weak(&self) -> WasmWeak<T> {
        match self {
            Self::Strong(arc) => WasmWeak::from_arc(arc),
            Self::Weak(weak) => weak.clone(),
        }
    }
}

impl<T> Clone for WasmGc<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Strong(strong) => Self::Strong(strong.clone()),
            Self::Weak(weak) => Self::Weak(weak.clone()),
        }
    }
}

/// An explicitly weak reference wrapper, for avoiding taking ownership
pub struct WasmWeak<T>(Weak<T>);

impl<T> WasmWeak<T> {
    pub fn from_arc(arc: &Arc<T>) -> Self {
        Self(Arc::downgrade(arc))
    }

    pub fn upgrade(&self) -> WasmResult<Arc<T>> {
        self.0
            .upgrade()
            .ok_or_else(|| WasmError::Generic("Object has been garbage collected".to_string()))
    }
}

impl<T> Clone for WasmWeak<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
