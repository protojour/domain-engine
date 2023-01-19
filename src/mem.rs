use std::fmt::Debug;

#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

impl Debug for Mem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mem").finish()
    }
}

/// Intern something in an arena
pub trait Intern<T> {
    type Facade;

    fn intern(&mut self, value: T) -> Self::Facade;
}
