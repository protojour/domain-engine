#[derive(Default)]
pub struct Mem {
    pub(crate) bump: bumpalo::Bump,
}

/// Intern something in an arena
pub trait Intern<T> {
    type Facade;

    fn intern(&mut self, value: T) -> Self::Facade;
}
